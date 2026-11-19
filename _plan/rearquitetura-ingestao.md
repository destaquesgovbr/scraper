# Rearquitetura da ingestão de notícias (scraping)

Plano de 18/11/2026. Mantém a raspagem resiliente, econômica e fácil de observar, e tira o Airflow do caminho crítico da ingestão.

Relacionado: migração do Composer para um Airflow próprio (`infra/_plan/migrar-composer-para-airflow-proprio.md`); scraper#76 e #77; infra#223.

## Contexto

Desde o scraper#76 (18/11/2026), a ingestão roda numa DAG `scrape_agencies`:
- uma task mapeada por agência ativa (147);
- execução a cada 2 h, das 06h às 22h BRT;
- no máximo 4 agências ao mesmo tempo.

Cada task faz `POST /scrape/agencies` no scraper-api (Cloud Run), que raspa, grava em `news` e em `scrape_runs` e publica em `dgb.news.scraped`.

Na primeira noite, um bug do Airflow 3.1.0 derrubou o scheduler: o retry de uma task mapeada com backoff exponencial lê `max_retry_delay`, que o `MappedOperator` não tem. **A ingestão ficou 7 h parada** junto com todas as DAGs do Composer. O problema é que a parte mais crítica da plataforma depende de um orquestrador de batch, que vai rodar numa VM única.

## Diagnóstico

Dados de `scrape_runs` e `news` dos últimos 27 dias:

| Fato | Consequência |
|---|---|
| As 147 agências estão no **mesmo host, `www.gov.br`** | O limite de concorrência existe para proteger o gov.br, não o scraper-api, que escala até 200 requisições simultâneas. O gov.br já é instável: 330 `network_error` em 68 agências em 7 dias |
| ~242 notícias novas/dia; **98% publicadas entre 06h e 22h** | A madrugada quase não tem notícias |
| **Mais de 98% das raspagens não trazem nada novo**; 35 agências publicam menos de 1 notícia a cada 5 dias e 51 menos de 1 por dia | A raspagem tem a mesma frequência para todas e gasta chamadas em agências quase paradas |
| 5 agências (EBC incluída) concentram 38% do volume (91 notícias/dia) | São as que mais ganhariam com uma frequência maior |
| Erros permanentes (`html_changed`: 1.787 execuções de 10 agências em 7 dias) e passageiros (`network_error`) recebem o mesmo retry | Cada agência quebrada gasta 2 retries de 5 min por execução, e a DAG fica vermelha em toda execução |
| `articles_saved` soma **inseridas + atualizadas**: a agencia_brasil aparece com 2.175/dia, mas o sistema inteiro tem ~242 novas/dia | A métrica de volume e a checagem de "agência sem notícias" medem a coisa errada |
| A janela é fixa, de 9 h para trás | Recupera só falhas curtas. Se as execuções das 06h e das 08h se perderem, perdem-se as notícias das 22h–24h |
| O scraper-api devolve o erro como texto livre, sem categoria (`AgencyError{agency, error}`) | A DAG não sabe se adianta repetir |

Achados no código (origin/main 8bd596c):
- `PostgresManager.insert` sempre roda um UPDATE nas linhas que já existem com a mesma `(agency_key, url)`, mesmo com `allow_update=False`, e as conta em `articles_saved` (`storage/postgres_manager.py:290,308`).
- O `EBCWebScraper` não usa a lista de URLs já conhecidas. Cada execução relê o dia inteiro, e isso é a maior parte da inflação de `articles_saved`.
- **Uma notícia ainda não enriquecida é publicada de novo em `dgb.news.scraped` a cada execução**, até ser enriquecida (`postgres_manager.py:290-293,535-539`).
- Erro de rede do Plone 6 vira `unknown`, porque a mensagem está em português ("Erro de rede ao acessar API…"). Um 4xx permanente que não seja 401/403/404/410 também vira `unknown`. A categoria `empty_content` nunca é gerada.
- O import check da reusable workflow (Airflow 3.0.1) nunca falha: só imprime um aviso.

## Arquitetura alvo

```
Cloud Scheduler (*/15, 06–23h BRT)
   └─► scraper-api POST /dispatch
         ├─ lê scrape_agency_state: agências vencidas (next_due_at <= now) com circuito fechado,
         │   ou circuito meio-aberto com teste vencido
         └─ cria 1 tarefa por agência no Cloud Tasks (nome = scrape-<agência>-<horário>)
                │
        Cloud Tasks "scrape-govbr"  ← max_concurrent_dispatches e max_dispatches_per_second (proteção do gov.br)
                │                     retry com backoff só para erro passageiro
                ▼
         scraper-api POST /scrape/agency
           ├─ janela = desde o último sucesso da agência (marca d'água), limitada a 7 dias
           ├─ grava scrape_runs (articles_new, articles_saved, categoria, retryable)
           ├─ atualiza scrape_agency_state (próxima raspagem, falhas seguidas, circuito)
           └─ responde 2xx para sucesso e para erro permanente; 5xx/429 só para erro passageiro
```

Princípios:
1. **Cada controle no lugar certo.** O limite de taxa para o gov.br fica numa fila feita para isso, não na contagem de tasks do Airflow. A proteção do pipeline seguinte fica no próprio pipeline.
2. **O estado mora nos dados, não no orquestrador.** `scrape_agency_state` é a fonte única de verdade por agência: o despacho, o monitoramento e o painel leem dela.
3. **Idempotente e autorrecuperável.** A janela parte da marca d'água, o nome da tarefa evita duplicata, e se uma rodada se perder a seguinte pega todas as agências vencidas.
4. **Erro permanente não é repetido.** Um disjuntor tira a agência de circulação até um teste diário passar.
5. **O Airflow fica com o batch.** Ele mantém o backfill manual, o relatório diário e as DAGs do data-platform, e deixa de ser o relógio da ingestão.

### Raspagem conforme o ritmo de cada agência (Passo 4)

O intervalo de cada agência vem da média móvel exponencial de notícias novas por dia nos últimos 28 dias:

| Notícias novas/dia | Agências | Notícias/dia | Intervalo | Chamadas/dia |
|---|---|---|---|---|
| ≥ 5 (inclui EBC) | 5 | 91 | 30 min | 170 |
| 1 a 5 | 58 | 123 | 2 h | 522 |
| 0,2 a 1 | 51 | 26 | 6 h | 153 |
| < 0,2 | 35 | 2 | 1× por dia | 35 |

São ~880 chamadas por dia, contra 1.323 hoje. O atraso das agências que concentram 38% do volume cai de até 2 h para até 30 min. Quando uma raspagem encontra notícia nova, o intervalo daquela agência encurta por algumas horas.

## Passos

### Passo 1: ajustes rápidos (scraper e data-platform; feito em 18/11)
1. **Categoria no erro da API.** `errors[]` de `/scrape/agencies` e `/scrape/ebc` passa a ter `category` e `retryable`. São permanentes:
   - `html_changed`, `url_broken`, `access_denied`, `anti_bot`;
   - agência desconhecida ou inativa.

   `network_error` e `unknown` continuam passageiros.
2. **Lacunas da classificação corrigidas.**
   - Erro de rede do Plone 6 passa a ser `network_error`.
   - Um 4xx permanente passa a ser `url_broken` (401/403 continuam `access_denied`).
3. **DAGs (`scrape_agencies` e `scrape_ebc`): erro permanente falha sem retry**, com `AirflowFailException`. Erro passageiro repete como antes. Com uma API antiga, sem `retryable`, tudo repete, como hoje.
4. **Task final `summarize`** (`trigger_rule=all_done`, única folha da DAG):
   - registra quais agências falharam;
   - só derruba a execução se nenhuma agência funcionou ou se mais de `scraper_max_failed_ratio` delas falharam (Variable, padrão 0,10).

   Vermelho volta a significar problema de plataforma, e a agência quebrada aparece na própria task mapeada.
5. **`articles_new`.** A coluna nova em `scrape_runs` vem da migration 037 do data-platform. Ela é preenchida pelo `RETURNING` da fase de inserção (com `xmax = 0` quando `allow_update=True`) e também volta na resposta da API.
   - A checagem "Agências Sem Notícias" e o total do relatório de cobertura passam a usar `COALESCE(articles_new, articles_saved)`.
   - `articles_saved` mantém o significado de "linhas gravadas".
   - **Ordem:** aplicar a 037 em produção (workflow `db-migrate`) antes do merge do scraper.
6. **Concorrência configurável.** `SCRAPE_MAX_CONCURRENT_AGENCIES` é um env lido no parse, com padrão 4, mínimo 1 e fallback 4 se o valor for inválido. No Airflow próprio, cada task tem ~3 s de overhead, contra ~40 s no Composer. Com 4 agências por vez, uma execução cai de ~25 min para ~6 min, sem pressionar mais o gov.br.

### Passo 2: estado por agência, marca d'água e disjuntor
- **Migration 038, `scrape_agency_state`:**
  - `agency_key` (PK), `poll_interval_minutes`, `next_due_at`;
  - `last_attempt_at`, `last_success_at`, `last_new_article_at`;
  - `consecutive_permanent_failures`, `consecutive_transient_failures`;
  - `circuit_state` (`closed`/`open`/`half_open`), `circuit_opened_at`, `last_error_category`, `updated_at`.
- **O scraper-api atualiza o estado no fim de cada agência**, na mesma transação do `scrape_runs`.
- **Janela por marca d'água:** `start = (last_success_at em BRT − 1 h).date`, limitada a 7 dias; sem histórico, ontem. Isso substitui o `lookback_hours=9`.
- **Disjuntor:**
  - 2 erros permanentes seguidos abrem o circuito;
  - circuito aberto é testado 1× por dia, às 09h;
  - um sucesso fecha o circuito;
  - abrir e fechar geram 1 alerta cada, via `dgb.ops.alerts`;
  - erro passageiro só abre o circuito se durar mais de 24 h.
- **Uso imediato:** enquanto o Passo 3 não chega, `list_agencies` da DAG atual já filtra as agências com circuito aberto.
- **Correções de custo no mesmo passo:**
  - EBC passa a usar a lista de URLs conhecidas;
  - o UPDATE de linha existente só roda se o `content_hash` mudou;
  - parar a republicação em `dgb.news.scraped` de notícia que já está na fila de enriquecimento.

### Passo 3: fila (Cloud Tasks) e Cloud Scheduler; o Airflow sai do caminho
- **infra:**
  - fila `scrape-govbr`: `max_concurrent_dispatches=6`, `max_dispatches_per_second=1`, `max_attempts=3`, backoff de 60 a 600 s, `dispatch_deadline=900s`;
  - SA `scrape-tasks-invoker` com `run.invoker` no scraper-api;
  - a SA do scraper-api ganha `cloudtasks.enqueuer` e `iam.serviceAccountUser` sobre a SA invoker;
  - job do Cloud Scheduler `*/15 6-23 * * *` (America/Sao_Paulo) chamando `/dispatch` com OIDC.
- **scraper-api:**
  - `/dispatch` escolhe as agências vencidas e cria as tarefas, com nome único por agência e horário;
  - `/scrape/agency` raspa uma agência e devolve 2xx para sucesso ou erro permanente e 5xx para erro passageiro;
  - as duas fontes da EBC entram como duas "agências".
- **Airflow:**
  - saem `scrape_agencies` e `scrape_ebc`;
  - entra a DAG manual `scrape_backfill`, com params `agencies` e `start_date`/`end_date`, que chama `/dispatch` com a janela pedida;
  - `scraper_coverage_report` fica;
  - `monitor_scraping_health` dá lugar aos alertas do Passo 4.
- **Custo:** Cloud Tasks (~30 mil operações/mês) e Cloud Scheduler (1 job) cabem no nível gratuito.
- **Rollout:**
  - o dispatcher roda "a seco" por 1 dia, registrando o que despacharia, para comparar com a DAG;
  - depois a fila assume e a DAG é pausada;
  - 3 dias depois as DAGs são removidas.

### Passo 4: raspagem conforme o ritmo e observabilidade
- **Raspagem conforme o ritmo:**
  - o `poll_interval_minutes` é recalculado todo dia, por média móvel exponencial de 28 dias, nas faixas da tabela acima;
  - quando a raspagem encontra notícia nova, o intervalo da agência cai pela metade por 6 h;
  - de madrugada só há uma passada, às 03h, para as faixas de 30 min e 2 h.
- **Métricas**, via logs estruturados e métricas baseadas em log:
  - `scrape_attempts{agency,outcome,category}` e `articles_new{agency}`;
  - as métricas nativas do Cloud Tasks (profundidade da fila e idade da tarefa mais antiga);
  - `freshness_lag_minutes{tier}`, calculado pelo `/dispatch` a partir do estado.
- **Alertas** no canal `ops_alerts_pubsub`, que vai pelo alert-relay até o Telegram:
  - tarefa mais antiga da fila com mais de 30 min;
  - Cloud Scheduler sem execução por 45 min;
  - mais de 20% das agências com erro numa janela de 1 h, que indica gov.br fora do ar ou nosso IP bloqueado;
  - circuito abrindo e fechando (Passo 2).
- **SLOs:**
  - 95% das agências ativas raspadas com sucesso dentro de 2× o próprio intervalo;
  - p90 entre publicação e ingestão abaixo de 45 min para a faixa de 30 min e abaixo de 3 h para a de 2 h. Hoje o p50 é 13 min e o p90 34 min, mas com raspagem a cada 10 min.
- **Painel:** uma página no streamlit-infra-health lendo `scrape_agency_state`, com atraso, circuito, último erro e intervalo de cada agência.

## Ordem em relação à migração do Composer
1. O Passo 1 vai junto com a Fase 3 da migração (deploy das DAGs nos dois buckets), sem bloqueá-la.
2. As Fases 4–7 seguem. Prioridade: desligar o Composer, que custa ~US$ 24/dia.
3. Os Passos 2–4 vêm depois do Composer desligado. Ao final do Passo 3, a VM do Airflow pode encolher (testar e2-medium, −US$ 39/mês).

## Riscos
- **Rate limit no gov.br.** Mesmo com raspagem conforme o ritmo, os picos de despacho são limitados pela fila. Começar com concorrência 6 e 1 requisição/s, e subir só olhando a taxa de `network_error`.
- **Deploy da 037 fora de ordem.** Se o scraper que grava `articles_new` subir antes da migration, os INSERTs falham e o monitor acusa "Sem Execução" em todas as agências. Mitigação: aplicar a 037 antes do merge.
- **Raspagem rara pode atrasar uma notícia rara.** Uma agência da faixa diária que publica fora de hora espera até 24 h. É aceitável pelo volume (2 notícias/dia no total). O "encurtar o intervalo quando há novidade" compensa as agências que voltam a publicar.
- **Fila sem DLQ nativa.** Tarefa que esgota as tentativas some da fila. O estado (falhas seguidas) e o alerta de falhas cobrem isso.

## Verificação
- **Passo 1:**
  - `summarize` verde nas execuções normais;
  - antaq e mec falham numa tentativa só, com `AirflowFailException`;
  - `scrape_runs.articles_new` preenchido e ~242 por dia no total;
  - testes unitários e `pytest -m postgres` verdes.
- **Passo 2:** `scrape_agency_state` com 149 linhas; uma agência derrubada de propósito abre o circuito e gera 1 alerta.
- **Passo 3:** no dia a seco, a lista do dispatcher bate com a da DAG. Na virada, `scrape_runs` mantém a cobertura de 147 agências e ~242 notícias/dia, e a fila zera entre os ciclos.
- **Passo 4:**
  - chamadas por dia ≤ 900;
  - p90 de latência por faixa dentro do SLO;
  - os alertas testados com teste sintético (`"test": true` no `dgb.ops.alerts`).

## Implementação (19/11/2026)

O desenho de implementação dos Passos 2–4 (contrato entre as frentes: scraper, data-platform, infra e streamlit-infra-health) ajustou o plano com base nos dados de 18–19/11.

### Desvios em relação ao plano

| Plano | Implementado | Motivo |
|---|---|---|
| `list_agencies` da DAG filtra o circuito aberto (Passo 2) | A **API** pula a agência (`skipped`, sem raspar nem gravar `scrape_runs`) e a task da DAG termina skipped | A lista da DAG é fixa no parse; filtrar exigiria acesso ao banco e mudaria a `summarize`. A sonda acontece quando vence |
| Circuito aberto testado 1× por dia, às 09h | Sondas com espera crescente: **2 h → 6 h → 24 h**, depois a cada 24 h | Falsos positivos de `html_changed`/`access_denied` duram minutos ou horas (`saude` 3 h, `gestao` 77 min); com teste diário, cada um deixaria a agência ~24 h sem raspar |
| Intervalo pela metade por 6 h em todas as faixas | Pela metade **só nas faixas h2, h6 e d1**; a m30 fica no piso de 30 min | ~1.100 chamadas/dia em vez de ~1.220 |
| `next_due` a partir do fim da raspagem | `next_due_at = início + intervalo − 5 min`, e o `/dispatch` aceita o que vence até `now + 7 min` (meio tick) | Sem a folga, um intervalo de 30 min vira 45 |
| Cloud Scheduler `*/15 6-23` | Scheduler `*/15 * * * *` **o dia todo**; o `/dispatch` decide o modo noturno: das 23:00 às 05:59 só despacha no tick das 03:00 | O alerta de ausência do heartbeat (`dispatch_run`) funciona 24 h, sem falso alarme à noite |
| Painel lê `scrape_agency_state` direto no Postgres | O painel lê `GET /agencies/state` do scraper-api, com ID token | O infra-health é público (allUsers + senha) e não tem VPC; evita pôr nele a credencial do banco |
| — | **Reconciliador** diário (modo `live`, a partir da passada das 03h): republica em `dgb.news.scraped` até 20 notícias sem tema criadas entre now − 7 d e now − 2 h | Sem a republicação da notícia inalterada (Passo 2), sumiu o único retry implícito de um enriquecimento que falhou. O lote ≤ 20 respeita o liveness do enrichment-worker |
| Meta de ≤ 900 chamadas/dia | Meta de **≤ 1.150/dia** | Medido com as faixas reais: 947 sem o encurtamento e ~1.100 com o encurtamento parcial |
| Passada das 03h só para as faixas m30 e h2 | A passada das 03h despacha **tudo o que venceu** (regra do desenho, §3.3; não está na tabela de ajustes dele) | As agências h6 e d1 que vencem de madrugada saem às 03h em vez de esperar até as 06h |

### Decisões da implementação dos Passos 3 e 4 (scraper)

Detalhes que o desenho deixou em aberto, decididos no código (`src/govbr_scraper/dispatch/`):

- **Falha fatal do tick não loga `dispatch_run`:** banco fora, 038 ausente (503), `live` sem as envs `SCRAPE_TASKS_*` (500) e tick `live` em que o Cloud Tasks recusou tarefas e nenhuma foi criada nem duplicada (500, evento `dispatch_failed`: permissão revogada, fila apagada ou desabilitada). Assim o alerta "Nenhuma execução do /dispatch em 45 min" também cobre o despachante que roda mas não consegue despachar. No `off` e nos ticks noturnos o evento sai normalmente. Falha parcial (alguma tarefa criada) é um tick normal, com warning.
- **`off` é a pausa:** `dry_run: true` no corpo rebaixa só o `live`; com a env em `off` (ou inválida) o `/dispatch` não lê o banco, nem no backfill.
- **Cloud Tasks:** depois de 5 falhas seguidas ao criar tarefas, as restantes do tick nem são tentadas (contam em `failed`), para o tick caber no `attempt_deadline` de 300 s do Scheduler mesmo com permissão faltando.
- **Faixas:** a histerese vale contra uma faixa já calculada; a linha criada pelo `ensure_rows` (nunca calculada) recebe a faixa da taxa direto, como no seed. Quando a faixa fica mais rápida e o circuito está fechado, `next_due_at` encurta para `última tentativa + intervalo novo − 5 min` (senão uma agência que sobe de d1 para m30 esperaria até 24 h). `update_tiers` só grava as colunas da faixa, para não sobrescrever o estado que o `record_scrape_run` grava ao mesmo tempo.
- **Reserva diária:** `scrape_dispatch_meta['tiers_recomputed_on']` e `['reconciled_on']` com `{"date", "claimed_at"}`, por `INSERT … ON CONFLICT … DO UPDATE … WHERE` a data é outra `… RETURNING` (o segundo tick concorrente espera o lock e recebe nada). O reconciliador tem a própria chave: uma falha no recálculo não o impede, e sem `PUBSUB_TOPIC_NEWS_SCRAPED` ele não reserva o dia.
- **Reconciliador:** as notícias mais novas primeiro (`ORDER BY created_at DESC`).
- **Frescor:** só nos ticks diurnos (06:00–22:59); `slo_ok_ratio` do evento é o geral (igual em todas as faixas) e `tier_slo_ok_ratio`, o da faixa. Faixa sem nenhuma agência com sucesso não gera evento (não há atraso a medir). Em `dry_run` o SLO usa o piso de 120 min da cadência das DAGs (`slo_interval_floor`, da revisão do Passo 2), como o `GET /agencies/state`.
- **Virada para `live`:** as DAGs de scraping só são pausadas depois do primeiro tick `live` com `dispatched > 0` (o desenho, §6, pausava antes). O `dry_run` nunca chama o Cloud Tasks, então um erro de permissão ou de fila só aparece na virada; com as DAGs no ar, voltar para `dry_run` não deixa buraco (runbook, "Virar para `live`").
- **Idempotência da tarefa do tick:** o corpo leva `dispatched_at` (desvio do contrato, que previa só `agency` e `trigger`). Na primeira entrega, sem datas e sem `force`, o `/scrape/agency` pula a agência já tentada depois do despacho (`already_attempted`), sem raspar nem gravar. O estado "em voo" expira em 30 min; sem isso, a fila parada ou lenta acumulava uma tarefa por agência a cada 30 min, e duas entregas de um `html_changed` passageiro abriam o circuito em segundos.
- **Backfill:** a janela explícita não é fatiada: uma tarefa por agência, que precisa caber nos 900 s (runbook, "Backfill": até ~100 notícias por agência por disparo). Não marca `last_dispatched_at` (a janela explícita não move `last_attempt_at`, e a agência pareceria em voo por 30 min, fora do tick regular) e ignora o modo noturno. A DAG `scrape_backfill` falha sem retry quando o despacho está em `off` ou `dry_run` sem o param `dry_run`, ou quando o Cloud Tasks recusa tarefas (lista as siglas, para disparar de novo só com elas). O nome das tarefas termina no `dedup_key` que a DAG manda (hash do `run_id`) em vez do `HHmm` do contrato: a repetição da task depois de um timeout ou 5xx recebe 409 nas tarefas já criadas.
- **`/scrape/agency`:** `trigger` ausente vale `backfill` com janela explícita e `task` sem; `unknown_agency` gera `scrape_attempt` com `outcome=failed` e `retryable=false`. O `StorageAdapter.close()` agora também fecha o publisher do `dgb.news.scraped`, esperando as publicações pendentes (até 10 s): no Cloud Run a CPU é cortada depois da resposta. Vale também para `/scrape/agencies` e `/scrape/ebc`.

### Estado de cada passo

- **Passo 1** (ajustes rápidos): mergeado em 18/11 (scraper#78).
- **Passo 2** (estado por agência, marca d'água, disjuntor, custo): pronto na branch `feat/ingestao-passo2-estado` (19/11), ainda sem merge. Depende da migration 038 do data-platform aplicada e do PR I-1 do infra.
- **Passo 3** (fila e Scheduler): pronto no scraper na branch `feat/ingestao-passo3-fila` (19/11, sobre a do Passo 2), ainda sem merge: `POST /dispatch` (tick, modos `off`/`dry_run`/`live`, seleção, criação das tarefas, backfill), `POST /scrape/agency` e a DAG `scrape_backfill`. A fila, a SA invocadora, o job do Scheduler e as envs vêm no PR I-2 do infra. As DAGs `scrape_agencies`, `scrape_ebc` e `monitor_scraping_health` ficam até a virada para `live` (runbook, "Despacho por Fila").
- **Passo 4** (ritmo e observabilidade): no scraper, junto com o Passo 3: recálculo diário das faixas com histerese, encurtamento do intervalo (Passo 2), frescor por faixa e os eventos `dispatch_run`/`freshness`. As métricas baseadas em log e os alertas vêm no PR I-2 do infra; o painel "Ingestão", no streamlit-infra-health.
- **Rollout:** 038 → infra I-1 → scraper Passo 2 → painel → scraper Passos 3 e 4 (o `/dispatch` fica em `off` até o infra) → infra I-2 (`dry_run`) → 1 dia a seco → virada para `live` → 3 dias depois, remover as 3 DAGs. Cada merge e cada ação em produção com o OK do usuário.
