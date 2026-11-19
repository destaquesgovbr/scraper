# Runbook Operacional — DestaquesGovBr Scraper

Procedimentos para operar e manter os ~155 scrapers em produção.

## Adicionar Nova Agência Gov.br

1. Identifique a URL da página de notícias da agência (ex: `https://www.gov.br/mme/pt-br/assuntos/noticias`). Use a URL final (depois dos redirects) e uma listagem paginada (`?b_start:int=N` traz outros itens). Uma capa (collective.cover: tiles com `div.collection-item`, só as notícias mais recentes, sem paginação, como a `mec/pt-br/assuntos/noticias` desde 28/10/2026) não serve: use a coleção do link "Mais notícias..." do tile

2. Determine o `scraper_type`:
   - Se a página renderiza a lista de notícias em HTML estático → `html` (omitir campo, é o default)
   - Se a página é React/Volto (conteúdo carregado via JS, `++api++` disponível) → `plone6_api`

3. Edite `src/govbr_scraper/scrapers/config/site_urls.yaml`:
   ```yaml
   agencies:
     nova_agencia:
       url: https://www.gov.br/nova-agencia/pt-br/assuntos/noticias
       active: true
       scraper_type: plone6_api  # omitir para html
   ```

4. **Insira a agência no banco de dados** (obrigatório — sem isso, todos os artigos serão descartados silenciosamente):
   ```sql
   INSERT INTO agencies (key, name, type, url)
   VALUES ('nova_agencia', 'Nome da Agência', 'federal', 'https://www.gov.br/nova-agencia/...')
   ON CONFLICT (key) DO NOTHING;
   ```
   O `storage_adapter.py` faz lookup por `agency_key` no cache do banco. Se a agência não existir, cada artigo é pulado com `logger.warning("Skipping record: unknown agency")` — sem erro visível na DAG.

5. Sincronize o arquivo:
   ```bash
   cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
   ```

6. Abra PR. O CI valida que os YAML estão sincronizados.

7. Após merge, a agência entra como mais uma task mapeada da DAG `scrape_agencies` (map index com a sigla, ex. `nova_agencia`) a partir da próxima execução. Não é criada uma DAG nova.

## Adicionar Endpoint EBC

Edite `src/govbr_scraper/scrapers/config/ebc_urls.yaml` e adicione o novo endpoint. Copie o padrão das entradas existentes.

## Desativar Agência

No `site_urls.yaml`, marque como inativa:
```yaml
agencies:
  agencia_problematica:
    url: https://www.gov.br/...
    active: false
    disabled_reason: "Site fora do ar desde 2026-05-01"
    disabled_date: "2026-05-10"
```

A agência **sai da lista de tasks mapeadas** da DAG `scrape_agencies` (que só mapeia as agências ativas) a partir da próxima execução depois do deploy das DAGs. O monitor também deixa de esperar execução dela. O `load_urls_from_yaml()` loga um resumo das agências filtradas em INFO (motivo individual por agência em DEBUG).

## Migrar Agência para Plone6

Quando uma agência migra de Plone clássico para Plone 6 (Volto/React):

**Descoberta automática via fallback:** Desde PR #60, o sistema tenta automaticamente Plone6APIScraper quando WebScraper falha com HTML_CHANGED. Se a sua agência está falhando consistentemente e o fallback está funcionando, você verá logs com:

```
{agency}: Plone6 API fallback SUCCEEDED. Found {N} articles.
RECOMMENDATION: Update site_urls.yaml to set scraper_type: plone6_api
```

**Identificar agências candidatas:**
```sql
-- Agências com fallback bem-sucedido nos últimos 7 dias
-- (infere sucesso via status='success')
SELECT
    agency_key,
    COUNT(*) as fallback_count,
    MAX(scraped_at) as last_fallback
FROM scrape_runs
WHERE fallback_triggered = true
    AND status = 'success'
    AND scraped_at > NOW() - INTERVAL '7 days'
GROUP BY agency_key
ORDER BY fallback_count DESC;
```

Se uma agência aparece consistentemente (>10x em 7 dias), ela é candidata para migração permanente.

**Procedimento de migração:**

1. Verifique que a API REST está acessível: `curl https://www.gov.br/{agencia}/++api++/pt-br/assuntos/noticias/@search?portal_type=News+Item` (de um IP do Brasil, como a VM do Airflow: de fora o gov.br responde vazio). A resposta tem de ser JSON: a API não acompanha o redirect da página, então use no YAML a URL final da listagem (a `antaq/pt-br/noticias` redireciona para `central-de-conteudos/noticias` desde 16/11/2026, e a API na antiga devolve HTML)
2. Altere `scraper_type` no YAML:
   ```yaml
   agencia_migrada:
     url: https://www.gov.br/agencia/pt-br/assuntos/noticias
     active: true
     scraper_type: plone6_api
   ```
3. Sincronize e abra PR.

## URLs com Ano (Virada do Ano)

Algumas agências só listam as notícias numa pasta ou coleção do ano. Na virada, a do ano novo começa vazia ou nem existe, e a do ano anterior para de receber notícias: a agência segue com sucesso e 0 notícias novas (só o alerta de agência stale do monitor acusa). Em janeiro, confira e troque a URL (com a antiga comentada e o `notes` atualizado):

| Agência | URL atual |
|---------|-----------|
| `casaruibarbosa` | `.../centrais-de-conteudo/noticias/2026` |
| `gsi` | `.../centrais-de-conteudo/noticias/2026` |
| `ibama` | `.../assuntos/noticias/2026` |
| `mec` | `.../assuntos/noticias/colecao-noticias-2026` (a coleção do link "Mais notícias..." da capa `assuntos/noticias`; só lista as 1.000 notícias mais recentes, 50 páginas: em 19/11/2026 ia até 12/06, e o mais antigo não sai por ela) |

## Debugar Scraper em Falha

### 1. Verificar scrape_runs

Conecte no banco e consulte os runs recentes:
```sql
SELECT agency_key, status, error_category, error_message, scraped_at
FROM scrape_runs
WHERE agency_key = 'agencia_com_problema'
ORDER BY scraped_at DESC
LIMIT 10;
```

### 2. Interpretar ErrorCategory

Ver CLAUDE.md § Classificação de Erros para detalhes de cada categoria. A coluna "Na DAG" diz o que a task faz com o erro (ver "Erro Permanente x Passageiro" abaixo).

| Categoria | Na DAG | Ação recomendada |
|-----------|--------|-----------------|
| `NETWORK_ERROR` | passageiro: repete | Aguardar; verificar se é transiente. Inclui 408/429, 5xx depois dos retries, o "Erro de rede ao acessar API" do Plone 6 o `Both scrapers failed` em que o fallback Plone 6 falhou por rede e, na EBC, uma página do índice não baixada depois dos retries (nas páginas depois da 1ª, também 4xx: a paginação interrompida não deixa a marca d'água avançar) |
| `ANTI_BOT` | permanente: falha sem retry | Desativar temporariamente se persistir (ver seção abaixo) |
| `EMPTY_CONTENT` | passageiro: repete | Normal se agência publica pouco (hoje nenhuma exceção gera esta categoria) |
| `URL_BROKEN` | permanente: falha sem retry | 404/410 e outros 4xx permanentes (400, 405...): verificar nova URL no site; atualizar YAML ou desativar |
| `ACCESS_DENIED` | permanente: falha sem retry | 401/403 ou redirect para login: procurar listagem pública; senão desativar (`active: false`) |
| `HTML_CHANGED` | permanente: falha sem retry | Verificar site manualmente; fix no parser. Inclui o `Both scrapers failed` em que o fallback Plone 6 falhou por outro motivo (404 ou resposta não-JSON da API: o site não serve o Plone 6) e, na EBC, a 1ª página do índice sem nenhum link de artigo (`No articles found on first page of EBC index`) |
| `UNKNOWN` | passageiro: repete | Ver `error_message` para detalhes |
| `unknown_agency` (só na resposta da API) | permanente: falha sem retry | Agência fora do `site_urls.yaml` da API ou inativa nele: YAMLs dessincronizados ou imagem da API antiga |

### 3. Verificar logs no Cloud Run

Os logs do scraper-api saem em JSON (`jsonPayload`, com `severity`). Cada agência gera um evento `scrape_attempt` por tentativa (inclusive quando pulada pelo disjuntor) e cada abertura/fechamento do disjuntor um `circuit_transition`:

```bash
# Tentativas de uma agência (outcome, category, duração, faixa, janela)
gcloud logging read 'resource.type="cloud_run_revision"
  AND resource.labels.service_name="destaquesgovbr-scraper-api"
  AND jsonPayload.event="scrape_attempt" AND jsonPayload.agency="mec"' \
  --project=inspire-7-finep --freshness=1d --limit=20 \
  --format='value(timestamp,jsonPayload.outcome,jsonPayload.category,jsonPayload.window_start)'

# Transições do disjuntor
gcloud logging read 'resource.type="cloud_run_revision"
  AND resource.labels.service_name="destaquesgovbr-scraper-api"
  AND jsonPayload.event="circuit_transition"' \
  --project=inspire-7-finep --freshness=7d --limit=50 \
  --format='value(timestamp,jsonPayload.agency,jsonPayload.to_state,jsonPayload.reason)'
```

### 4. Testar localmente

```bash
# Logs legíveis no terminal (o padrão é JSON)
export LOG_FORMAT=text

# Chamar API local para uma agência específica (sem datas: janela pela marca d'água)
curl -X POST http://localhost:8000/scrape/agencies \
  -H "Content-Type: application/json" \
  -d '{"agencies": ["mec"]}'

# Janela explícita (não mexe no estado da agência)
curl -X POST http://localhost:8000/scrape/agencies \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2026-05-18", "agencies": ["mec"]}'
```

## Erro Permanente x Passageiro (DAGs `scrape_agencies` e `scrape_ebc`)

Cada item de `errors[]` da resposta de `/scrape/agencies` e `/scrape/ebc` traz `category` (a mesma gravada em `scrape_runs.error_category`) e `retryable`. O status HTTP não mudou (200 sucesso, 207 parcial, 500 todas falharam).

- **Permanente** (`retryable: false`): `url_broken`, `access_denied`, `html_changed`, `anti_bot` e `unknown_agency`. Repetir na mesma execução não resolve. A task levanta `AirflowFailException` e falha **numa tentativa só**, com a agência e a categoria na mensagem (`Scraping failed for <sigla>: erro permanente (<categoria>), sem retry: ...`). Antes, cada agência quebrada gastava 2 retries de 5 min por execução.
- **Passageiro** (`retryable: true`): `network_error`, `unknown`, `empty_content`, erro sem categoria. A task repete como antes: 2x, a cada 5 min.
- **Sem decisão possível**, também repete: corpo que não é JSON (página de erro do Cloud Run, 503), `{"detail": ...}` de exceção do manager, timeout ou erro de conexão do httpx e API antiga, sem `retryable`.
- **Pulada pelo disjuntor** (agência em `skipped[]`, circuito aberto): a API não raspou e responde 200 (`status: "skipped"` se todas foram puladas). A task termina **skipped**, sem retry. A `scrape_ebc` também tem uma task mapeada por fonte (`agencia_brasil`, `tvbrasil`).
- Só contam os erros da própria agência (ou fonte EBC) da task.

Limpar (Clear) uma task que falhou por erro permanente não adianta: corrija o YAML ou o parser ("Debugar Scraper em Falha"), ou desative a agência.

**Na fila (`POST /scrape/agency`, Passo 3)** a mesma classificação vira status HTTP, porque o Cloud Tasks repete qualquer resposta que não seja 2xx (até 3 tentativas, 60 s e 120 s depois):

| Resultado | HTTP | `status` | O Cloud Tasks |
|-----------|------|----------|---------------|
| Sucesso | 200 | `completed` | encerra |
| Erro permanente (`retryable: false`) | 200 | `failed` | encerra |
| Circuito aberto (sonda não vencida) | 200 | `skipped` | encerra |
| Tarefa repetida: agência já tentada depois do despacho (`reason: already_attempted`) | 200 | `skipped` | encerra |
| Agência fora do YAML ou inativa | 200 | `unknown_agency` | encerra |
| Corpo inválido (JSON quebrado, sem `agency`, data inválida) | 200 | `invalid_payload` | encerra (nunca 422) |
| Erro passageiro, banco fora, exceção | 503 | `failed` | repete |

**Tarefa repetida.** A tarefa do tick leva `dispatched_at` (o horário do tick). Na primeira entrega (`X-CloudTasks-TaskRetryCount` 0 ou ausente), sem datas e sem `force`, a agência cuja última tentativa começou depois do despacho é pulada sem raspar nem gravar (`scrape_attempt` com `outcome: skipped`, `reason: already_attempted`). Isso cobre a tarefa que o tick recria depois de 30 min de fila parada e a reentrega do Cloud Tasks. A repetição depois de um 503 (contagem 1 ou 2) raspa normalmente.

## Disjuntor e Marca d'Água (`scrape_agency_state`)

Desde o Passo 2 da rearquitetura da ingestão (`_plan/rearquitetura-ingestao.md`), o scraper-api guarda o estado de cada agência na tabela `scrape_agency_state` (migration 038 do data-platform), atualizado na mesma transação do `scrape_runs`.

**Janela pela marca d'água.** As DAGs não mandam mais datas. Cada agência raspa desde `(last_success_at − 1 h)` (em datas de Brasília) até hoje, limitado a 7 dias ou aos dias que somam ~80 notícias da agência (pela `rate_ewma_per_day`), o que for menor: a `agencia_brasil` (~40/dia) volta no máximo 1 dia antes de hoje, a `pf` 3, a `tvbrasil` 4, as outras 7. Dentro desse limite, a queda se recupera sozinha na execução seguinte; antes, a janela fixa de 9 h perdia o que caísse fora dela. Sem a tabela (038 não aplicada) ou sem histórico da agência, a janela é de ontem a hoje.

O limite existe porque a raspagem de uma agência tem de caber numa requisição (900 s no Cloud Run, 10 min por task na `scrape_agencies`, 15 min por fonte na `scrape_ebc`) e porque o enrichment-worker cai com ~40 mensagens de uma vez (as notícias são publicadas em lotes de 20, com 30 s entre eles). **Janela cortada** sai no log: `jsonPayload.event="scrape_attempt" AND jsonPayload.window_truncated=true` (e o warning `janela automática limitada`). Os dias de fora não são raspados sozinhos e, depois do sucesso, a marca d'água passa por cima deles: recupere com datas ("Recuperar o período perdido", em "Agências Sem Execução").

**Disjuntor.**

- **Abre** com 2 erros permanentes (`url_broken`, `access_denied`, `html_changed`, `anti_bot`) desde o último sucesso, ou com 24 h de falhas seguidas de qualquer tipo. Sai um alerta 🔴 `Disjuntor aberto: <sigla>` no Telegram da equipe (via `dgb.ops.alerts`, publicado pelo scraper-api).
- **Aberto**, a agência não é raspada: a API responde `skipped` (`reason: circuit_open`, `next_probe_at`), não grava `scrape_runs`, e a task mapeada da `scrape_agencies` termina **skipped** (rosa na UI), sem contar como falha na `summarize`. O monitor a tira de "Agências Sem Execução" e de "Falhas Consecutivas" e a lista na seção "Circuito Aberto", que entra no dedup: enquanto o circuito estiver aberto, ela é lembrada a cada `scraper_alert_repeat_hours` (24 h). O alerta 🔴 é publicado uma vez só, sem retry; se ele se perder, essa seção é o aviso.
- **Sondas:** a primeira 2 h depois da abertura; depois de cada sonda que falha, 6 h, 24 h e daí em diante a cada 24 h. Com as DAGs de 2 em 2 h, a sonda roda na primeira execução depois do vencimento (tolerância de 7 min). Sonda que falha não gera alerta.
- **Fecha** na primeira sonda com sucesso: alerta 🟢 `Disjuntor fechado: <sigla>`.

**Consultar o estado** (somente leitura):

```sql
SELECT agency_key, circuit_state, next_due_at, circuit_failed_probes,
       consecutive_permanent_failures, last_success_at, last_error_category,
       LEFT(last_error_message, 120) AS erro
FROM scrape_agency_state
WHERE circuit_state <> 'closed' OR consecutive_permanent_failures > 0
ORDER BY circuit_state, agency_key;
```

Ou `GET /agencies/state` do scraper-api (com ID token; é o que o painel "Ingestão" do streamlit-infra-health lê).

**Depois de corrigir a agência** (YAML ou parser, ver "Debugar Scraper em Falha"), não é preciso esperar a sonda: dispare a `scrape_agencies` (ou a `scrape_ebc`) com o param **`force: true`**. Ele ignora o disjuntor em todas as agências da execução; o sucesso fecha o circuito (`forced_success`, alerta 🟢). Pela CLI, na VM:

```bash
af dags trigger scrape_agencies --conf '{"force": true}'
```

Com `start_date`/`end_date` (janela explícita, para recuperar um período), a tentativa grava `scrape_runs`, mas **não mexe no estado**: não move a marca d'água nem fecha o circuito. Para fechar e recuperar, rode primeiro com `force: true` e depois com as datas.

**Com o despacho em `live`** (Passo 3, as DAGs de scraping pausadas), a sonda roda sozinha quando vence (2 h, 6 h, 24 h). Para fechar antes, chame o `/scrape/agency` da agência com `force` (sem datas: o sucesso fecha o circuito e move a marca d'água). Precisa de `roles/run.invoker` no scraper-api:

```bash
URL=https://destaquesgovbr-scraper-api-990583792367.southamerica-east1.run.app
curl -sS -X POST "$URL/scrape/agency" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{"agency": "mec", "force": true}'
```

Para recuperar o período perdido depois, use a DAG `scrape_backfill` ("Backfill (DAG `scrape_backfill`)").

**Alerta que não chegou.** Sem a env `PUBSUB_TOPIC_OPS_ALERTS` no scraper-api (Terraform, repo `infra`) ou sem `roles/pubsub.publisher` da SA dele em `dgb.ops.alerts`, o alerta só aparece no log (`... não definido: alerta não publicado` ou `Falha ao publicar alerta`). O evento `circuit_transition` sai no log de qualquer forma.

## Despacho por Fila (`POST /dispatch`, Cloud Tasks)

Passos 3 e 4 da rearquitetura da ingestão (`_plan/rearquitetura-ingestao.md`). O Airflow sai do caminho crítico: o Cloud Scheduler `scrape-dispatch` chama o `POST /dispatch` do scraper-api a cada 15 min, o dia todo, com `{"reason": "tick"}`. O `/dispatch` lê `scrape_agency_state` e cria uma tarefa por agência vencida na fila `scrape-govbr` do Cloud Tasks, que chama o `POST /scrape/agency` (no máximo 6 ao mesmo tempo e 1 nova por segundo, para proteger o www.gov.br). Recursos no repo `infra` (`terraform/scrape-dispatch.tf`, PR I-2).

**A cada tick:**

1. cria a linha das agências ativas que ainda não têm (`ensure_rows`);
2. no primeiro tick do dia depois das 03:00 BRT, **recalcula as faixas** (ver abaixo);
3. **modo noturno:** das 23:00 às 05:59 só despacha no tick das 03:00 (a passada das 03h, que despacha tudo o que venceu); nos outros ticks da noite, só loga;
4. **seleciona** as agências ativas que vencem até `now + 7 min`, com o circuito fechado ou com a sonda vencida, e que não estão **em voo** (despachadas há menos de 30 min sem tentativa depois); ordena pela mais atrasada, até 200 por tick;
5. cria as tarefas (`scrape-<sigla>-<AAAAMMDDHHmm do tick em BRT>`: o nome repetido dá 409 e conta como duplicada), com `{"agency", "trigger": "task", "dispatched_at"}` no corpo, e grava `last_dispatched_at` das criadas e duplicadas. Erro do Cloud Tasks numa tarefa só vira warning e conta em `failed`; depois de 5 falhas seguidas o tick para de tentar (permissão, fila inexistente). Se o Cloud Tasks recusar **todas** (nenhuma criada nem duplicada), o tick termina em erro: 500 e evento `dispatch_failed`, sem o `dispatch_run`;
6. nos ticks diurnos, loga o **frescor** por faixa (evento `freshness`);
7. no modo `live`, 1× por dia a partir das 03:00, roda o **reconciliador** (ver abaixo).

**Modos** (env `SCRAPE_DISPATCH_MODE` do scraper-api, de `var.scrape_dispatch_mode` no repo `infra`):

| Modo | O que faz |
|------|-----------|
| `off` (padrão sem a env) | Só loga `dispatch_run` e responde. Não lê o banco. É a **pausa** |
| `dry_run` (padrão do Terraform) | Faz tudo, menos criar tarefas e reconciliar: seleciona, loga o que despacharia e recalcula as faixas (que **são gravadas**) |
| `live` | Cria as tarefas, marca o despacho e roda o reconciliador |

`{"dry_run": true}` no corpo rebaixa `live` para `dry_run` naquela chamada; `off` continua `off`. Valor desconhecido na env vale `off`.

**Logs** (`jsonPayload.event`):

- `dispatch_run` (um por chamada que terminou, em todos os modos e também nos ticks noturnos): `mode`, `reason`, `night_skip`, `due`, `dispatched`, `duplicates`, `skipped_inflight`, `tiers_recomputed`, `reconciled`, mais `failed`, `overflow`, `tiers_changed`, `slo_ok_ratio` (geral; nulo nos ticks noturnos) e `agencies` (as siglas selecionadas). É o heartbeat do alerta "Nenhuma execução do /dispatch em 45 min". Um tick que falha responde 500/503 e **não** loga `dispatch_run`, de propósito, para o alerta de ausência disparar: banco fora, 038 ausente, `live` sem as envs do Cloud Tasks e `live` em que o Cloud Tasks recusou todas as tarefas.
- `dispatch_failed` (ERROR, no lugar do `dispatch_run`): o tick `live` em que nenhuma tarefa foi criada nem duplicada e houve recusa do Cloud Tasks. Leva as mesmas contagens e o `error` (`o Cloud Tasks recusou N de N tarefa(s) e nenhuma foi criada: HTTP 403: ...`).
- `freshness` (um por faixa, nos ticks diurnos): `tier`, `lag_p50_min`, `lag_p90_min`, `lag_max_min`, `agencies`, `slo_ok_ratio` (geral) e `tier_slo_ok_ratio`. Entram as agências ativas de circuito fechado, sem a `tvbrasil` (o `published_at` dela é o horário da grade). Em `dry_run` o SLO usa o piso de 120 min da cadência das DAGs, como o painel (`GET /agencies/state`); em `live`, o intervalo de cada agência.
- `scrape_attempt` com `trigger: "task"` (tick) ou `"backfill"`, e `task_name`/`task_retry` dos cabeçalhos do Cloud Tasks. A tarefa repetida sai com `outcome: skipped` e `reason: already_attempted` (mais `dispatched_at` e `last_attempt_at`).

```bash
# Últimos ticks (modo, vencidas, despachadas, duplicadas, em voo)
gcloud logging read 'resource.type="cloud_run_revision"
  AND resource.labels.service_name="destaquesgovbr-scraper-api"
  AND jsonPayload.event="dispatch_run"' \
  --project=inspire-7-finep --freshness=3h --limit=12 \
  --format='value(timestamp,jsonPayload.mode,jsonPayload.night_skip,jsonPayload.due,jsonPayload.dispatched,jsonPayload.duplicates,jsonPayload.skipped_inflight,jsonPayload.failed)'

# Tarefas na fila agora (somente leitura)
gcloud tasks list --queue=scrape-govbr --location=southamerica-east1 \
  --project=inspire-7-finep --limit=50
```

**Faixas** (recálculo diário, `dry_run` e `live`). A taxa é a do seed da 038: notícias por dia BRT de `published_at` nos 28 dias completos anteriores a hoje (0 nos dias sem notícia), com peso `0,5^(idade/7)` (idade 0 = ontem). ≥ 5/dia `m30` (30 min), ≥ 1 `h2` (2 h), ≥ 0,2 `h6` (6 h), senão `d1` (1× por dia). Histerese de 20%: só sobe se a taxa passar o limite da faixa nova × 1,2, e só desce abaixo do limite da atual × 0,8. A linha que nunca passou pelo cálculo (criada pelo `ensure_rows`) recebe a faixa da taxa direto. Quando a faixa fica mais rápida (e o circuito está fechado), o vencimento encurta para `última tentativa + intervalo novo − 5 min`. O dia fica reservado em `scrape_dispatch_meta['tiers_recomputed_on']` (`INSERT … ON CONFLICT … WHERE`): dois ticks simultâneos não recalculam duas vezes. Log `tiers_recomputed` com as mudanças (`changes`). Para recalcular de novo no mesmo dia, apague a chave (`DELETE FROM scrape_dispatch_meta WHERE key = 'tiers_recomputed_on'`); o próximo tick recalcula.

**Reconciliador** (só `live`, 1× por dia a partir das 03:00, reservado em `scrape_dispatch_meta['reconciled_on']`). Republica em `dgb.news.scraped` até 20 notícias sem tema (`most_specific_theme_id IS NULL`) criadas entre 7 dias e 2 h atrás, as mais novas primeiro, no mesmo formato do scraper. É o único retry de um enriquecimento que falhou. O lote pequeno respeita o liveness do enrichment-worker. Sem `PUBSUB_TOPIC_NEWS_SCRAPED`, não reserva o dia e só loga warning.

**Conferir o dia a seco (`dry_run`).** Compare a lista do `/dispatch` com a das DAGs: `jsonPayload.agencies` dos `dispatch_run` contra as tasks mapeadas da `scrape_agencies` do mesmo período. Nos ticks entre as execuções de 2 em 2 h, as agências `m30` aparecem vencidas (as DAGs ainda são o relógio): é o esperado. O `slo_ok_ratio` do `freshness` e do `dispatch_run` já desconta isso (piso de 120 min fora do `live`); os percentis de atraso (`lag_p90_min`) são os reais, então a `m30` fica perto de 2 h até a virada. Confira também as faixas (`SELECT tier, COUNT(*) FROM scrape_agency_state GROUP BY 1`) e o volume previsto (~1.100 chamadas/dia).

**Virar para `live`** (depois de 1 dia em `dry_run` sem surpresas). O `dry_run` nunca chama o Cloud Tasks (nem o backfill, que exige `live`): um erro de permissão ou de fila só aparece na virada. Por isso as DAGs só são pausadas depois de o `live` provar que cria tarefas:

1. Logo depois de uma execução de hora par terminar, PR no repo `infra` com `scrape_dispatch_mode = "live"`; merge e apply (o deploy do Cloud Run troca a env).
2. No tick seguinte, conferir: `dispatch_run` com `mode: live` e `dispatched > 0`; `scrape_attempt` com `trigger: "task"` e `outcome: "success"`; a fila esvaziando entre os ticks. Um 500 com `dispatch_failed` no log é o Cloud Tasks recusando as tarefas ("Problemas comuns"): volte para `dry_run` (PR no `infra`) com as DAGs ainda no ar.
3. Só então, antes da próxima hora par, pause na VM (`af dags pause`) a `scrape_agencies`, a `scrape_ebc` e a `monitor_scraping_health`. Se a hora par chegar antes, nada se perde: algumas agências são raspadas duas vezes, sem duplicar notícia.
4. Nos dias seguintes, `scrape_runs` mantendo as ~149 agências.

   Com a `monitor_scraping_health` pausada, some também a seção "Circuito Aberto", o lembrete diário de agência com o disjuntor aberto. Fica o alerta 🔴 do scraper-api, publicado uma vez só, sem retry: confira os circuitos abertos no painel "Ingestão" (ou em `summary.circuit_open` do `GET /agencies/state`) até existir um alerta do Cloud Monitoring para eles.
5. Três dias depois, PR no scraper removendo as 3 DAGs (e as seções delas neste runbook).

**Pausar o despacho:** PR no repo `infra` com `scrape_dispatch_mode = "off"`. O job do Scheduler continua chamando (o heartbeat fica), mas nada é despachado; tarefas já na fila ainda rodam. Para voltar à ingestão pelo Airflow, despause a `scrape_agencies` e a `scrape_ebc` na VM. Não mude a env direto no Cloud Run: o Terraform a reverte no próximo apply.

**Problemas comuns:**

- `dispatch_run` com `failed > 0` (parte das tarefas recusada): veja o warning `Falha ao criar a tarefa ...` (HTTP 403: falta `roles/cloudtasks.enqueuer` na fila ou `roles/iam.serviceAccountUser` na SA `scrape-tasks-invoker`; 404: fila inexistente; 400 `FAILED_PRECONDITION`: fila desabilitada). A agência não é marcada e entra no próximo tick.
- 500 no `/dispatch` com `dispatch_failed` no log (`o Cloud Tasks recusou N de N tarefa(s)`): **nenhuma** tarefa criada, nenhuma agência sendo raspada. Mesmas causas do item anterior (o `error` traz o HTTP e o corpo da API). Sem `dispatch_run`, o alerta "despachante sem heartbeat" dispara em 45 min. Até corrigir, volte para `dry_run` e despause as DAGs de scraping.
- Fila parada (pausada à mão, ou o scraper-api recusando o OIDC da `scrape-tasks-invoker`): nos primeiros 30 min depois do despacho as agências aparecem em `skipped_inflight`; depois o estado "em voo" expira e o tick cria uma tarefa nova para cada agência ainda vencida, a cada 30 min (o que cresce é `dispatched`). Ao retomar, a primeira tarefa de cada agência raspa e as repetidas saem como `skipped` com `reason: already_attempted`, sem raspar nem gravar. O alerta "Fila travada" acusa depois de 30 min. Veja o log da fila (`resource.type="cloud_tasks_queue"`) e os 401/403 do scraper-api.
- 503 no `/dispatch`: a 038 não está aplicada. 500 em `live` com `SCRAPE_TASKS_...` na mensagem: faltam as envs do Cloud Tasks no scraper-api (infra I-2).
- Tarefa que esgota as 3 tentativas some da fila (não há DLQ): a agência fica com a falha em `scrape_agency_state`, entra de novo quando vencer, e o disjuntor abre depois de 24 h de falhas.

## Backfill (DAG `scrape_backfill`)

DAG manual (`schedule=None`) que chama o `POST /dispatch` com `{"reason": "backfill", ...}`. O `/dispatch` cria **uma tarefa por agência** com a janela explícita (`backfill-<sigla>-<início>-<fim>-<dedup_key>`, em que `dedup_key` são 12 hex do hash do `run_id`; numa chamada sem ele, o `HHmm` do relógio) e responde na hora; a raspagem roda na fila. A janela explícita não mexe no estado da agência (marca d'água, disjuntor) e o backfill não marca `last_dispatched_at` (o tick regular segue normal).

Params:

| Param | Uso |
|-------|-----|
| `agencies` | Lista de siglas (`mec`, `agencia_brasil`...). Vazia = todas as ativas. Sigla fora das ativas é ignorada e listada em `unknown_agencies` |
| `start_date`, `end_date` | Obrigatórios, `YYYY-MM-DD`, datas de Brasília |
| `force` | Inclui as agências com o circuito aberto (sem ele, vão para `skipped_circuit`) |
| `dry_run` | Só lista o que seria despachado |

```bash
# Na VM (ver "Agências Sem Execução" para o alias af)
af dags trigger scrape_backfill --conf '{"agencies": ["mec"], "start_date": "2026-11-10", "end_date": "2026-11-12"}'
```

A task falha **sem retry** com params inválidos, com o despacho em `off` ou em `dry_run` (sem o param `dry_run`, nada é criado: use a recuperação pelos params `start_date`/`end_date` da `scrape_agencies` enquanto ela existir) e quando o Cloud Tasks recusa tarefas (a mensagem lista as siglas: dispare de novo só com elas). Erro de rede, timeout (300 s) e 5xx repetem uma vez, 5 min depois. Como o nome das tarefas vem do `run_id`, a repetição não raspa de novo o que a primeira chamada já criou: essas voltam 409 e contam como `duplicates`. Um disparo novo (outro `run_id`) cria tudo de novo.

**Tamanho da janela.** Cada tarefa raspa a janela inteira da agência numa requisição só: 900 s (`dispatchDeadline` da tarefa e timeout do Cloud Run), sem o limite de ~80 notícias da marca d'água. A raspagem leva ~2 s por notícia no gov.br e ~3–4 s na EBC, e a publicação, 30 s a cada 20 notícias (lotes do `dgb.news.scraped`). As notícias só são gravadas depois de raspada a janela inteira. Estourado o prazo, o Cloud Run responde 504 e o Cloud Tasks repete a tarefa, mas a requisição original **continua rodando** no Cloud Run e pode terminar e gravar enquanto a repetição raspa a mesma janela em paralelo (até 3 tentativas). O `ON CONFLICT` evita notícia duplicada, mas o trabalho (e a carga no gov.br) dobra ou triplica. Mantenha cada disparo em até ~100 notícias por agência, como na recuperação pela `scrape_agencies`: para a `agencia_brasil`, fatias de 2 dias; para a `pf` e a `tvbrasil`, de 4; para as outras, a semana inteira cabe. Um disparo com `agencies` vazio manda a mesma janela para todas: com mais de 2 dias, a tarefa da `agencia_brasil` pode estourar o prazo (e as da `pf` e da `tvbrasil`, com mais de 4), gastar as 3 tentativas e falhar. Refaça só elas em fatias. Sem `allow_update`, a notícia já gravada só é regravada (e republicada) se o conteúdo raspado mudou (`content_hash`); a cerca de URLs conhecidas (os 200 URLs mais recentes da agência) nem a raspa de novo, mas num período mais antigo que eles ela é raspada e, com o conteúdo diferente (como nas `plone6_api` depois da correção da ordem dos blocos, em 19/11/2026), atualizada e republicada.

**Paginação funda no HTML do gov.br.** O gov.br responde por vários Varnish, cada um com a sua cópia de cada página da listagem (`?b_start:int=N`), de idades diferentes: em 19/11/2026 a página 0 da coleção da `mec` veio numa cópia de 2 h antes (cabeçalho `Date`; o `Age` vem sempre 0), e `Cache-Control: no-cache` não muda isso. Com notícias publicadas entre as cópias de duas páginas seguidas, a fronteira repete itens (deduplicados no INSERT) ou pula itens: num passeio de 9 páginas da `mec` (28/10 a 19/11), 5 e 12 das 173 notícias ficaram de fora em duas passadas. A janela regular (1 a 7 dias, quase sempre na página 0) quase não sente; o backfill de uma agência HTML com muitas páginas sente. Por isso: dispare a recuperação longe das publicações (à noite, horas depois da última notícia do dia, as cópias tendem a coincidir), em fatias de **um dia** (`start_date` = `end_date`) e confira depois a contagem por dia contra a listagem. A cerca de URLs conhecidas vale também na janela explícita: uma repetição do mesmo dia para depois de 3 notícias já gravadas seguidas, então só recupera a notícia pulada que não tiver 3 conhecidas acima dela no mesmo dia. Como as janelas são de dias inteiros, a fatia de um dia fica inteira dentro ou inteira fora de qualquer outra raspagem (inclusive a da marca d'água), e a ordem dos disparos não importa; já uma fatia de vários dias cujo último dia foi raspado antes para no topo (cerca) e perde os dias mais antigos dela. Notícia antiga, fora do cache do gov.br, leva ~7–10 s por artigo (contra ~2–3 s nas recentes). Na contagem por dia, conte pela data de cada item na listagem (ou pelo `datePublished` do artigo), não pela posição: a coleção da `mec` não está estritamente em ordem de publicação (em 19/11/2026, 7 de 120 itens vinham acima de um mais novo do mesmo dia, ex.: o das 15h51 de 15/11 acima do das 18h30), e o `WebScraper` para no primeiro item mais antigo que `start_date`. Uma inversão que atravesse a meia-noite encerraria a fatia antes da hora e pularia, sem aviso, o que vem abaixo; não houve nenhuma nos itens vistos (a repetição de itens na fronteira de páginas de cópias diferentes, acima, parece uma, mas é do cache).

**Notícias que mudam de pasta.** A cerca de URLs conhecidas e o casamento por URL do INSERT comparam `(agency_key, url)`; notícia que a agência mudou de lugar volta como nova, e só o `ON CONFLICT (unique_id)` (título + `published_at`) evita a duplicata, se o título e o horário não foram editados depois. A `antaq` moveu em 11/2026 as notícias até 03/11 para `central-de-conteudos/noticias/AAAA/<slug>`, e a maioria está no banco sem o ano: numa janela de 22/10 em diante, 2 das 13 movidas (uma de 22/10 e uma de 23/10, com outro `unique_id` hoje) entrariam de novo. Não faça backfill da `antaq` antes de 05/11/2026 (`notes` no `site_urls.yaml`).

Uso típico depois da virada: recuperar os dias que a janela automática deixou de fora (`window_truncated` no `scrape_attempt`; "Disjuntor e Marca d'Água"). O resultado de cada agência sai no log do scraper-api (`jsonPayload.event="scrape_attempt" AND jsonPayload.trigger="backfill"`) e em `scrape_runs`.

## Execução Vermelha da `scrape_agencies` (task `summarize`)

A task `summarize` (`trigger_rule="all_done"`, `retries=0`) roda depois de todas as agências, com ou sem falha, e é a **única folha** da DAG: a cor do DagRun é a dela. Agência que falhou aparece vermelha na própria task mapeada (Mapped Tasks), mas a execução fica verde.

O log da `summarize` resume a execução:

```
Resumo scrape_agencies: 140/147 agências OK, 5 com falha (3.5% das tentadas, limite 10%), 2 puladas (circuito aberto); 231 notícias novas, 260 linhas gravadas
Agências com falha: anvisa, capes, ...
Agências puladas (circuito aberto): antaq, mec
```

Agência pulada pelo disjuntor (ver "Disjuntor e Marca d'Água") não é falha: a task mapeada termina skipped e a `summarize` a conta à parte, pela XCom `skipped` que a task grava antes de pular.

Ela só falha (`AirflowFailException`, execução vermelha) se:

- **nenhuma agência funcionou** (`Nenhuma agência raspada com sucesso`): scraper-api fora do ar, token IAM, `scraper_api_url` errada, gov.br inteiro fora ou nosso IP bloqueado. Se todas foram puladas pelo disjuntor (nenhuma tentada), fica verde;
- **a fração de agências com falha, entre as tentadas (sem as puladas), passou de `scraper_max_failed_ratio`** (Airflow Variable, fração entre 0 e 1, padrão `0.10`; valor inválido vale 0,10). Mensagem: `N de M agências tentadas falharam (X%, acima do limite de Y%): <siglas>`.

Vermelho, portanto, quer dizer problema de plataforma: comece pelo scraper-api (logs do Cloud Run) e pela taxa de `network_error` em `scrape_runs`. Para mudar o limite, edite a Variable (ex.: `0.15` num período de instabilidade conhecida do gov.br) e volte ao padrão depois.

**Recuperar uma execução vermelha.** Muitas vezes não precisa: a execução seguinte, 2 h depois, raspa cada agência desde o último sucesso dela (marca d'água, até 7 dias ou ~80 notícias da agência) e cobre a que falhou. Para refazer a própria execução (depois de corrigir a causa), limpe as `scrape` com falha **junto com a `summarize`**. Limpar só as `scrape` raspa as agências de novo, mas o DagRun termina `failed` outra vez: a `summarize`, única folha, continua `failed` e o resumo não é recalculado. Comandos em "Agências Sem Execução", Correção: a CLI com `-d` ou as duas chamadas da REST API (`scrape` e depois `summarize`), na VM do Airflow. Na UI, limpe primeiro as tasks `failed` em Mapped Tasks e depois a `summarize` (Clear sem Upstream/Downstream). Se a `summarize` for limpa antes das `scrape`, ela roda na hora (`all_done`) e falha de novo.

As falhas são deduzidas pela ausência: cada `scrape` com sucesso devolve `{agency, articles_new, articles_saved}` por XCom, e a `summarize` compara com a lista de agências ativas do parse. Uma agência que falhou não deixa XCom (o Airflow só grava o retorno de task com sucesso); a pulada deixa só a XCom `skipped`.

## Concorrência (`SCRAPE_MAX_CONCURRENT_AGENCIES`)

O número de agências raspadas ao mesmo tempo (`max_active_tis_per_dag` da task `scrape`) vem do env `SCRAPE_MAX_CONCURRENT_AGENCIES` dos componentes do Airflow, lido **no parse** da DAG:

- ausente ou vazio: 4 (o valor de sempre);
- inválido (`abc`, `2.5`): 4, com aviso no log do parse (um erro no import faria a DAG sumir);
- 0 ou negativo: 1 (com 0 o scheduler nunca roda nenhuma task mapeada e, com `max_active_runs=1`, a DAG para).

O limite existe para proteger o www.gov.br (as ~147 agências estão no mesmo host), não o scraper-api. Suba devagar, olhando a taxa de `network_error`. A variável vem do Terraform (repo `infra`: `terraform/templates/airflow/airflow.env.tftpl`, o env dos containers da VM); enquanto não for definida lá, vale o padrão 4. Mudar o valor exige um novo parse da DAG (na VM, mudar o env recria os containers do Airflow) e cria uma DagVersion nova. Outros limites continuam valendo: `max_active_tasks` da DAG, pools e `parallelism` do Airflow.

## Agências Sem Execução (DAG `scrape_agencies`)

Desde nov/2026 (infra#223) o scraping gov.br é **uma DAG só**, `scrape_agencies`, com uma task mapeada (`scrape`) por agência ativa e a sigla como map index, e a task final `summarize`. Ela roda às 06h, 08h, ..., 22h (BRT, 9 execuções por dia), com no máximo `SCRAPE_MAX_CONCURRENT_AGENCIES` agências ao mesmo tempo (padrão 4, ~4 a 5 min por execução). A `scrape_ebc` segue a mesma agenda. Antes eram ~147 DAGs `scrape_<agencia>`, de 10 em 10 min.

Desde 18/11/2026 tudo roda no Airflow próprio (3.1.8) da VM `destaquesgovbr-airflow`; o Cloud Composer foi desligado. UI em `https://airflow.dgb.app.gov.br` (via IAP). A VM não tem IP público: a CLI do Airflow e o banco de metadados só se alcançam por SSH via IAP, nos containers do docker compose em `/opt/airflow` (`airflow-airflow-scheduler-1`, `-apiserver-1`, `-dag-processor-1`, `airflow-postgres-1`, `airflow-dag-sync-1`).

**Sintoma.** O `monitor_scraping_health` (às 06h30, 08h30, ..., 22h30) alerta "Agencias Sem Execucao": a agência está ativa no `site_urls.yaml`, mas não tem nenhum `scrape_run` (sucesso ou erro) nas últimas `scraper_silent_hours` (default 3 h). Agência com o disjuntor aberto também não grava `scrape_run` (a API a pula), mas fica fora desse alerta: ela aparece na seção "Circuito Aberto" do mesmo monitor (ver "Disjuntor e Marca d'Água"). As outras checagens não veem o caso, porque partem de `scrape_runs`, e a chamada à API nem chegou a acontecer. Com o scraping de 2 em 2 h, o alerta sai quando a agência perde duas execuções seguidas, ou só a das 06h (a anterior é a das 22h do dia anterior).

**Primeiro passo: todas ou só algumas?**

- **Todas (ou quase todas) as agências:** a DAG inteira não rodou. Pode estar pausada (`dags list` mostra `is_paused`), ausente (erro de import no parse), com um DagRun preso em `running` (zumbi, ver abaixo) ou com o scraper-api fora do ar (5xx ou 401 antes de gravar o `scrape_run`).
- **Só algumas:** as tasks mapeadas dessas agências falharam antes de a API gravar o resultado (token, timeout, 5xx do Cloud Run) ou ainda não rodaram (execução atrasada, fila do pool).

**Diagnóstico (somente leitura).**

```bash
# SSH na VM (só via IAP)
gcloud compute ssh destaquesgovbr-airflow --zone=southamerica-east1-a \
  --project=inspire-7-finep --tunnel-through-iap

# --- daqui em diante, na VM ---
# CLI do Airflow no container do scheduler. O stderr traz avisos do Python e logs de
# info; 2>/dev/null limpa a saída, mas esconde erros (tire se o comando não mostrar nada).
af() { sudo docker exec airflow-airflow-scheduler-1 airflow "$@"; }

# 1. Execuções recentes da DAG (estado e horário; o run_id é em UTC)
af dags list-runs scrape_agencies -o plain 2>/dev/null | head

# 2. Estado de cada task mapeada de uma execução (uma linha por agência, com map_index)
af tasks states-for-dag-run scrape_agencies '<run_id>' -o plain 2>/dev/null

# 3. A CLI não mostra a sigla. No banco de metadados (somente leitura), as tasks
#    que não terminaram em sucesso, com a sigla (rendered_map_index):
sudo docker exec -i -e PGOPTIONS='-c default_transaction_read_only=on' airflow-postgres-1 \
  sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT map_index, rendered_map_index AS agencia, state, try_number, end_date
FROM task_instance
WHERE dag_id = 'scrape_agencies' AND task_id = 'scrape' AND run_id = '<run_id>'
  AND state IS DISTINCT FROM 'success'
ORDER BY map_index;
SQL
```

Na UI do Airflow (`https://airflow.dgb.app.gov.br`): DAG `scrape_agencies` → Grid → task `scrape` → aba **Mapped Tasks**. O map index aparece com a sigla da agência; filtre por estado `failed` e abra os **Logs** da tentativa. O `on_failure_callback` também loga `Scrape DAG failure: dag=scrape_agencies task=scrape agency=<sigla> map_index=<N> ...`.

Os logs das tasks ficam no GCS (30 dias), e dá para lê-los do notebook, sem SSH:

```bash
gcloud storage cat --project=inspire-7-finep \
  "gs://destaquesgovbr-airflow/logs/dag_id=scrape_agencies/run_id=<run_id>/task_id=scrape/map_index=<N>/attempt=<K>.log"
```

Os logs dos containers (scheduler, dag-processor, api-server) vão para o Cloud Logging, no log `gcplogs-docker-driver` (recurso `gce_instance`):

```bash
gcloud logging read 'logName="projects/inspire-7-finep/logs/gcplogs-docker-driver"
  AND jsonPayload.container.name="/airflow-airflow-scheduler-1"
  AND jsonPayload.message:"scrape_agencies"' \
  --project=inspire-7-finep --freshness=3h --limit=20 --format='value(timestamp,jsonPayload.message)'
```

**DagRun zumbi (visto no Composer 3, Airflow 3.1.0).** Logo depois de um deploy das DAGs, a DagVersion mais recente de algumas DAGs já ficou gravada sem `serialized_dag` e sem `dag_code`. O scheduler deixa o DagRun em `running` sem task instances e, com `max_active_runs=1`, não cria outro. Ainda não apareceu no Airflow 3.1.8 da VM, mas o procedimento vale igual. No log do scheduler (Cloud Logging, container `/airflow-airflow-scheduler-1`), o erro se repete em loop:

```
DAG 'scrape_agencies' not found in serialized_dag table
Couldn't find DAG scrape_agencies in DAG bag or database!
```

Casos de 2026, ainda com uma DAG por agência: `scrape_inma` (13/04), `scrape_inpe` (11/06), `scrape_aids` e `scrape_patrimonio` (21/08) e `scrape_esg` (sem execução desde 11/11). Hoje um zumbi para **todas** as agências de uma vez, e o monitor acusa na execução seguinte. Para confirmar (somente leitura, na VM):

```bash
# O run em 'running' há horas não tem task instances (resposta vazia no passo 2 acima).
# A REST API responde em localhost:8080 na VM; o GET /auth/token devolve um JWT de admin
# (SimpleAuthManager com all_admins: o IAP é a única porta de entrada).
TOKEN=$(curl -s http://localhost:8080/auth/token | jq -r .access_token)
API=http://localhost:8080/api/v2
curl -s -H "Authorization: Bearer $TOKEN" \
  "$API/dags/scrape_agencies/dagVersions?order_by=-version_number&limit=1"
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $TOKEN" \
  "$API/dagSources/scrape_agencies?version_number=<N>"  # 404 = versão órfã
```

Ou direto no banco de metadados: a versão mais recente com `tem_serialized_dag = f` é órfã.

```bash
sudo docker exec -i -e PGOPTIONS='-c default_transaction_read_only=on' airflow-postgres-1 \
  sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' <<'SQL'
SELECT dv.version_number, dv.created_at,
       sd.id IS NOT NULL AS tem_serialized_dag, dc.id IS NOT NULL AS tem_dag_code
FROM dag_version dv
LEFT JOIN serialized_dag sd ON sd.dag_version_id = dv.id
LEFT JOIN dag_code dc ON dc.dag_version_id = dv.id
WHERE dv.dag_id = 'scrape_agencies'
ORDER BY dv.version_number DESC LIMIT 3;
SQL
```

**Correção.** Todas as ações abaixo mudam estado no Airflow e precisam de aprovação humana. Os comandos rodam na VM (SSH via IAP, função `af` e `TOKEN`/`API` definidos acima).

- **Task mapeada que falhou (algumas agências):** limpe só as tasks com falha da execução, para rodarem de novo, e a `summarize` junto quando a execução ficou vermelha (ver "Execução Vermelha"). Na UI: Mapped Tasks → selecione as tasks `failed` → **Clear**; depois, se a `summarize` falhou, Clear nela. Pela CLI, todas as tasks com falha das execuções do intervalo; `-d` inclui a `summarize` (downstream), que com `--only-failed` só é limpa se falhou:

  ```bash
  af tasks clear scrape_agencies \
    -t scrape -d --only-failed -s <AAAA-MM-DDTHH:MM> -e <AAAA-MM-DDTHH:MM> --yes
  ```

  Apesar do nome `--task-regex`, no Airflow 3.1 (3.1.0 e 3.1.8) o `tasks clear` usa a DAG serializada, cujo `partial_subset` compara `-t` como **substring** do task_id: `-t '^scrape$'` não casa com nada e o comando não limpa nada. `-t scrape` casa só com a `scrape` (não é substring de `summarize`).

  Pela REST API, uma agência só (`task_ids` com `[task_id, map_index]`) e, numa segunda chamada, a `summarize`, que recalcula o resumo e a cor do DagRun (nessa ordem):

  ```bash
  clear_tis() {  # $1 = task_ids em JSON
    curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
      "$API/dags/scrape_agencies/clearTaskInstances" \
      -d "{\"dag_run_id\": \"<run_id>\", \"task_ids\": $1, \"only_failed\": false, \"dry_run\": false}"
  }
  clear_tis '[["scrape", <map_index>]]'
  clear_tis '["summarize"]'
  ```

  Não junte as duas numa chamada nem use `"include_downstream": true` com `[task_id, map_index]`: no Airflow 3.1.0 (o do Composer), quando a seleção tem mais de uma task, o `clearTaskInstances` acrescentava `"scrape"` sem map index à lista e, com `only_failed: false`, limpava as ~147 agências da execução. O 3.1.8, que roda na VM, corrigiu (`exclude_original=True`), mas as duas chamadas separadas continuam sendo o caminho seguro. Na dúvida, rode antes com `"dry_run": true` e confira as task instances devolvidas.

  Se a falha é da agência (site fora, 4xx permanente: a task falhou numa tentativa só, com `erro permanente` na mensagem), limpar não adianta: ver "Debugar Scraper em Falha".
- **DAG pausada:** `af dags unpause scrape_agencies` (ou o botão de pausa na UI).
- **DagRun zumbi:** marcar o run como `failed` **não resolve**, porque o run seguinte também é criado com a versão órfã e trava de novo. Apague os metadados da DAG. O `delete_dag` do Airflow 3.1 remove todas as linhas com o `dag_id`, inclusive `dag_version` e `serialized_dag`. Depois, o dag-processor reparseia `scraper/scrape_agencies.py` e cria uma v1 nova, já serializada:

  ```bash
  af dags delete scrape_agencies --yes
  ```

  Só se perde o histórico de runs da DAG no Airflow. A tabela `scrape_runs` e as notícias no Postgres não são afetadas. A DAG recriada pode nascer pausada: despause.

**Validação.**
1. `af dags list-runs scrape_agencies` mostra uma execução nova, com task instances.
2. O erro `not found in serialized_dag table` some do log do scheduler (no caso do zumbi).
3. O próximo `monitor_scraping_health` não lista mais as agências.

**Recuperar o período perdido.** A execução normal raspa cada agência desde o último sucesso dela (marca d'água), até 7 dias ou os dias que somam ~80 notícias da agência ("Disjuntor e Marca d'Água"): numa queda de 3 dias, só a `agencia_brasil` e a `pf` ficam com dias de fora. Para o que ficou de fora (`window_truncated` no log), um período mais antigo, ou para forçar uma janela, dispare a DAG com os params opcionais `start_date` e `end_date` (YYYY-MM-DD, datas de Brasília, os dois juntos). Eles mandam a mesma janela explícita para todas as agências, que **não mexe no estado** (marca d'água e disjuntor); agência com o circuito aberto continua pulada, a não ser com `force: true`:

- **UI:** DAG `scrape_agencies` → **Trigger** → preencha "Data inicial (recuperação)" e "Data final (recuperação)".
- **CLI:**

  ```bash
  af dags trigger scrape_agencies \
    --conf '{"start_date": "2026-11-15", "end_date": "2026-11-17"}'
  ```

Data em outro formato é recusada no disparo (e a API recusa data inválida com 422). Só uma das duas ou início depois do fim falham as tasks na hora, sem retry e sem chamar a API. Com `max_active_runs=1`, o run manual espera a execução agendada em andamento terminar. Sem `allow_update`, o re-scrape insere o que falta e só regrava (e republica) a notícia já gravada cujo conteúdo raspado mudou (`content_hash`). A janela explícita não tem o limite de ~80 notícias: cada task tem timeout de 10 min (15 min por fonte na `scrape_ebc`, e 900 s no Cloud Run), a raspagem leva ~2 s por notícia no gov.br e ~3–4 s na EBC, e a publicação, 30 s a cada 20 notícias. Mantenha cada execução em até ~100 notícias por agência: para a `agencia_brasil`, fatias de 2 dias; para a `pf` e a `tvbrasil`, de 4; para as outras, a semana inteira cabe. Para recuperar só uma agência, use o re-scrape dirigido ("Forçar Re-scrape Completo"). A `scrape_ebc` aceita os mesmos params.

**Depois de deploys que mudam as DAGs** (`site_urls.yaml`, schedule, `default_args`): confira que a próxima execução da `scrape_agencies` criou task instances e acompanhe o monitor seguinte.

## Tratar Anti-Bot

Quando uma agência retorna `ErrorCategory.ANTI_BOT`:

1. Acesse o site manualmente no navegador para confirmar
2. Se for Cloudflare temporário, geralmente resolve sozinho em horas
3. Se persistir, considere desativar a agência temporariamente com `disabled_reason`
4. O `WebScraper` já implementa random delays entre requests — não é possível ajustar externamente

## Ajustar Retenção de scrape_runs

A tabela `scrape_runs` cresce ~1,4 mil rows/dia (~150 agências × 9 execuções da `scrape_agencies`, mais retries e EBC; até nov/2026, com execuções de 10 em 10 min, eram ~22 mil). O `cleanup_old_scrape_runs` DAG limpa registros antigos.

Para alterar o período de retenção, mude a Airflow Variable:
```
scraper_retention_days = 90  (default)
```

## Forçar Re-scrape Completo

Por padrão, o artigo que já existe (casado pela URL) só é atualizado se o conteúdo raspado mudou (`content_hash`), e o que só casa pelo `unique_id` é ignorado. Para forçar atualização:

```bash
curl -X POST https://{API_URL}/scrape/agencies \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2026-01-01", "end_date": "2026-05-18", "agencies": ["mec"], "allow_update": true}'
```

`allow_update: true` faz `ON CONFLICT (unique_id) DO UPDATE` em vez de `DO NOTHING`. Na resposta, `articles_saved` conta as linhas gravadas (inseridas + atualizadas) e `articles_new` só as inseridas (o `RETURNING ... (xmax = 0)` separa as duas). Com datas a janela é explícita e não mexe no estado da agência; com o circuito aberto a agência volta em `skipped`, a não ser com `"force": true`.

## Verificar Alertas

### Canais

`notify.send_alert()` tenta, nesta ordem, e para no primeiro que entregar:

1. **Telegram direto**, se as Variables `scraper_telegram_bot_token` e `scraper_telegram_monitor_chat_id` existirem (hoje não existem).
2. **Pub/Sub `dgb.ops.alerts`**: o alert-relay (data-platform) repassa ao Telegram da equipe. O tópico é a Variable `scraper_alert_pubsub_topic` ou, por padrão, `projects/$GCP_PROJECT_ID/topics/dgb.ops.alerts`; Variable vazia desliga o canal. A mensagem vai como texto puro (sem `<b>`, até 3400 caracteres, cortada em fim de linha), com título ("Scraper: saúde das agências" ou "Scraper: cobertura baixa"), severidade `warning` e a DAG de origem. A SA do Airflow (`destaquesgovbr-composer`, nome herdado; hoje é a SA da VM) precisa de `roles/pubsub.publisher` no tópico (infra).
3. **Webhook** (`scraper_alert_webhook_url`).
4. **Só log** (`Alert (log-only): ...`).

### Tipos

- **Disjuntor** (scraper-api, direto no `dgb.ops.alerts`, sem passar pelo monitor): 🔴 ao abrir e 🟢 ao fechar o circuito de uma agência, com o motivo e a categoria. Ver "Disjuntor e Marca d'Água".
- **Despacho por fila** (Cloud Monitoring, canal `ops_alerts_pubsub`; repo `infra`, PR I-2): "despachante sem heartbeat" (nenhum `dispatch_run` em 45 min: job parado, scraper-api fora, banco fora ou o Cloud Tasks recusando todas as tarefas do tick), "fila scrape-govbr travada" (tarefas na fila por 30 min seguidos) e "mais de 20% das agências com erro" (na última hora, mais de 20% das agências tentadas tiveram falha e nenhum sucesso, com ao menos 10 agências tentadas; as puladas não contam: gov.br fora do ar ou nosso IP bloqueado). Ver "Despacho por Fila".
- **Agências sem execução:** agência ativa sem nenhum `scrape_run` (sucesso ou erro) em X horas (`scraper_silent_hours`, default 3 h). A task mapeada da agência na `scrape_agencies` não chamou a API; ver "Agências Sem Execução (DAG `scrape_agencies`)". Vem primeiro na mensagem. Agência com o disjuntor aberto fica de fora.
- **Circuito aberto:** agências ativas (gov.br do `site_urls.yaml` e as fontes EBC) com o disjuntor aberto (`open`/`half_open`), com o último erro, a abertura e a próxima sonda. Elas saem das seções de sem execução e de falhas consecutivas. A seção entra no dedup, então serve de lembrete diário de que a agência está fora da raspagem regular, mesmo que o alerta 🔴 do scraper-api (publicado uma vez, sem retry) tenha se perdido.
- **Falhas consecutivas** (sem as agências com o disjuntor aberto): agência com N+ erros seguidos (`scraper_consecutive_failure_threshold`, default 3) na janela `scraper_failure_window_hours` (default 6 h), **ou** cujo `scrape_run` mais recente é um erro permanente (`url_broken`, `access_denied`, `html_changed`, `anti_bot`). Erro passageiro grava até 3 erros por execução, um por tentativa, e 3 seguidos bastam. Erro permanente falha a task sem retry e grava 1 por execução: sem o segundo critério, a agência quebrada só entraria depois de 3 execuções (4 h ou mais) e sairia da seção toda manhã (às 06h30 a janela só tem a execução das 06h, às 08h30 as das 06h e 08h), mudando o resumo do alerta e reenviando-o duas vezes por dia.
- **Agências stale:** sem notícia **nova** em X horas (`scraper_stale_hours`, default 24). Conta `articles_new` (linhas de fato inseridas; migration 037 do data-platform); linhas anteriores à 037 (`articles_new` NULL) usam `articles_saved`. Antes valia `articles_saved`, que soma as linhas atualizadas por re-scrape: a EBC e as agências em fallback Plone 6 relêem a janela inteira e nunca apareciam.
- **Cobertura baixa** (`scraper_coverage_report`, 08h): menos de 80% das agências com sucesso em 24 h. O "Total artigos" do relatório soma `COALESCE(articles_new, articles_saved)` (notícias novas, ~242/dia em nov/2026).

### Sem repetição

O monitor roda 9 vezes por dia, mas só reenvia o alerta quando muda o conjunto de agências **sem execução**, **com falhas consecutivas** ou **com o circuito aberto**, ou quando o último envio tem `scraper_alert_repeat_hours` (default 24 h; `0` desliga o dedup). Mudança só na lista de agências sem notícias (umas 48 crônicas) não reenvia: elas vão na mensagem, mas aparecem no máximo uma vez por dia. O último envio fica na Variable `scraper_monitor_last_alert` (JSON com as siglas e o horário, no metastore). Para forçar o reenvio na próxima execução, apague essa Variable. Sem nada a alertar, o estado é zerado e um problema que volta sai na hora. Erro ao ler ou gravar o estado não impede o envio. No modo log-only o estado não é gravado, então o alerta volta ao log a cada execução.

### Tamanho e log

Cada seção lista no máximo 25 agências (`... e mais N`), e a mensagem é cortada em fim de linha para caber em 4000 caracteres: o Telegram recusa texto acima de 4096, e o Cloud Logging corta a linha `Alert (log-only): ...` em 4096. A lista completa fica nos logs das tasks `check_*`.

Para ler os alertas que só foram logados (`Alert (log-only)`, task `send_alerts`), leia os logs da task no GCS. O `run_id` é em UTC; o exemplo pega as execuções de um dia:
```bash
gcloud storage cat --project=inspire-7-finep \
  "gs://destaquesgovbr-airflow/logs/dag_id=monitor_scraping_health/run_id=scheduled__2026-11-18T*/task_id=send_alerts/attempt=*.log" \
  | grep -E 'Alert \(log-only\)|Pub/Sub|reenviado'
```

Falha do Pub/Sub aparece no log da task como `Failed to publish Pub/Sub alert: HTTP 403 ...` (ex.: falta o `roles/pubsub.publisher`), seguida do fallback.

## Monitorar Fallback de Scraper

O fallback automático WebScraper → Plone6API é acionado quando agências migram para Plone 6 mas ainda estão configuradas como `scraper_type: html`. Este é um mecanismo de resiliência, não uma solução permanente.

**Verificar se fallback está sendo usado:**
```sql
-- Agências com fallback ativo nas últimas 24h
-- (infere sucesso via status='success' + fallback_triggered)
SELECT
    agency_key,
    COUNT(*) as total_runs,
    COUNT(*) FILTER (WHERE fallback_triggered) as fallback_count,
    COUNT(*) FILTER (WHERE fallback_triggered AND status = 'success') as fallback_success_count,
    ROUND(
        COUNT(*) FILTER (WHERE fallback_triggered AND status = 'success') * 100.0
        / NULLIF(COUNT(*) FILTER (WHERE fallback_triggered), 0),
        2
    ) as taxa_sucesso_fallback_pct
FROM scrape_runs
WHERE scraped_at > NOW() - INTERVAL '24 hours'
GROUP BY agency_key
HAVING COUNT(*) FILTER (WHERE fallback_triggered) > 0
ORDER BY fallback_count DESC;
```

**Verificar logs no Cloud Run:**
```bash
gcloud logging read "resource.type=cloud_run_revision \
  AND resource.labels.service_name=destaquesgovbr-scraper-api \
  AND jsonPayload.fallback_triggered=true" \
  --limit 50 --format json
```

**Quando migrar para Plone6 permanentemente:**
- Se fallback está sendo usado >80% das execuções em 7 dias
- Se fallback está bem-sucedido consistentemente (status='success' quando fallback_triggered=true)
- Ver procedimento em "Migrar Agência para Plone6"

**Alertar sobre fallback recorrente:**
Se uma agência usa fallback >10x por dia por >3 dias consecutivos, considere atualizar site_urls.yaml. O fallback adiciona pequeno overhead (tentativa WebScraper + tentativa Plone6).
