# Runbook Operacional — DestaquesGovBr Scraper

Procedimentos para operar e manter os ~155 scrapers em produção.

## Adicionar Nova Agência Gov.br

1. Identifique a URL da página de notícias da agência (ex: `https://www.gov.br/mec/pt-br/assuntos/noticias`)

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

1. Verifique que a API REST está acessível: `curl https://www.gov.br/{agencia}/++api++/pt-br/assuntos/noticias/@search?portal_type=News+Item`
2. Altere `scraper_type` no YAML:
   ```yaml
   agencia_migrada:
     url: https://www.gov.br/agencia/pt-br/assuntos/noticias
     active: true
     scraper_type: plone6_api
   ```
3. Sincronize e abra PR.

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
| `NETWORK_ERROR` | passageiro: repete | Aguardar; verificar se é transiente. Inclui 408/429, 5xx depois dos retries, o "Erro de rede ao acessar API" do Plone 6 o `Both scrapers failed` em que o fallback Plone 6 falhou por rede e, na EBC, a 1ª página do índice não baixada depois dos retries |
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
- **Pulada pelo disjuntor** (agência em `skipped[]`, circuito aberto): a API não raspou e responde 200 (`status: "skipped"` se todas foram puladas). A task termina **skipped**, sem retry. Na `scrape_ebc`, uma fonte pulada não falha a task; as duas puladas, a task fica skipped.
- Na `scrape_agencies` só contam os erros da própria agência da task. Na `scrape_ebc` (duas fontes numa task), a task só falha sem retry se **todos** os erros forem permanentes.

Limpar (Clear) uma task que falhou por erro permanente não adianta: corrija o YAML ou o parser ("Debugar Scraper em Falha"), ou desative a agência.

## Disjuntor e Marca d'Água (`scrape_agency_state`)

Desde o Passo 2 da rearquitetura da ingestão (`_plan/rearquitetura-ingestao.md`), o scraper-api guarda o estado de cada agência na tabela `scrape_agency_state` (migration 038 do data-platform), atualizado na mesma transação do `scrape_runs`.

**Janela pela marca d'água.** As DAGs não mandam mais datas. Cada agência raspa desde `(last_success_at − 1 h)` (em datas de Brasília), limitado a 7 dias, até hoje. Uma queda de até 7 dias se recupera sozinha na execução seguinte; antes, a janela fixa de 9 h perdia o que caísse fora dela. Sem a tabela (038 não aplicada) ou sem histórico da agência, a janela é de ontem a hoje.

**Disjuntor.**

- **Abre** com 2 erros permanentes (`url_broken`, `access_denied`, `html_changed`, `anti_bot`) desde o último sucesso, ou com 24 h de falhas seguidas de qualquer tipo. Sai um alerta 🔴 `Disjuntor aberto: <sigla>` no Telegram da equipe (via `dgb.ops.alerts`, publicado pelo scraper-api).
- **Aberto**, a agência não é raspada: a API responde `skipped` (`reason: circuit_open`, `next_probe_at`), não grava `scrape_runs`, e a task mapeada da `scrape_agencies` termina **skipped** (rosa na UI), sem contar como falha na `summarize`. O monitor também a tira de "Agências Sem Execução" e de "Falhas Consecutivas".
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

**Alerta que não chegou.** Sem a env `PUBSUB_TOPIC_OPS_ALERTS` no scraper-api (Terraform, repo `infra`) ou sem `roles/pubsub.publisher` da SA dele em `dgb.ops.alerts`, o alerta só aparece no log (`... não definido: alerta não publicado` ou `Falha ao publicar alerta`). O evento `circuit_transition` sai no log de qualquer forma.

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

**Recuperar uma execução vermelha.** Muitas vezes não precisa: a execução seguinte, 2 h depois, raspa cada agência desde o último sucesso dela (marca d'água, até 7 dias) e cobre a que falhou. Para refazer a própria execução (depois de corrigir a causa), limpe as `scrape` com falha **junto com a `summarize`**. Limpar só as `scrape` raspa as agências de novo, mas o DagRun termina `failed` outra vez: a `summarize`, única folha, continua `failed` e o resumo não é recalculado. Comandos em "Agências Sem Execução", Correção: a CLI com `-d` ou as duas chamadas da REST API (`scrape` e depois `summarize`), na VM do Airflow. Na UI, limpe primeiro as tasks `failed` em Mapped Tasks e depois a `summarize` (Clear sem Upstream/Downstream). Se a `summarize` for limpa antes das `scrape`, ela roda na hora (`all_done`) e falha de novo.

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

**Sintoma.** O `monitor_scraping_health` (às 06h30, 08h30, ..., 22h30) alerta "Agencias Sem Execucao": a agência está ativa no `site_urls.yaml`, mas não tem nenhum `scrape_run` (sucesso ou erro) nas últimas `scraper_silent_hours` (default 3 h). Agência com o disjuntor aberto também não grava `scrape_run` (a API a pula), mas fica fora desse alerta: o alerta do disjuntor cobre (ver "Disjuntor e Marca d'Água"). As outras checagens não veem o caso, porque partem de `scrape_runs`, e a chamada à API nem chegou a acontecer. Com o scraping de 2 em 2 h, o alerta sai quando a agência perde duas execuções seguidas, ou só a das 06h (a anterior é a das 22h do dia anterior).

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

**Recuperar o período perdido.** Quedas de até 7 dias se recuperam sozinhas: a execução normal raspa cada agência desde o último sucesso dela (marca d'água). Para um período mais antigo, ou para forçar uma janela, dispare a DAG com os params opcionais `start_date` e `end_date` (YYYY-MM-DD, datas de Brasília, os dois juntos). Eles mandam a mesma janela explícita para todas as agências, que **não mexe no estado** (marca d'água e disjuntor); agência com o circuito aberto continua pulada, a não ser com `force: true`:

- **UI:** DAG `scrape_agencies` → **Trigger** → preencha "Data inicial (recuperação)" e "Data final (recuperação)".
- **CLI:**

  ```bash
  af dags trigger scrape_agencies \
    --conf '{"start_date": "2026-11-15", "end_date": "2026-11-17"}'
  ```

Data em outro formato é recusada no disparo (e a API recusa data inválida com 422). Só uma das duas ou início depois do fim falham as tasks na hora, sem retry e sem chamar a API. Com `max_active_runs=1`, o run manual espera a execução agendada em andamento terminar. Sem `allow_update`, o re-scrape só insere o que falta. Cada task tem timeout de 10 min, então em quedas longas (mais de ~3 dias) divida em várias execuções, ou use o re-scrape dirigido por agência ("Forçar Re-scrape Completo"). A `scrape_ebc` aceita os mesmos params.

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

Por padrão, artigos já existentes (por `unique_id`) são ignorados. Para forçar atualização:

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
- **Agências sem execução:** agência ativa sem nenhum `scrape_run` (sucesso ou erro) em X horas (`scraper_silent_hours`, default 3 h). A task mapeada da agência na `scrape_agencies` não chamou a API; ver "Agências Sem Execução (DAG `scrape_agencies`)". Vem primeiro na mensagem. Agência com o disjuntor aberto fica de fora.
- **Falhas consecutivas** (sem as agências com o disjuntor aberto): agência com N+ erros seguidos (`scraper_consecutive_failure_threshold`, default 3) na janela `scraper_failure_window_hours` (default 6 h), **ou** cujo `scrape_run` mais recente é um erro permanente (`url_broken`, `access_denied`, `html_changed`, `anti_bot`). Erro passageiro grava até 3 erros por execução, um por tentativa, e 3 seguidos bastam. Erro permanente falha a task sem retry e grava 1 por execução: sem o segundo critério, a agência quebrada só entraria depois de 3 execuções (4 h ou mais) e sairia da seção toda manhã (às 06h30 a janela só tem a execução das 06h, às 08h30 as das 06h e 08h), mudando o resumo do alerta e reenviando-o duas vezes por dia.
- **Agências stale:** sem notícia **nova** em X horas (`scraper_stale_hours`, default 24). Conta `articles_new` (linhas de fato inseridas; migration 037 do data-platform); linhas anteriores à 037 (`articles_new` NULL) usam `articles_saved`. Antes valia `articles_saved`, que soma as linhas atualizadas por re-scrape: a EBC e as agências em fallback Plone 6 relêem a janela inteira e nunca apareciam.
- **Cobertura baixa** (`scraper_coverage_report`, 08h): menos de 80% das agências com sucesso em 24 h. O "Total artigos" do relatório soma `COALESCE(articles_new, articles_saved)` (notícias novas, ~242/dia em nov/2026).

### Sem repetição

O monitor roda 9 vezes por dia, mas só reenvia o alerta quando muda o conjunto de agências **sem execução** ou **com falhas consecutivas**, ou quando o último envio tem `scraper_alert_repeat_hours` (default 24 h; `0` desliga o dedup). Mudança só na lista de agências sem notícias (umas 48 crônicas) não reenvia: elas vão na mensagem, mas aparecem no máximo uma vez por dia. O último envio fica na Variable `scraper_monitor_last_alert` (JSON com as siglas e o horário, no metastore). Para forçar o reenvio na próxima execução, apague essa Variable. Sem nada a alertar, o estado é zerado e um problema que volta sai na hora. Erro ao ler ou gravar o estado não impede o envio. No modo log-only o estado não é gravado, então o alerta volta ao log a cada execução.

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
