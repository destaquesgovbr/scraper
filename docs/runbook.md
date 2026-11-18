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
| `NETWORK_ERROR` | passageiro: repete | Aguardar; verificar se é transiente. Inclui 408/429, 5xx depois dos retries e o "Erro de rede ao acessar API" do Plone 6 |
| `ANTI_BOT` | permanente: falha sem retry | Desativar temporariamente se persistir (ver seção abaixo) |
| `EMPTY_CONTENT` | passageiro: repete | Normal se agência publica pouco (hoje nenhuma exceção gera esta categoria) |
| `URL_BROKEN` | permanente: falha sem retry | 404/410 e outros 4xx permanentes (400, 405...): verificar nova URL no site; atualizar YAML ou desativar |
| `ACCESS_DENIED` | permanente: falha sem retry | 401/403 ou redirect para login: procurar listagem pública; senão desativar (`active: false`) |
| `HTML_CHANGED` | permanente: falha sem retry | Verificar site manualmente; fix no parser |
| `UNKNOWN` | passageiro: repete | Ver `error_message` para detalhes |
| `unknown_agency` (só na resposta da API) | permanente: falha sem retry | Agência fora do `site_urls.yaml` da API ou inativa nele: YAMLs dessincronizados ou imagem da API antiga |

### 3. Verificar logs no Cloud Run

```bash
gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="destaquesgovbr-scraper-api"' --project=destaquesgovbr --limit=50 --format=json
```

### 4. Testar localmente

```bash
# Chamar API local para uma agência específica
curl -X POST http://localhost:8000/scrape/agencies \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2026-05-18", "agencies": ["mec"]}'
```

## Erro Permanente x Passageiro (DAGs `scrape_agencies` e `scrape_ebc`)

Cada item de `errors[]` da resposta de `/scrape/agencies` e `/scrape/ebc` traz `category` (a mesma gravada em `scrape_runs.error_category`) e `retryable`. O status HTTP não mudou (200 sucesso, 207 parcial, 500 todas falharam).

- **Permanente** (`retryable: false`): `url_broken`, `access_denied`, `html_changed`, `anti_bot` e `unknown_agency`. Repetir na mesma execução não resolve. A task levanta `AirflowFailException` e falha **numa tentativa só**, com a agência e a categoria na mensagem (`Scraping failed for <sigla>: erro permanente (<categoria>), sem retry: ...`). Antes, cada agência quebrada gastava 2 retries de 5 min por execução.
- **Passageiro** (`retryable: true`): `network_error`, `unknown`, `empty_content`, erro sem categoria. A task repete como antes: 2x, a cada 5 min.
- **Sem decisão possível**, também repete: corpo que não é JSON (página de erro do Cloud Run, 503), `{"detail": ...}` de exceção do manager, timeout ou erro de conexão do httpx e API antiga, sem `retryable`.
- Na `scrape_agencies` só contam os erros da própria agência da task. Na `scrape_ebc` (duas fontes numa task), a task só falha sem retry se **todos** os erros forem permanentes.

Limpar (Clear) uma task que falhou por erro permanente não adianta: corrija o YAML ou o parser ("Debugar Scraper em Falha"), ou desative a agência.

## Execução Vermelha da `scrape_agencies` (task `summarize`)

A task `summarize` (`trigger_rule="all_done"`, `retries=0`) roda depois de todas as agências, com ou sem falha, e é a **única folha** da DAG: a cor do DagRun é a dela. Agência que falhou aparece vermelha na própria task mapeada (Mapped Tasks), mas a execução fica verde.

O log da `summarize` resume a execução:

```
Resumo scrape_agencies: 140/147 agências OK, 7 com falha (4.8%, limite 10%); 231 notícias novas, 260 linhas gravadas
Agências com falha: antaq, mec, ...
```

Ela só falha (`AirflowFailException`, execução vermelha) se:

- **nenhuma agência funcionou** (`Nenhuma agência raspada com sucesso`): scraper-api fora do ar, token IAM, `scraper_api_url` errada, gov.br inteiro fora ou nosso IP bloqueado;
- **a fração de agências com falha passou de `scraper_max_failed_ratio`** (Airflow Variable, fração entre 0 e 1, padrão `0.10`; valor inválido vale 0,10). Mensagem: `N de M agências falharam (X%, acima do limite de Y%): <siglas>`.

Vermelho, portanto, quer dizer problema de plataforma: comece pelo scraper-api (logs do Cloud Run) e pela taxa de `network_error` em `scrape_runs`. Para mudar o limite, edite a Variable (ex.: `0.15` num período de instabilidade conhecida do gov.br) e volte ao padrão depois.

As falhas são deduzidas pela ausência: cada `scrape` com sucesso devolve `{agency, articles_new, articles_saved}` por XCom, e a `summarize` compara com a lista de agências ativas do parse. Uma agência que falhou não deixa XCom (o Airflow só grava o retorno de task com sucesso).

## Concorrência (`SCRAPE_MAX_CONCURRENT_AGENCIES`)

O número de agências raspadas ao mesmo tempo (`max_active_tis_per_dag` da task `scrape`) vem do env `SCRAPE_MAX_CONCURRENT_AGENCIES` dos componentes do Airflow, lido **no parse** da DAG:

- ausente ou vazio: 4 (o valor de sempre);
- inválido (`abc`, `2.5`): 4, com aviso no log do parse (um erro no import faria a DAG sumir);
- 0 ou negativo: 1 (com 0 o scheduler nunca roda nenhuma task mapeada e, com `max_active_runs=1`, a DAG para).

O limite existe para proteger o www.gov.br (as ~147 agências estão no mesmo host), não o scraper-api. Suba devagar, olhando a taxa de `network_error`. A variável vem do Terraform (repo `infra`: `env_variables` do Composer em `composer.tf`; Airflow próprio em `airflow.env.tftpl`); enquanto não for definida lá, vale o padrão 4. Mudar o valor exige um novo parse da DAG (no Composer, a atualização do ambiente reinicia os componentes) e cria uma DagVersion nova. Outros limites continuam valendo: `max_active_tasks` da DAG, pools e `parallelism` do Airflow.

## Agências Sem Execução (DAG `scrape_agencies`)

Desde nov/2026 (infra#223) o scraping gov.br é **uma DAG só**, `scrape_agencies`, com uma task mapeada (`scrape`) por agência ativa e a sigla como map index, e a task final `summarize`. Ela roda às 06h, 08h, ..., 22h (BRT, 9 execuções por dia), com no máximo `SCRAPE_MAX_CONCURRENT_AGENCIES` agências ao mesmo tempo (padrão 4, ~5 a 8 min por execução no Composer). A `scrape_ebc` segue a mesma agenda. Antes eram ~147 DAGs `scrape_<agencia>`, de 10 em 10 min.

**Sintoma.** O `monitor_scraping_health` (às 06h30, 08h30, ..., 22h30) alerta "Agencias Sem Execucao": a agência está ativa no `site_urls.yaml`, mas não tem nenhum `scrape_run` (sucesso ou erro) nas últimas `scraper_silent_hours` (default 3 h). As outras checagens não veem o caso, porque partem de `scrape_runs`, e a chamada à API nem chegou a acontecer. Com o scraping de 2 em 2 h, o alerta sai quando a agência perde duas execuções seguidas, ou só a das 06h (a anterior é a das 22h do dia anterior).

**Primeiro passo: todas ou só algumas?**

- **Todas (ou quase todas) as agências:** a DAG inteira não rodou. Pode estar pausada (`dags list` mostra `is_paused`), ausente (erro de import no parse), com um DagRun preso em `running` (zumbi, ver abaixo) ou com o scraper-api fora do ar (5xx ou 401 antes de gravar o `scrape_run`).
- **Só algumas:** as tasks mapeadas dessas agências falharam antes de a API gravar o resultado (token, timeout, 5xx do Cloud Run) ou ainda não rodaram (execução atrasada, fila do pool).

**Diagnóstico (somente leitura).**

```bash
# array: funciona igual no bash e no zsh
COMPOSER=(destaquesgovbr-composer --location southamerica-east1 --project inspire-7-finep)

# 1. Execuções recentes da DAG (estado e horário)
gcloud composer environments run "${COMPOSER[@]}" dags list-runs -- scrape_agencies

# 2. Estado de cada task mapeada de uma execução (uma linha por agência, com map_index)
gcloud composer environments run "${COMPOSER[@]}" tasks states-for-dag-run -- scrape_agencies <run_id>
```

Na UI do Airflow: DAG `scrape_agencies` → Grid → task `scrape` → aba **Mapped Tasks**. O map index aparece com a sigla da agência; filtre por estado `failed` e abra os **Logs** da tentativa. O `on_failure_callback` também loga `Scrape DAG failure: dag=scrape_agencies task=scrape agency=<sigla> map_index=<N> ...`.

**DagRun zumbi (Composer 3, Airflow 3.1.0).** Logo depois de um deploy das DAGs, a DagVersion mais recente de algumas DAGs já ficou gravada sem `serialized_dag` e sem `dag_code`. O scheduler deixa o DagRun em `running` sem task instances e, com `max_active_runs=1`, não cria outro. No log do scheduler, o erro se repete em loop:

```
DAG 'scrape_agencies' not found in serialized_dag table
Couldn't find DAG scrape_agencies in DAG bag or database!
```

Casos de 2026, ainda com uma DAG por agência: `scrape_inma` (13/04), `scrape_inpe` (11/06), `scrape_aids` e `scrape_patrimonio` (21/08) e `scrape_esg` (sem execução desde 11/11). Hoje um zumbi para **todas** as agências de uma vez, e o monitor acusa na execução seguinte. Para confirmar (somente leitura):

```bash
# O run em 'running' há horas não tem task instances (resposta vazia no passo 2 acima)
AIRFLOW_URI=$(gcloud composer environments describe "${COMPOSER[@]}" --format='value(config.airflowUri)')
TOKEN=$(gcloud auth print-access-token)
curl -s -H "Authorization: Bearer $TOKEN" \
  "$AIRFLOW_URI/api/v2/dags/scrape_agencies/dagVersions?order_by=-version_number&limit=1"
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $TOKEN" \
  "$AIRFLOW_URI/api/v2/dagSources/scrape_agencies?version_number=<N>"  # 404 = versão órfã
```

**Correção.** Todas as ações abaixo mudam estado no Airflow e precisam de aprovação humana.

- **Task mapeada que falhou (algumas agências):** limpe só as tasks com falha da execução, para rodarem de novo. Na UI: Mapped Tasks → selecione as tasks `failed` → **Clear**. Pela CLI, todas as tasks com falha das execuções do intervalo:

  ```bash
  gcloud composer environments run "${COMPOSER[@]}" tasks clear -- scrape_agencies \
    -t '^scrape$' --only-failed -s <AAAA-MM-DDTHH:MM> -e <AAAA-MM-DDTHH:MM> --yes
  ```

  Pela REST API, uma agência só (`task_ids` com `[task_id, map_index]`):

  ```bash
  curl -s -X POST -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    "$AIRFLOW_URI/api/v2/dags/scrape_agencies/clearTaskInstances" \
    -d '{"dag_run_id": "<run_id>", "task_ids": [["scrape", <map_index>]], "only_failed": false, "dry_run": false}'
  ```

  Se a falha é da agência (site fora, 4xx permanente: a task falhou numa tentativa só, com `erro permanente` na mensagem), limpar não adianta: ver "Debugar Scraper em Falha".
- **DAG pausada:** `gcloud composer environments run "${COMPOSER[@]}" dags unpause -- scrape_agencies`.
- **DagRun zumbi:** marcar o run como `failed` **não resolve**, porque o run seguinte também é criado com a versão órfã e trava de novo. Apague os metadados da DAG. O `delete_dag` do Airflow 3.1 remove todas as linhas com o `dag_id`, inclusive `dag_version` e `serialized_dag`. Depois, o dag-processor reparseia `scraper/scrape_agencies.py` e cria uma v1 nova, já serializada:

  ```bash
  gcloud composer environments run "${COMPOSER[@]}" dags delete -- scrape_agencies --yes
  ```

  Só se perde o histórico de runs da DAG no Airflow. A tabela `scrape_runs` e as notícias no Postgres não são afetadas. A DAG recriada pode nascer pausada: despause.

**Validação.**
1. `dags list-runs -- scrape_agencies` mostra uma execução nova, com task instances.
2. O erro `not found in serialized_dag table` some do log do scheduler (no caso do zumbi).
3. O próximo `monitor_scraping_health` não lista mais as agências.

**Recuperar o período perdido (execução manual com params).** A execução normal raspa a janela de 9 h antes da `logical_date`. Para recuperar uma queda, dispare a DAG com os params opcionais `start_date` e `end_date` (YYYY-MM-DD, datas de Brasília, os dois juntos). Eles substituem a janela calculada em todas as agências:

- **UI:** DAG `scrape_agencies` → **Trigger** → preencha "Data inicial (recuperação)" e "Data final (recuperação)".
- **CLI:**

  ```bash
  gcloud composer environments run "${COMPOSER[@]}" dags trigger -- scrape_agencies \
    --conf '{"start_date": "2026-11-15", "end_date": "2026-11-17"}'
  ```

Data em outro formato é recusada no disparo. Só uma das duas ou início depois do fim falham as tasks na hora, sem retry e sem chamar a API. Com `max_active_runs=1`, o run manual espera a execução agendada em andamento terminar. Sem `allow_update`, o re-scrape só insere o que falta. Cada task tem timeout de 10 min, então em quedas longas (mais de ~3 dias) divida em várias execuções, ou use o re-scrape dirigido por agência ("Forçar Re-scrape Completo"). A `scrape_ebc` aceita os mesmos params.

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

`allow_update: true` faz `ON CONFLICT (unique_id) DO UPDATE` em vez de `DO NOTHING`. Na resposta, `articles_saved` conta as linhas gravadas (inseridas + atualizadas) e `articles_new` só as inseridas (o `RETURNING ... (xmax = 0)` separa as duas).

## Verificar Alertas

### Canais

`notify.send_alert()` tenta, nesta ordem, e para no primeiro que entregar:

1. **Telegram direto**, se as Variables `scraper_telegram_bot_token` e `scraper_telegram_monitor_chat_id` existirem (hoje não existem).
2. **Pub/Sub `dgb.ops.alerts`**: o alert-relay (data-platform) repassa ao Telegram da equipe. O tópico é a Variable `scraper_alert_pubsub_topic` ou, por padrão, `projects/$GCP_PROJECT_ID/topics/dgb.ops.alerts`; Variable vazia desliga o canal. A mensagem vai como texto puro (sem `<b>`, até 3400 caracteres, cortada em fim de linha), com título ("Scraper: saúde das agências" ou "Scraper: cobertura baixa"), severidade `warning` e a DAG de origem. A SA do Composer precisa de `roles/pubsub.publisher` no tópico (infra).
3. **Webhook** (`scraper_alert_webhook_url`).
4. **Só log** (`Alert (log-only): ...`).

### Tipos

- **Agências sem execução:** agência ativa sem nenhum `scrape_run` (sucesso ou erro) em X horas (`scraper_silent_hours`, default 3 h). A task mapeada da agência na `scrape_agencies` não chamou a API; ver "Agências Sem Execução (DAG `scrape_agencies`)". Vem primeiro na mensagem.
- **Falhas consecutivas:** agência com N+ erros seguidos (`scraper_consecutive_failure_threshold`, default 3) na janela `scraper_failure_window_hours` (default 6 h). Cada execução que falha grava até 3 erros, um por tentativa.
- **Agências stale:** sem notícia **nova** em X horas (`scraper_stale_hours`, default 24). Conta `articles_new` (linhas de fato inseridas; migration 037 do data-platform); linhas anteriores à 037 (`articles_new` NULL) usam `articles_saved`. Antes valia `articles_saved`, que soma as linhas atualizadas por re-scrape: a EBC e as agências em fallback Plone 6 relêem a janela inteira e nunca apareciam.
- **Cobertura baixa** (`scraper_coverage_report`, 08h): menos de 80% das agências com sucesso em 24 h. O "Total artigos" do relatório soma `COALESCE(articles_new, articles_saved)` (notícias novas, ~242/dia em nov/2026).

### Sem repetição

O monitor roda 9 vezes por dia, mas só reenvia o alerta quando muda o conjunto de agências **sem execução** ou **com falhas consecutivas**, ou quando o último envio tem `scraper_alert_repeat_hours` (default 24 h; `0` desliga o dedup). Mudança só na lista de agências sem notícias (umas 48 crônicas) não reenvia: elas vão na mensagem, mas aparecem no máximo uma vez por dia. O último envio fica na Variable `scraper_monitor_last_alert` (JSON com as siglas e o horário, no metastore). Para forçar o reenvio na próxima execução, apague essa Variable. Sem nada a alertar, o estado é zerado e um problema que volta sai na hora. Erro ao ler ou gravar o estado não impede o envio. No modo log-only o estado não é gravado, então o alerta volta ao log a cada execução.

### Tamanho e log

Cada seção lista no máximo 25 agências (`... e mais N`), e a mensagem é cortada em fim de linha para caber em 4000 caracteres: o Telegram recusa texto acima de 4096, e o Cloud Logging corta a linha `Alert (log-only): ...` em 4096. A lista completa fica nos logs das tasks `check_*`.

Para ler os alertas que só foram logados (`Alert (log-only)`, task `send_alerts`):
```bash
gcloud logging read 'resource.type="cloud_composer_environment" AND textPayload:"Alert (log-only)"' \
  --project=inspire-7-finep --freshness=6h --limit=5 --format='value(timestamp,textPayload)'
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
