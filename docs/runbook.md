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

Ver CLAUDE.md § Classificação de Erros para detalhes de cada categoria.

| Categoria | Ação recomendada |
|-----------|-----------------|
| `NETWORK_ERROR` | Aguardar; verificar se é transiente |
| `ANTI_BOT` | Desativar temporariamente se persistir (ver seção abaixo) |
| `EMPTY_CONTENT` | Normal se agência publica pouco |
| `URL_BROKEN` | 404/410: verificar nova URL no site; atualizar YAML ou desativar |
| `ACCESS_DENIED` | 401/403 ou redirect para login: procurar listagem pública; senão desativar (`active: false`) |
| `HTML_CHANGED` | Verificar site manualmente; fix no parser |
| `UNKNOWN` | Ver `error_message` para detalhes |

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

## Agências Sem Execução (DAG `scrape_agencies`)

Desde nov/2026 (infra#223) o scraping gov.br é **uma DAG só**, `scrape_agencies`, com uma task mapeada (`scrape`) por agência ativa e a sigla como map index. Ela roda às 06h, 08h, ..., 22h (BRT, 9 execuções por dia), com no máximo 4 agências ao mesmo tempo (~5 a 8 min por execução). A `scrape_ebc` segue a mesma agenda. Antes eram ~147 DAGs `scrape_<agencia>`, de 10 em 10 min.

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

  Se a falha é da agência (site fora, 4xx permanente), limpar não adianta: ver "Debugar Scraper em Falha".
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

`allow_update: true` faz `ON CONFLICT (unique_id) DO UPDATE` em vez de `DO NOTHING`.

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
- **Agências stale:** sem notícias em X horas (`scraper_stale_hours`, default 24).
- **Cobertura baixa** (`scraper_coverage_report`, 08h): menos de 80% das agências com sucesso em 24 h.

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
