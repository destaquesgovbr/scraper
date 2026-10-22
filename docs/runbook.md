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

7. Após merge, a DAG `scrape_nova_agencia` será criada automaticamente pelo `scrape_agencies.py` (geração dinâmica).

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

A DAG **deixa de ser gerada** — o `scrape_agencies.py` gera DAGs dinamicamente apenas para agências ativas. No Airflow UI, a DAG ficará em estado "missing" após o próximo deploy das DAGs. O `load_urls_from_yaml()` loga um resumo das agências filtradas em INFO (motivo individual por agência em DEBUG).

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
| `URL_BROKEN` | Verificar nova URL no site; atualizar YAML |
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

## DAG de Agência Travada (DagRun zumbi)

**Sintoma.** O `monitor_scraping_health` alerta "Agencias Sem Execucao": a agência está ativa no `site_urls.yaml`, mas não tem nenhum `scrape_run` (sucesso ou erro) na janela de `scraper_silent_hours`. As outras checagens não veem o caso, porque partem de `scrape_runs` e a DAG nem chega a chamar a API. No log do scheduler, o erro se repete em loop (centenas de linhas por hora por DAG):

```
DAG 'scrape_X' not found in serialized_dag table
Couldn't find DAG scrape_X in DAG bag or database!
```

**Causa conhecida (Composer 3, Airflow 3.1.0).** Logo depois de um deploy das DAGs, a DagVersion mais recente de algumas DAGs ficou gravada sem `serialized_dag` e sem `dag_code`. O bundle `dags-folder` não é versionado, então o scheduler resolve qualquer run para a versão mais recente, não acha a DAG serializada e deixa o DagRun em `running` sem task instances. Como `max_active_runs=1`, nenhum run novo é criado e a agência para de ser raspada sem nenhum erro de task. Casos de 2026: `scrape_inma` (13/04), `scrape_inpe` (11/06), `scrape_aids` e `scrape_patrimonio` (21/08), todos minutos depois de um deploy.

**Diagnóstico (somente leitura).**

```bash
# array: funciona igual no bash e no zsh
COMPOSER=(destaquesgovbr-composer --location southamerica-east1 --project inspire-7-finep)

# 1. Um único run em 'running' há horas ou dias
gcloud composer environments run "${COMPOSER[@]}" dags list-runs -- scrape_X

# 2. O run não tem task instances (resposta vazia)
gcloud composer environments run "${COMPOSER[@]}" tasks states-for-dag-run -- scrape_X <run_id>

# 3. A versão mais recente não tem código serializado (REST API, só GET)
AIRFLOW_URI=$(gcloud composer environments describe "${COMPOSER[@]}" --format='value(config.airflowUri)')
TOKEN=$(gcloud auth print-access-token)
curl -s -H "Authorization: Bearer $TOKEN" \
  "$AIRFLOW_URI/api/v2/dags/scrape_X/dagVersions?order_by=-version_number&limit=1"
curl -s -o /dev/null -w '%{http_code}\n' -H "Authorization: Bearer $TOKEN" \
  "$AIRFLOW_URI/api/v2/dagSources/scrape_X?version_number=<N>"  # 404 = versão órfã
```

**Correção.** É uma ação mutante no Airflow e precisa de aprovação humana.

Marcar o run como `failed` **não resolve**: o run seguinte também é criado com a versão órfã e trava de novo. Apague os metadados da DAG. O `delete_dag` do Airflow 3.1 remove todas as linhas com o `dag_id`, inclusive `dag_version` e `serialized_dag`. Depois, o dag-processor reparseia `scraper/scrape_agencies.py` e cria uma v1 nova, já serializada:

```bash
COMPOSER=(destaquesgovbr-composer --location southamerica-east1 --project inspire-7-finep)
gcloud composer environments run "${COMPOSER[@]}" dags delete -- scrape_X --yes
```

Só se perde o histórico de runs dessa DAG no Airflow. A tabela `scrape_runs` e as notícias no Postgres não são afetadas.

**Validação.**
1. `dagSources/scrape_X?version_number=<nova versão>` responde 200.
2. `dags list-runs -- scrape_X` mostra runs a cada 10 min, com task instances.
3. O erro `not found in serialized_dag table` para a DAG some do log do scheduler.
4. O próximo `monitor_scraping_health` não lista mais a agência.

**Recuperar o período perdido.** A DAG só raspa a janela da `logical_date`. O que a agência publicou enquanto a DAG estava travada só volta com um re-scrape dirigido (ver "Forçar Re-scrape Completo"), com `start_date` no dia em que o run travou. Sem `allow_update`, o re-scrape só insere o que falta.

**Depois de deploys que mudam as DAGs** (`site_urls.yaml`, schedule, `default_args`): acompanhe o alerta "Agencias Sem Execucao" nas 2 h seguintes.

## Tratar Anti-Bot

Quando uma agência retorna `ErrorCategory.ANTI_BOT`:

1. Acesse o site manualmente no navegador para confirmar
2. Se for Cloudflare temporário, geralmente resolve sozinho em horas
3. Se persistir, considere desativar a agência temporariamente com `disabled_reason`
4. O `WebScraper` já implementa random delays entre requests — não é possível ajustar externamente

## Ajustar Retenção de scrape_runs

A tabela `scrape_runs` cresce ~22k rows/dia. O `cleanup_old_scrape_runs` DAG limpa registros antigos.

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

### Telegram

Alertas são enviados para o chat configurado em `scraper_telegram_monitor_chat_id`. Tipos:
- **Agências sem execução:** Agência ativa sem nenhum `scrape_run` (sucesso ou erro) em X horas (`scraper_silent_hours`, default 2h). A DAG `scrape_*` não está chamando a API; ver "DAG de Agência Travada". Vem primeiro na mensagem.
- **Falhas consecutivas:** Agência com N+ erros seguidos (threshold configurável)
- **Agências stale:** Sem notícias em X horas (stale_hours configurável)
- **Cobertura baixa:** Menos de 80% das agências com sucesso em 24h

### Fallback

Se Telegram não estiver configurado, alertas vão para o webhook (`scraper_alert_webhook_url`). Se nenhum estiver configurado, alertas são apenas logados.

Cada seção lista no máximo 25 agências (`... e mais N`), e a mensagem é cortada em fim de linha para caber em 4000 caracteres: o Telegram recusa texto acima de 4096, e o Cloud Logging corta a linha `Alert (log-only): ...` em 4096. A lista completa fica nos logs das tasks `check_*`.

Para ler os alertas que só foram logados (`Alert (log-only)`, task `send_alerts`):
```bash
gcloud logging read 'resource.type="cloud_composer_environment" AND textPayload:"Alert (log-only)"' \
  --project=inspire-7-finep --freshness=2h --limit=5 --format='value(timestamp,textPayload)'
```

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
