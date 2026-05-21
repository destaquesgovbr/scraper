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
- **Falhas consecutivas:** Agência com N+ erros seguidos (threshold configurável)
- **Agências stale:** Sem notícias em X horas (stale_hours configurável)
- **Cobertura baixa:** Menos de 80% das agências com sucesso em 24h

### Fallback

Se Telegram não estiver configurado, alertas vão para o webhook (`scraper_alert_webhook_url`). Se nenhum estiver configurado, alertas são apenas logados.
