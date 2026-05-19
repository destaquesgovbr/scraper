# DestaquesGovBr Scraper

Scraper standalone para notícias de sites governamentais brasileiros (~155 agências gov.br + EBC). Suporta três tipos de scraper (HTML, EBC, Plone6 API), publica eventos Pub/Sub após inserção, e inclui sistema completo de monitoramento com alertas.

## Visão Geral

O scraper coleta notícias publicadas em sites gov.br e da EBC (Agência Brasil, TV Brasil, etc.), extraindo título, conteúdo, data de publicação e metadados. Os dados são persistidos no PostgreSQL (upsert por unique_id, deduplicação por URL). Após persistência, eventos são publicados no Pub/Sub (`dgb.news.scraped`) para consumo downstream.

A coleta é orquestrada por **DAGs Airflow** (Cloud Composer) que chamam a **API FastAPI** hospedada no Cloud Run. O sistema inclui 4 DAGs de manutenção para monitoramento, alertas e retenção de dados.

## Arquitetura

```
DAGs Airflow (Cloud Composer)
│   ├── scrape_agencies (a cada 10min, offset por agência)
│   ├── scrape_ebc (a cada 10min)
│   ├── monitor_scraping_health (a cada 30min)
│   ├── cleanup_old_scrape_runs (diário 03h)
│   └── scraper_coverage_report (diário 08h)
│
│   [IAM: google.oauth2.id_token → Bearer token]
│
└── HTTP POST → Cloud Run API (FastAPI)
        ├── POST /scrape/agencies → ScrapeManager
        │       ├── WebScraper (HTML) ──┐
        │       └── Plone6APIScraper ───┤
        ├── POST /scrape/ebc → EBCScrapeManager
        │       └── EBCWebScraper ──────┤
        ├── POST /verify/integrity      │
        │       └── integrity.service ──┤
        └── GET /health                 │
                                        ▼
                              ┌─── PostgreSQL ───┐
                              │  news            │
                              │  agencies        │
                              │  themes          │
                              │  scrape_runs     │
                              └──────────────────┘
                                        │
                                        ▼
                              Pub/Sub (dgb.news.scraped)
                              [opcional, graceful degradation]

    Credenciais: DATABASE_URL env → Secret Manager fallback
```

## Estrutura do Repositório

```
scraper/
├── src/govbr_scraper/
│   ├── api.py                         # FastAPI (Cloud Run) — 4 endpoints
│   ├── scrapers/
│   │   ├── webscraper.py              # Scraper HTML gov.br (~1382 linhas)
│   │   ├── scrape_manager.py          # Coordenador gov.br (routing por scraper_type)
│   │   ├── ebc_webscraper.py          # Scraper EBC (~624 linhas)
│   │   ├── ebc_scrape_manager.py      # Coordenador EBC
│   │   ├── plone6_api_scraper.py      # Scraper Plone6 API (~394 linhas)
│   │   ├── content_hash.py            # Deduplicação: normalize + SHA256[:16]
│   │   ├── unique_id.py              # IDs legíveis: slug + sufixo hex
│   │   ├── yaml_config.py            # Utilitário de carga YAML
│   │   └── config/
│   │       ├── site_urls.yaml         # ✓ Arquivo fonte (sempre edite este)
│   │       └── ebc_urls.yaml          # Endpoints EBC
│   ├── storage/
│   │   ├── storage_adapter.py         # Abstração de persistência
│   │   ├── postgres_manager.py        # PostgreSQL + connection pooling
│   │   └── event_publisher.py         # Pub/Sub (dgb.news.scraped)
│   ├── models/
│   │   ├── news.py                    # Modelos: News, Agency, Theme
│   │   └── monitoring.py             # ErrorCategory, ScrapeRunResult
│   ├── integrity/
│   │   ├── checker.py                 # Verificação de imagens e conteúdo
│   │   └── service.py                # Batch verification com deadline
│   └── monitoring/
│       ├── health_checks.py           # Queries SQL de saúde
│       └── structured_log.py         # Log estruturado + record_scrape_run
├── dags/
│   ├── scrape_agencies.py             # ~155 DAGs dinâmicas (1 por agência)
│   ├── scrape_ebc.py                  # 1 DAG para sites EBC
│   ├── monitor_scraping_health.py     # Monitoramento: falhas + agências stale
│   ├── cleanup_old_scrape_runs.py     # Retenção: delete > N dias
│   ├── scraper_coverage_report.py     # Relatório diário + alerta cobertura
│   ├── notify.py                      # Utilitário: Telegram > webhook > log
│   └── config/
│       ├── README.md
│       └── site_urls.yaml             # → Cópia (sincronize manualmente)
├── docker/
│   └── Dockerfile
├── docs/
│   └── runbook.md                     # Procedimentos operacionais
├── tests/
│   ├── unit/                          # 30 arquivos de teste
│   └── integration/                   # 3 arquivos (requer DB real)
├── pyproject.toml
└── .github/workflows/
    ├── tests.yaml                     # pytest em PRs
    ├── scraper-api-deploy.yaml        # Build + deploy Cloud Run (push main)
    └── composer-deploy-dags.yaml      # Deploy DAGs → Composer (push main)
```

### ⚠️ Importante sobre site_urls.yaml

O arquivo `site_urls.yaml` existe em **dois locais** que devem ser mantidos sincronizados:

- **Sempre edite:** `src/govbr_scraper/scrapers/config/site_urls.yaml` (fonte)
- **Depois copie para:** `dags/config/site_urls.yaml` (cópia)

**Comando para sincronizar:**
```bash
cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
```

**Por que dois arquivos?**
- DAGs do Airflow são autocontidas e não importam código Python da API
- API Cloud Run empacota o arquivo de `src/` na imagem Docker
- Mantém separação de responsabilidades entre orquestração (DAGs) e execução (API)

**Validação:** O CI valida automaticamente que os arquivos estão sincronizados via `test_config_sync.py`. PRs com arquivos dessincronizados são bloqueados.

## Tipos de Scraper

O campo `scraper_type` no `site_urls.yaml` determina qual scraper é usado para cada agência gov.br:

| Tipo | Classe | Uso |
|------|--------|-----|
| `html` (default) | `WebScraper` | Sites gov.br com listagem HTML padrão |
| `plone6_api` | `Plone6APIScraper` | Sites Plone 6 com Volto (React SPA) via `++api++` |

**EBC é uma pipeline separada** — não usa `scraper_type`. Tem DAG própria (`scrape_ebc.py`), manager próprio (`EBCScrapeManager`), scraper próprio (`EBCWebScraper`) e config YAML próprio (`ebc_urls.yaml`).

**Quando usar `plone6_api`:** Agências que migraram para Plone 6 renderizam conteúdo via JavaScript (React/Volto). O HTML da página não contém os artigos — é necessário usar a REST API (`++api++`) do Plone para obter os dados. Identificar pela ausência de listagem de notícias no HTML e presença de `++api++` no site.

## Features

### Known URL Fence

Otimização que evita re-processar artigos já coletados. Antes de iniciar o scraping, o `ScrapeManager` consulta as últimas 200 URLs da agência via `get_recent_urls()`. Durante o scraping, URLs já conhecidas são contadas consecutivamente. Ao atingir **3 URLs conhecidas consecutivas**, o scraper para — assumindo que o restante já foi coletado em execuções anteriores.

### Content Hash

Deduplicação por conteúdo via `compute_content_hash(title, content)`:
1. Normaliza texto (NFKD → ASCII, lowercase, remove pontuação, colapsa espaços)
2. Concatena `título\nconteúdo` normalizado
3. SHA256 dos primeiros 16 caracteres hex

Usado para detectar artigos duplicados com URLs diferentes e para verificação de integridade.

### Unique ID

IDs legíveis no formato `{slug}_{suffix}` (ex: `governo-anuncia-programa_a3f2e1`):
- Slug: título slugificado, máx 100 chars, truncado em fronteira de palavra
- Suffix: 6 chars hex de MD5(agency + date + title) para unicidade determinística

### Classificação de Erros

`ErrorCategory` (StrEnum) classifica falhas de scraping para monitoramento:

| Categoria | Padrões detectados |
|-----------|-------------------|
| `NETWORK_ERROR` | timeout, connection error/refused |
| `ANTI_BOT` | anti-bot, JS challenge, cloudflare |
| `EMPTY_CONTENT` | no news found |
| `URL_BROKEN` | HTTP 403, 404, 410 |
| `HTML_CHANGED` | no articles found (estrutura mudou) |
| `UNKNOWN` | fallback |

### Pub/Sub Events

Após persistir artigos (insert ou update), `EventPublisher` publica no tópico `dgb.news.scraped`:

```json
{
  "unique_id": "governo-anuncia-programa_a3f2e1",
  "agency_key": "mec",
  "published_at": "2026-05-18T14:30:00-03:00",
  "scraped_at": "2026-05-18T17:45:12.123456+00:00"
}
```

Atributos da mensagem: `trace_id` (UUID por batch), `event_version` ("1.0").

**Graceful degradation:** Se `PUBSUB_TOPIC_NEWS_SCRAPED` não estiver definido ou o client falhar na inicialização, todos os publishes são no-ops. Artigos já estão persistidos no PostgreSQL.

## API Endpoints

A API roda no Cloud Run e é chamada pelas DAGs Airflow.

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/scrape/agencies` | Raspa sites gov.br (aceita lista de agências, datas) |
| `POST` | `/scrape/ebc` | Raspa sites EBC (Agência Brasil, TV Brasil) |
| `POST` | `/verify/integrity` | Verifica integridade de artigos (imagens, conteúdo) |
| `GET` | `/health` | Health check |

### Request: `/scrape/agencies`

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-02",
  "agencies": ["mec", "mds"],
  "allow_update": false,
  "sequential": true
}
```

### Request: `/scrape/ebc`

```json
{
  "start_date": "2025-01-01",
  "end_date": "2025-01-02",
  "agencies": ["agencia_brasil"],
  "allow_update": false,
  "sequential": true
}
```

### Request: `/verify/integrity`

```json
{
  "articles": [
    {
      "unique_id": "governo-anuncia-programa_a3f2e1",
      "url": "https://www.gov.br/mec/pt-br/...",
      "image_url": "https://www.gov.br/mec/pt-br/.../image.png",
      "content_hash": "a1b2c3d4e5f67890",
      "source_etag": "\"abc123\"",
      "check_content": true
    }
  ]
}
```

**SSRF mitigation:** URLs são validadas contra allowlist de domínios aceitos:
- `https://www.gov.br/`
- `https://agenciabrasil.ebc.com.br/`
- `https://imagens.ebc.com.br/`
- `https://memoria.ebc.com.br/`
- `https://tvbrasil.ebc.com.br/`
- `https://live.staticflickr.com/`
- `https://storage.googleapis.com/destaquesgovbr-thumbnails/`

**Batch com deadline:** Verificação paralela (até 20 workers). Deadline de 100s (< timeout HTTP de 120s da DAG). Artigos pendentes ao estourar o deadline são marcados como `image_status=timeout`.

**Response:** `{results: [...], summary: {total, images_ok, images_broken, images_timeout, content_unchanged, content_changed, ...}}`

## DAGs Airflow

| DAG | Schedule | Descrição |
|-----|----------|-----------|
| `scrape_{agency_key}` (~155) | A cada 10min (offset 0-9) | 1 DAG por agência gov.br |
| `scrape_ebc` | A cada 10min (offset 0) | Sites EBC |
| `monitor_scraping_health` | A cada 30min | Falhas consecutivas + agências stale |
| `cleanup_old_scrape_runs` | Diário 03h UTC | Delete registros > retention_days |
| `scraper_coverage_report` | Diário 08h UTC | Cobertura 24h + alerta se < threshold |
| `notify.py` | (utilitário) | Telegram > webhook > log-only |

### DAGs de scraping

Cada DAG de scraping:
- Chama a API no Cloud Run via HTTP POST
- Autentica com ID token IAM (`google.oauth2.id_token`)
- Retry: 2x com backoff exponencial (base 5min)
- Timeout: 10min (agências) / 30min (EBC)

### DAGs de manutenção

**`monitor_scraping_health`:** Verifica agências com N falhas consecutivas (threshold configurável) e agências sem notícias em X horas (stale). Envia alertas via `notify.send_alert()`.

**`cleanup_old_scrape_runs`:** Deleta registros da tabela `scrape_runs` mais antigos que `scraper_retention_days` (default 90). Com ~155 agências rodando a cada 10min, isso são ~22k rows/dia.

**`scraper_coverage_report`:** Calcula cobertura 24h (agências com sucesso / total ativas), top erros por categoria. Alerta se cobertura < `scraper_min_coverage_ratio` (default 0.8).

**`notify.py`:** Utilitário compartilhado. `send_alert(message, telegram_token, telegram_chat_id, webhook_url)` — tenta Telegram primeiro, fallback para webhook, último recurso é log.

## Schema do Banco

### Tabela `news`

| Coluna | Tipo | Notas |
|--------|------|-------|
| `unique_id` | text PK | Formato: `{slug}_{suffix}` |
| `agency_id` | int FK | → agencies.id |
| `title` | text | |
| `url` | text | |
| `content` | text | Markdown |
| `image_url` | text | |
| `video_url` | text | |
| `category` | text | |
| `tags` | text[] | |
| `editorial_lead` | text | |
| `subtitle` | text | |
| `summary` | text | |
| `content_hash` | text | SHA256[:16] de título+conteúdo |
| `published_at` | timestamptz | |
| `updated_datetime` | timestamptz | |
| `extracted_at` | timestamptz | |
| `created_at` | timestamptz | |
| `updated_at` | timestamptz | |
| `agency_key` | text | Desnormalizado |
| `agency_name` | text | Desnormalizado |
| `theme_l1_id` ... `most_specific_theme_id` | int FK | → themes.id |
| `content_embedding` | vector(768) | Phase 4.7 |
| `embedding_generated_at` | timestamptz | Phase 4.7 |

**Constraints:** `unique_id` é PK (unique). Deduplicação por `(agency_key, url)` via pré-check + SAVEPOINT.

### Tabela `agencies`

| Coluna | Tipo |
|--------|------|
| `id` | serial PK |
| `key` | text unique |
| `name` | text |
| `type` | text |
| `parent_key` | text |
| `url` | text |
| `created_at` | timestamptz |

### Tabela `themes`

| Coluna | Tipo |
|--------|------|
| `id` | serial PK |
| `code` | text unique |
| `label` | text |
| `full_name` | text |
| `level` | int (1-3) |
| `parent_code` | text |
| `created_at` | timestamptz |

### Tabela `scrape_runs`

| Coluna | Tipo | Notas |
|--------|------|-------|
| `agency_key` | text | |
| `status` | text | "success" ou "error" |
| `error_category` | text | ErrorCategory value |
| `error_message` | text | Truncado em 500 chars |
| `articles_scraped` | int | |
| `articles_saved` | int | |
| `execution_time_seconds` | float | |
| `scraped_at` | timestamptz | |

## Variáveis de Ambiente (API / Cloud Run)

```bash
DATABASE_URL=postgresql://...        # Connection string (prioridade 1)
LOG_LEVEL=INFO                       # Loguru level
PUBSUB_TOPIC_NEWS_SCRAPED=projects/{project}/topics/dgb.news.scraped  # Opcional
```

**Fallback para DATABASE_URL:** Se não definido, tenta Secret Manager (`destaquesgovbr-postgres-connection-string`). Se falhar, detecta Cloud SQL Proxy local.

## Airflow Variables (Cloud Composer)

| Variable | Obrigatória | Default | Uso |
|----------|-------------|---------|-----|
| `scraper_api_url` | Sim | — | URL base da API Cloud Run |
| `scraper_database_url` | Sim | — | Connection string para DAGs de monitoramento |
| `scraper_retention_days` | Não | 90 | Dias de retenção em scrape_runs |
| `scraper_telegram_bot_token` | Não | — | Token do bot Telegram para alertas |
| `scraper_telegram_monitor_chat_id` | Não | — | Chat ID do grupo de monitoramento |
| `scraper_alert_webhook_url` | Não | — | Webhook fallback para alertas |
| `scraper_consecutive_failure_threshold` | Não | 3 | Falhas consecutivas para alerta |
| `scraper_failure_window_hours` | Não | 2 | Janela de tempo para verificar falhas |
| `scraper_stale_hours` | Não | 24 | Horas sem notícias para considerar stale |
| `scraper_min_coverage_ratio` | Não | 0.8 | Cobertura mínima antes de alertar |

## IAM e Autenticação

DAGs autenticam no Cloud Run via ID token:
```python
import google.auth.transport.requests
import google.oauth2.id_token

auth_req = google.auth.transport.requests.Request()
token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)
headers = {"Authorization": f"Bearer {token}"}
```

O Service Account do Composer deve ter a role `roles/run.invoker` no serviço Cloud Run.

## CI/CD

| Workflow | Trigger | Ação |
|----------|---------|------|
| `tests.yaml` | PR | pytest com coverage |
| `scraper-api-deploy.yaml` | push main | Build Docker + deploy Cloud Run |
| `composer-deploy-dags.yaml` | push main | rsync `dags/` → bucket Composer |

## Testes

- **30 unit tests** (`tests/unit/`) — cobertura de API, scrapers, storage, monitoring, DAGs
- **3 integration tests** (`tests/integration/`) — requerem DB real, marcados com `@pytest.mark.integration`

```bash
# Rodar todos os testes unitários
poetry run pytest tests/unit/

# Rodar apenas integration (requer DATABASE_URL configurado)
poetry run pytest -m integration

# Rodar todos com coverage
poetry run pytest --cov=govbr_scraper --cov-report=term-missing
```

## Deploy

| Componente | Destino | Workflow |
|-----------|---------|----------|
| **API** | Cloud Run (`destaquesgovbr-scraper-api`) | `scraper-api-deploy.yaml` |
| **DAGs** | Cloud Composer (`{bucket}/scraper/`) | `composer-deploy-dags.yaml` |
| **Infra** | Terraform (repo `infra/`) | SA, Cloud Run, Artifact Registry, IAM |

### Deploy da API

O workflow `scraper-api-deploy.yaml` faz build da imagem Docker e deploy no Cloud Run automaticamente em push para `main`.

### Deploy das DAGs

O workflow `composer-deploy-dags.yaml` sincroniza a pasta `dags/` para o bucket do Composer no subdiretório `scraper/`:

```bash
gsutil -m rsync -r -d dags/ gs://{COMPOSER_BUCKET}/dags/scraper/
```

## Desenvolvimento Local

```bash
# Instalar dependências
poetry install

# Rodar API localmente
poetry run uvicorn govbr_scraper.api:app --reload
```

### Code Style

- **Formatter:** Black (line-length 100, target Python 3.12)
- **Linter:** Ruff (E, W, F, I, B, C4, UP — config em `pyproject.toml`)

```bash
poetry run black .
poetry run ruff check . --fix
```

### Processo de PR

1. Crie branch a partir de `main`
2. Garanta que `poetry run pytest` passa
3. Abra PR — CI roda testes automaticamente
4. Após merge em `main`, deploy é automático (API + DAGs)

## Política de Commits

Commits NÃO devem incluir atribuições ao Claude Code ou co-autoria do Claude.
