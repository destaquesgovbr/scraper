# DestaquesGovBr Scraper

Scraper standalone para notícias de sites governamentais brasileiros (~155 agências gov.br + EBC).

## Visão Geral

O scraper coleta notícias publicadas em sites gov.br e da EBC (Agência Brasil, TV Brasil, etc.), extraindo título, conteúdo, data de publicação e metadados. Os dados são inseridos diretamente no PostgreSQL (insert-only, sem dual-write).

A coleta é orquestrada por **DAGs Airflow** (Cloud Composer) que chamam a **API FastAPI** hospedada no Cloud Run.

## Arquitetura

```
DAGs Airflow (a cada 15min)
    → HTTP POST para Cloud Run API
        → Scraper faz fetch do site gov.br
            → Parse HTML → Markdown
                → INSERT no PostgreSQL
```

## Estrutura do Repositório

```
scraper/
├── src/govbr_scraper/
│   ├── api.py                    # FastAPI (Cloud Run)
│   ├── scrapers/
│   │   ├── webscraper.py         # Scraper gov.br (~1200 linhas)
│   │   ├── scrape_manager.py     # Coordenador de scraping gov.br
│   │   ├── ebc_webscraper.py     # Scraper EBC
│   │   ├── ebc_scrape_manager.py # Coordenador de scraping EBC
│   │   └── config/
│   │       └── site_urls.yaml    # Lista de URLs por agência
│   ├── storage/
│   │   ├── storage_adapter.py    # Abstração de persistência
│   │   └── postgres_manager.py   # Acesso ao PostgreSQL
│   └── models/
│       └── news.py               # Modelos Pydantic
├── dags/                          # DAGs Airflow (deploy → {bucket}/scraper/)
│   ├── scrape_agencies.py         # ~158 DAGs dinâmicas (1 por agência)
│   ├── scrape_ebc.py              # 1 DAG para sites EBC
│   └── config/
│       └── site_urls.yaml         # Config de agências para as DAGs
├── docker/
│   └── Dockerfile                 # Imagem da API
├── tests/
│   └── unit/
│       └── test_ebc_scraper.py
├── pyproject.toml
└── .github/workflows/
    ├── scraper-api-deploy.yaml    # Build + deploy Cloud Run
    ├── composer-deploy-dags.yaml  # Deploy DAGs → {bucket}/scraper/
    └── tests.yaml                 # pytest on PR
```

## API Endpoints

A API roda no Cloud Run e é chamada pelas DAGs Airflow.

| Método | Endpoint | Descrição |
|--------|----------|-----------|
| `POST` | `/scrape/agencies` | Raspa sites gov.br (aceita lista de agências, datas) |
| `POST` | `/scrape/ebc` | Raspa sites EBC (Agência Brasil, TV Brasil) |
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
  "allow_update": false,
  "sequential": true
}
```

## DAGs Airflow

| DAG | Quantidade | Schedule | Descrição |
|-----|-----------|----------|-----------|
| `scrape_{agency_key}` | ~158 | A cada 15min | 1 DAG por agência gov.br |
| `scrape_ebc` | 1 | A cada 15min | Sites EBC |

Cada DAG:
- Chama a API no Cloud Run via HTTP POST
- Retry: 2x com backoff de 5min
- Timeout: 15min por execução

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

## Variáveis de Ambiente

```bash
DATABASE_URL=postgresql://...   # PostgreSQL connection string
STORAGE_BACKEND=postgres        # Sempre postgres para o scraper
LOG_LEVEL=INFO                  # Loguru level
```

## Desenvolvimento Local

```bash
# Instalar dependências
poetry install

# Rodar testes
poetry run pytest

# Rodar API localmente
poetry run uvicorn govbr_scraper.api:app --reload
```

## Política de Commits

Commits NÃO devem incluir atribuições ao Claude Code ou co-autoria do Claude.
