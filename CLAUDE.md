# DestaquesGovBr Scraper

Scraper standalone para notícias de sites governamentais brasileiros (gov.br) e EBC.

## Arquitetura

```
scraper/
├── src/govbr_scraper/       # Código Python do scraper
│   ├── api.py               # FastAPI (Cloud Run)
│   ├── scrapers/             # Lógica de scraping
│   │   ├── webscraper.py     # Scraper gov.br (~1200 linhas)
│   │   ├── scrape_manager.py # Coordenador de scraping
│   │   ├── ebc_webscraper.py # Scraper EBC
│   │   └── ebc_scrape_manager.py
│   ├── storage/              # Persistência (postgres-only)
│   │   ├── storage_adapter.py
│   │   └── postgres_manager.py
│   └── models/news.py        # Modelos Pydantic
├── dags/                     # DAGs Airflow (deploy → {bucket}/scraper/)
│   ├── scrape_agencies.py    # ~155 DAGs dinâmicas
│   ├── scrape_ebc.py         # 1 DAG EBC
│   └── config/site_urls.yaml
├── docker/Dockerfile
└── .github/workflows/
    ├── scraper-api-deploy.yaml    # Build + deploy Cloud Run
    ├── composer-deploy-dags.yaml  # Deploy DAGs → {bucket}/scraper/
    └── tests.yaml                 # pytest on PR
```

## Deploy

- **Scraper API**: Cloud Run (`destaquesgovbr-scraper-api`), deploy via GitHub Actions
- **DAGs**: Cloud Composer, deploy para `{bucket}/scraper/` via gsutil rsync
- **Infra**: Terraform no repo `infra/` (SA, Cloud Run, Artifact Registry, IAM)

## Variáveis de Ambiente

```bash
DATABASE_URL=postgresql://...   # PostgreSQL connection string
STORAGE_BACKEND=postgres        # Always postgres for scraper
LOG_LEVEL=INFO                  # Loguru level
```

## Testes

```bash
poetry install
poetry run pytest
```

## Política de Commits

Commits NÃO devem incluir atribuições ao Claude Code ou co-autoria do Claude.
