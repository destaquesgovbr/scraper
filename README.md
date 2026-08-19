# DestaquesGovBr Scraper

[![Tests](https://github.com/destaquesgovbr/scraper/actions/workflows/tests.yaml/badge.svg)](https://github.com/destaquesgovbr/scraper/actions/workflows/tests.yaml)

Coleta automatizada de notícias de ~155 agências do governo brasileiro (sites gov.br) e da EBC (Agência Brasil, TV Brasil).

## O que faz

- Raspa notícias de sites gov.br usando parsing HTML, API Plone 6, ou scraper EBC especializado
- Armazena artigos em PostgreSQL com deduplicação por content hash e URL
- Publica eventos no Google Cloud Pub/Sub para processamento downstream
- Monitora saúde do scraping com alertas automáticos via Telegram
- Executa a cada 10 minutos via Airflow DAGs, cobrindo todas as agências

## Arquitetura

```
Airflow (Cloud Composer)  →  API (Cloud Run)  →  PostgreSQL
                                    │
                                    └──→  Pub/Sub (dgb.news.scraped)
```

## Quick Start

```bash
# Instalar dependências
poetry install

# Rodar testes
poetry run pytest

# Rodar API localmente
poetry run uvicorn govbr_scraper.api:app --reload
```

Requer Python 3.12+.

## Development Setup

### Prerequisites

- Python 3.12+
- Poetry 1.8+
- Pre-commit

⚠️ **Important:** Run `poetry install` BEFORE `pre-commit install`. Some hooks depend on Poetry being available in PATH.

### Setup

```bash
# 1. Install dependencies (REQUIRED FIRST)
poetry install

# 2. Install pre-commit hooks (AFTER poetry install)
poetry run pre-commit install

# 3. Run all pre-commit checks manually (first-time setup)
poetry run pre-commit run --all-files
```

**Note:** If you install pre-commit before running `poetry install`, some hooks (like `test-config-sync`) will fail with "poetry: command not found".

### Code Quality

This project uses pre-commit hooks to maintain code quality and security:

| Category | Hooks | Description |
|----------|-------|-------------|
| **Security** 🔒 | `detect-secrets`, `bandit` | Prevent credential leaks + SSRF protection |
| **Python Quality** 🐍 | `ruff-check`, `ruff-format` | Linter + formatter (line-length 100) |
| **Type Checking** 📝 | `mypy` | Type checking (enabled for `models/`, `config.py`) |
| **File Hygiene** 📄 | `trailing-whitespace`, `end-of-file-fixer`, `mixed-line-ending` | Normalize whitespace |
| **Syntax Validation** ✅ | `check-yaml`, `check-json`, `check-toml` | Validate config files |
| **Git Safety** 🚨 | `check-merge-conflict`, `check-added-large-files` | Detect markers + large files |
| **Custom Validators** ⚙️ | `validate-site-urls`, `validate-ssrf-allowlist`, `test-config-sync` | Project-specific checks |

#### Performance

- **Typical commit**: ~7-9 seconds
- **First commit**: ~30-60 seconds (installs hook environments)

#### Emergency Bypass

```bash
# Only for emergencies (not recommended)
git commit --no-verify
```

**Note**: CI always runs pre-commit, so issues will be caught even with `--no-verify`.

### Code Standards

- **Type hints**: Required for public functions
- **Formatting**: Ruff formatter (max line 100)
- **Linting**: Ruff (replaces Black, Flake8, isort, pyupgrade)
- **Type checking**: Mypy enabled for core modules

```bash
# Run manually (outside pre-commit)
poetry run ruff check .        # Linting
poetry run ruff format .       # Formatting
poetry run mypy src/           # Type checking
```

### site_urls.yaml Sync

⚠️ **IMPORTANT**: The file `site_urls.yaml` exists in TWO locations that must stay synchronized:

- **Always edit**: `src/govbr_scraper/scrapers/config/site_urls.yaml` (source)
- **Then copy to**: `dags/config/site_urls.yaml` (used by Airflow DAGs)

**Sync command**:
```bash
cp src/govbr_scraper/scrapers/config/site_urls.yaml dags/config/site_urls.yaml
```

Pre-commit will block commits if files are out of sync.

## Stack

- **API:** FastAPI + Uvicorn (Cloud Run)
- **Orquestração:** Apache Airflow (Cloud Composer)
- **Banco:** PostgreSQL (Cloud SQL)
- **Eventos:** Google Cloud Pub/Sub
- **Alertas:** Telegram Bot API
- **CI/CD:** GitHub Actions (testes, deploy API, deploy DAGs)

## Documentação

| Documento | Conteúdo |
|-----------|----------|
| [CLAUDE.md](CLAUDE.md) | Documentação técnica completa (arquitetura, schema, features, API, contribuição) |
| [docs/runbook.md](docs/runbook.md) | Procedimentos operacionais (adicionar agência, debug, etc.) |

## Licença

Uso interno — DestaquesGovBr.
