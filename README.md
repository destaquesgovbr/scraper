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
