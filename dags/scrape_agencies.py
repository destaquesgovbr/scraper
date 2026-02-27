"""
Gera ~158 DAGs de scraping, uma por agência gov.br.

Cada DAG:
- Roda a cada 15 minutos
- Chama a Scraper API no Cloud Run via HTTP
- Retry: 2x com backoff de 5 min
- Timeout: 15 min por execução
"""
import json
import logging
import os
from datetime import datetime, timedelta

import yaml
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


def _load_agencies_config() -> dict:
    """Carrega config de agências ativas do YAML.

    Suporta formato dicionário com campos:
    - url: str (obrigatório)
    - active: bool (opcional, default: True)
    - disabled_reason: str (opcional)
    - disabled_date: str (opcional)

    Returns:
        dict: Mapeamento {agency_key: url} apenas para agências ativas
    """
    config_path = os.path.join(os.path.dirname(__file__), "config", "site_urls.yaml")
    with open(config_path) as f:
        agencies = yaml.safe_load(f)["agencies"]

    # Filtrar apenas agências ativas e extrair URLs
    active_agencies = {}
    for key, data in agencies.items():
        is_active = data.get("active", True)
        if is_active:
            active_agencies[key] = data.get("url")

    return active_agencies


def create_scraper_dag(agency_key: str, agency_url: str):
    """Factory que cria uma DAG de scraping para uma agência."""

    @dag(
        dag_id=f"scrape_{agency_key}",
        description=f"Scrape notícias de {agency_key}",
        schedule="*/15 * * * *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["scraper", "govbr", agency_key],
        default_args={
            "owner": "scraper",
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(minutes=15),
            "execution_timeout": timedelta(minutes=15),
        },
    )
    def scraper_dag():

        @task
        def scrape(**context):
            """Chama Scraper API no Cloud Run para scraping da agência."""
            import google.auth.transport.requests
            import google.oauth2.id_token
            import httpx
            from airflow.models import Variable

            scraper_api_url = Variable.get("scraper_api_url")

            # Token IAM para autenticação no Cloud Run
            auth_req = google.auth.transport.requests.Request()
            token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

            logical_date = context.get("logical_date") or context.get("execution_date")
            if logical_date is None:
                from datetime import datetime as dt
                logical_date = dt.utcnow()

            min_date = (logical_date - timedelta(hours=1)).strftime("%Y-%m-%d")
            max_date = logical_date.strftime("%Y-%m-%d")

            logger.info(f"Calling scraper API for {agency_key}: {min_date} to {max_date}")

            response = httpx.post(
                f"{scraper_api_url}/scrape/agencies",
                json={
                    "start_date": min_date,
                    "end_date": max_date,
                    "agencies": [agency_key],
                    "allow_update": False,
                    "sequential": True,
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=900.0,
            )
            response.raise_for_status()
            result = response.json()
            logger.info("Scraper API response:\n%s", json.dumps(result, indent=2, ensure_ascii=False))

        scrape()

    return scraper_dag()


# Gerar DAGs dinamicamente
for key, url in _load_agencies_config().items():
    globals()[f"scrape_{key}"] = create_scraper_dag(key, url)
