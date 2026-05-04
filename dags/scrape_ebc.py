"""DAG para scraping de notícias EBC (Agência Brasil, TV Brasil) via Cloud Run API."""
import json
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


def _on_scrape_failure(context):
    """Callback para log estruturado de falha na DAG de scraping."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "unknown"
    task_id = ti.task_id if ti else "unknown"
    exception = context.get("exception", "unknown")
    try_number = ti.try_number if ti else 0
    logger.error(
        "Scrape DAG failure: dag=%s task=%s try=%d error=%s",
        dag_id, task_id, try_number, exception,
    )


@dag(
    dag_id="scrape_ebc",
    description="Scrape notícias EBC (Agência Brasil, TV Brasil)",
    schedule="0/10 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "ebc"],
    default_args={
        "owner": "scraper",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": _on_scrape_failure,
    },
)
def scrape_ebc_dag():

    @task
    def scrape_ebc(**context):
        """Chama Scraper API no Cloud Run para scraping EBC."""
        import google.auth.transport.requests
        import google.oauth2.id_token
        import httpx
        from airflow.models import Variable

        scraper_api_url = Variable.get("scraper_api_url", default_var="")
        if not scraper_api_url:
            raise ValueError("Missing required Airflow Variable: scraper_api_url")

        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

        logical_date = context.get("logical_date") or context.get("execution_date")
        if logical_date is None:
            from datetime import datetime as dt
            logical_date = dt.utcnow()

        min_date = (logical_date - timedelta(hours=1)).strftime("%Y-%m-%d")
        max_date = logical_date.strftime("%Y-%m-%d")

        logger.info(f"Calling scraper API for EBC: {min_date} to {max_date}")

        response = httpx.post(
            f"{scraper_api_url}/scrape/ebc",
            json={
                "start_date": min_date,
                "end_date": max_date,
                "allow_update": False,
                "sequential": True,
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=900.0,
        )
        response.raise_for_status()
        result = response.json()
        logger.info("Scraper API response:\n%s", json.dumps(result, indent=2, ensure_ascii=False))

        # Verificar status da resposta (API pode retornar 200 com falha lógica)
        if result.get("status") in ("failed", "partial"):
            from airflow.exceptions import AirflowException
            errors = result.get("errors", [])
            raise AirflowException(
                f"EBC scraping {result['status']}: {errors}"
            )

    scrape_ebc()


dag_instance = scrape_ebc_dag()
