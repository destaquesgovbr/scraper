"""
DAG de retencao da tabela scrape_runs.

Roda diariamente e deleta registros mais antigos que N dias
para evitar crescimento ilimitado (~22k rows/dia com 155 agencias).
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="cleanup_old_scrape_runs",
    description="Remove registros antigos de scrape_runs para controle de retencao",
    schedule="0 3 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "monitoring", "maintenance"],
    default_args={
        "owner": "scraper",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=15),
    },
)
def cleanup_old_scrape_runs_dag():

    @task
    def delete_old_runs() -> dict:
        """Deleta registros de scrape_runs mais antigos que retention_days."""
        import psycopg2
        from airflow.models import Variable

        retention_days = int(Variable.get("scraper_retention_days", default_var=90))
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM scrape_runs WHERE scraped_at < NOW() - INTERVAL '1 day' * %s",
                    (retention_days,),
                )
                deleted = cur.rowcount
            conn.commit()

            logger.info(f"Deleted {deleted} scrape_runs older than {retention_days} days")
            return {"deleted": deleted, "retention_days": retention_days}
        finally:
            conn.close()

    delete_old_runs()


dag_instance = cleanup_old_scrape_runs_dag()
