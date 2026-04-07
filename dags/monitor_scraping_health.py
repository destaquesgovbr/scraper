"""
DAG de monitoramento de saude do scraper.

Roda a cada 30 minutos e verifica:
1. Agencias com N falhas consecutivas
2. Agencias sem noticias em X horas

Envia alertas via Telegram (ou loga se nao configurado).
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="monitor_scraping_health",
    description="Monitora saude do scraper: falhas consecutivas e agencias sem noticias",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "monitoring"],
    default_args={
        "owner": "scraper",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=5),
    },
)
def monitor_scraping_health_dag():

    @task
    def check_consecutive_failures() -> list[dict]:
        """Verifica agencias com falhas consecutivas."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        from airflow.models import Variable

        threshold = int(Variable.get("scraper_consecutive_failure_threshold", default_var=3))
        database_url = Variable.get("scraper_database_url")

        query = """
            WITH ranked AS (
                SELECT agency_key, status, error_category, scraped_at,
                       ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY scraped_at DESC) AS rn
                FROM scrape_runs
                WHERE scraped_at > NOW() - INTERVAL '24 hours'
            ),
            recent AS (
                SELECT agency_key, status, error_category, scraped_at
                FROM ranked
                WHERE rn <= %(threshold)s
            )
            SELECT
                agency_key,
                COUNT(*) AS consecutive_failures,
                MAX(error_category) AS last_error,
                MAX(scraped_at) AS last_failure_at
            FROM recent
            GROUP BY agency_key
            HAVING COUNT(*) = %(threshold)s
               AND COUNT(*) FILTER (WHERE status = 'error') = %(threshold)s
        """

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, {"threshold": threshold})
                results = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        if results:
            logger.warning(f"Agencias com {threshold}+ falhas consecutivas: {len(results)}")
            for r in results:
                logger.warning(
                    f"  {r['agency_key']}: {r['last_error']} (ultima falha: {r['last_failure_at']})"
                )
        else:
            logger.info("Nenhuma agencia com falhas consecutivas.")

        return results

    @task
    def check_stale_agencies() -> list[dict]:
        """Verifica agencias sem noticias em X horas."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        from airflow.models import Variable

        stale_hours = int(Variable.get("scraper_stale_hours", default_var=24))
        database_url = Variable.get("scraper_database_url")

        query = """
            SELECT
                agency_key,
                MAX(scraped_at) FILTER (
                    WHERE status = 'success' AND articles_saved > 0
                ) AS last_success_at
            FROM scrape_runs
            GROUP BY agency_key
            HAVING MAX(scraped_at) FILTER (
                       WHERE status = 'success' AND articles_saved > 0
                   ) < NOW() - INTERVAL '1 hour' * %(stale_hours)s
               AND MAX(scraped_at) FILTER (
                       WHERE status = 'success' AND articles_saved > 0
                   ) IS NOT NULL
        """

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, {"stale_hours": stale_hours})
                results = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        if results:
            logger.warning(f"Agencias sem noticias em {stale_hours}h: {len(results)}")
            for r in results:
                logger.warning(f"  {r['agency_key']}: ultima noticia em {r['last_success_at']}")
        else:
            logger.info("Todas as agencias com noticias recentes.")

        return results

    @task
    def send_alerts(failures: list[dict], stale: list[dict]):
        """Envia alertas agregados via Telegram (ou loga)."""
        from airflow.models import Variable
        from notify import send_alert

        if not failures and not stale:
            logger.info("Sem alertas para enviar.")
            return

        parts = []
        if failures:
            lines = [f"- <b>{r['agency_key']}</b>: {r['last_error']} (ultima falha: {r['last_failure_at']})"
                     for r in failures]
            parts.append(
                "<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n" + "\n".join(lines)
            )

        if stale:
            lines = [f"- <b>{r['agency_key']}</b>: ultima noticia em {r['last_success_at']}"
                     for r in stale]
            parts.append(
                "<b>Alerta: Agencias Sem Noticias</b>\n\n" + "\n".join(lines)
            )

        message = "\n\n".join(parts)

        token = Variable.get("scraper_telegram_bot_token", default_var=None)
        chat_id = Variable.get("scraper_telegram_monitor_chat_id", default_var=None)
        webhook = Variable.get("scraper_alert_webhook_url", default_var=None)

        send_alert(
            message=message,
            telegram_token=token,
            telegram_chat_id=chat_id,
            webhook_url=webhook,
        )

    failures = check_consecutive_failures()
    stale = check_stale_agencies()
    send_alerts(failures, stale)


dag_instance = monitor_scraping_health_dag()
