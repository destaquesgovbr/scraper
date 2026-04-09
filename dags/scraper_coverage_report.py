"""
DAG de relatorio de cobertura do scraper.

Roda diariamente e reporta:
1. Quantas agencias foram raspadas vs total ativas
2. Ratio de cobertura
3. Top erros por categoria
4. Alerta se cobertura abaixo do threshold
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)


@dag(
    dag_id="scraper_coverage_report",
    description="Relatorio diario de cobertura do scraper",
    schedule="0 8 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "monitoring", "report"],
    default_args={
        "owner": "scraper",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
    },
)
def scraper_coverage_report_dag():

    @task
    def generate_report() -> dict:
        """Gera relatorio de cobertura das ultimas 24h."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        from airflow.models import Variable

        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        coverage_query = """
            SELECT
                COUNT(DISTINCT agency_key) FILTER (WHERE status = 'success') AS agencies_scraped,
                COUNT(DISTINCT agency_key) FILTER (WHERE status = 'error') AS agencies_with_errors,
                COALESCE(SUM(articles_saved), 0) AS total_articles,
                (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs) AS total_active
            FROM scrape_runs
            WHERE scraped_at > NOW() - INTERVAL '24 hours'
        """

        top_errors_query = """
            SELECT error_category, COUNT(*) AS count
            FROM scrape_runs
            WHERE scraped_at > NOW() - INTERVAL '24 hours'
              AND status = 'error'
              AND error_category IS NOT NULL
            GROUP BY error_category
            ORDER BY count DESC
            LIMIT 5
        """

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(coverage_query)
                coverage = dict(cur.fetchone() or {})

                cur.execute(top_errors_query)
                top_errors = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        total_active = coverage.get("total_active", 0) or 0
        agencies_scraped = coverage.get("agencies_scraped", 0) or 0
        coverage_ratio = round(agencies_scraped / total_active, 2) if total_active > 0 else 0

        report = {
            "total_active": total_active,
            "agencies_scraped": agencies_scraped,
            "agencies_with_errors": coverage.get("agencies_with_errors", 0) or 0,
            "total_articles": coverage.get("total_articles", 0) or 0,
            "coverage_ratio": coverage_ratio,
            "top_errors": top_errors,
        }

        logger.info(
            f"Cobertura 24h: {agencies_scraped}/{total_active} agencias "
            f"({coverage_ratio:.0%}), {report['total_articles']} artigos, "
            f"{report['agencies_with_errors']} com erros"
        )
        for err in top_errors:
            logger.info(f"  Top erro: {err['error_category']} = {err['count']}")

        return report

    @task
    def alert_on_low_coverage(report: dict):
        """Alerta se cobertura abaixo do threshold."""
        from airflow.models import Variable
        from notify import send_alert

        min_ratio = float(Variable.get("scraper_min_coverage_ratio", default_var=0.8))

        if report["coverage_ratio"] >= min_ratio:
            logger.info(
                f"Cobertura OK: {report['coverage_ratio']:.0%} >= {min_ratio:.0%}"
            )
            return

        errors_text = ""
        if report.get("top_errors"):
            error_lines = [f"  - {e['error_category']}: {e['count']}" for e in report["top_errors"]]
            errors_text = "\n\n<b>Top erros:</b>\n" + "\n".join(error_lines)

        message = (
            f"<b>Alerta: Cobertura Baixa do Scraper</b>\n\n"
            f"Cobertura 24h: <b>{report['coverage_ratio']:.0%}</b> "
            f"(threshold: {min_ratio:.0%})\n"
            f"Agencias raspadas: {report['agencies_scraped']}/{report['total_active']}\n"
            f"Agencias com erros: {report['agencies_with_errors']}\n"
            f"Total artigos: {report['total_articles']}"
            f"{errors_text}"
        )

        token = Variable.get("scraper_telegram_bot_token", default_var=None)
        chat_id = Variable.get("scraper_telegram_monitor_chat_id", default_var=None)
        webhook = Variable.get("scraper_alert_webhook_url", default_var=None)

        send_alert(
            message=message,
            telegram_token=token,
            telegram_chat_id=chat_id,
            webhook_url=webhook,
        )

    report = generate_report()
    alert_on_low_coverage(report)


dag_instance = scraper_coverage_report_dag()
