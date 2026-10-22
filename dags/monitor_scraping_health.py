"""
DAG de monitoramento de saude do scraper.

Roda a cada 30 minutos e verifica:
1. Agencias ativas sem nenhuma execucao (scrape_run) em X horas: a DAG
   scrape_<agencia> nao esta chamando a API (DagRun travado, DAG pausada ou ausente)
2. Agencias com N falhas consecutivas
3. Agencias sem noticias em X horas

Envia alertas via Telegram (ou loga se nao configurado).
"""

import logging
import os
import re
from datetime import datetime, timedelta

import yaml
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

# Mesmo arquivo que dags/scrape_agencies.py usa para gerar as DAGs scrape_*.
_AGENCIES_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config", "site_urls.yaml")
_DEFAULT_SILENT_HOURS = 2
# Uma pane global (ex.: Composer sem acesso a API) poe as ~155 agencias numa secao.
_MAX_LINES_PER_SECTION = 25
# O Telegram recusa texto acima de 4096 caracteres e, sem Telegram, a linha
# "Alert (log-only): ..." e cortada em 4096 no Cloud Logging (visto em out/2026,
# quando o fim do alerta sumia). 4000 deixa folga para o prefixo.
_MAX_MESSAGE_CHARS = 4000
_TRUNCATED_MARKER = "... (alerta truncado; lista completa no log da task)"

# NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
# (SILENT_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
_SILENT_AGENCIES_QUERY = """
    SELECT k.agency_key, last_run.scraped_at AS last_run_at
    FROM unnest(%(agencies)s::text[]) AS k(agency_key)
    LEFT JOIN LATERAL (
        SELECT sr.scraped_at
        FROM scrape_runs sr
        WHERE sr.agency_key = k.agency_key
        ORDER BY sr.scraped_at DESC LIMIT 1
    ) AS last_run ON TRUE
    WHERE last_run.scraped_at IS NULL
       OR last_run.scraped_at < NOW() - INTERVAL '1 hour' * %(silent_hours)s
    ORDER BY k.agency_key
"""


def _sanitize(value) -> str:
    """Remove control characters from a value before using in logs/messages."""
    if not value:
        return ""
    return re.sub(r"[\x00-\x1f\x7f-\x9f]", "", str(value))


def _load_active_agencies(config_path: str = _AGENCIES_CONFIG_PATH) -> list[str]:
    """Agencias ativas do site_urls.yaml, as mesmas que viram DAG em scrape_agencies.py."""
    with open(config_path) as f:
        agencies = yaml.safe_load(f)["agencies"]
    return sorted(key for key, data in agencies.items() if data.get("active", True))


def _get_silent_hours() -> int:
    """Janela (horas) sem nenhum scrape_run para considerar a agencia silenciosa."""
    from airflow.models import Variable

    hours = int(Variable.get("scraper_silent_hours", default_var=_DEFAULT_SILENT_HOURS))
    # Janela 0 marcaria todas as agencias como silenciosas.
    return max(1, hours)


def _capped(lines: list[str]) -> list[str]:
    if len(lines) <= _MAX_LINES_PER_SECTION:
        return lines
    hidden = len(lines) - _MAX_LINES_PER_SECTION
    return lines[:_MAX_LINES_PER_SECTION] + [f"... e mais {hidden}"]


def _fit_message(message: str) -> str:
    """Corta a mensagem em fim de linha para caber em _MAX_MESSAGE_CHARS.

    Cada linha fecha as proprias tags <b>, entao cortar entre linhas nao quebra
    o HTML do Telegram.
    """
    if len(message) <= _MAX_MESSAGE_CHARS:
        return message
    budget = _MAX_MESSAGE_CHARS - len(_TRUNCATED_MARKER) - 1
    kept: list[str] = []
    size = 0
    for line in message.split("\n"):
        extra = len(line) + (1 if kept else 0)
        if size + extra > budget:
            break
        kept.append(line)
        size += extra
    return "\n".join(kept + [_TRUNCATED_MARKER])


def _build_alert_message(
    failures: list[dict],
    stale: list[dict],
    silent: list[dict] | None = None,
    silent_hours: int = _DEFAULT_SILENT_HOURS,
) -> str | None:
    """Monta o alerta agregado (HTML do Telegram, so com <b>), ou None se nao ha o que alertar.

    A secao de agencias silenciosas vem primeiro: e a unica em que nada esta sendo
    raspado e nenhuma outra checagem enxerga o problema, e o fim de um alerta longo
    se perde (limite de 4096 caracteres).
    """
    parts = []
    if silent:
        header = (
            "<b>Alerta: Agencias Sem Execucao</b>\n"
            f"{len(silent)} agencia(s) ativa(s) sem nenhum scrape_run nas ultimas "
            f"{silent_hours}h: a DAG scrape_* da agencia nao esta chamando a API "
            "(DagRun travado em running, DAG pausada ou ausente) ou a chamada nao "
            "chega a gravar o resultado. "
            "Ver docs/runbook.md, secao 'DAG de Agencia Travada'."
        )
        lines = []
        for r in silent:
            key = _sanitize(r["agency_key"])
            if r.get("last_run_at") is None:
                lines.append(f"- <b>{key}</b>: nenhuma execucao registrada em scrape_runs")
            else:
                lines.append(f"- <b>{key}</b>: ultima execucao em {r['last_run_at']}")
        parts.append(header + "\n\n" + "\n".join(_capped(lines)))

    if failures:
        lines = [
            f"- <b>{_sanitize(r['agency_key'])}</b>: {_sanitize(r['last_error'])} "
            f"(ultima falha: {r['last_failure_at']})"
            for r in failures
        ]
        parts.append(
            "<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n" + "\n".join(_capped(lines))
        )

    if stale:
        lines = [
            f"- <b>{_sanitize(r['agency_key'])}</b>: ultima noticia em {r['last_success_at']}"
            for r in stale
        ]
        parts.append("<b>Alerta: Agencias Sem Noticias</b>\n\n" + "\n".join(_capped(lines)))

    if not parts:
        return None
    return _fit_message("\n\n".join(parts))


@dag(
    dag_id="monitor_scraping_health",
    description=(
        "Monitora saude do scraper: agencias sem execucao, falhas consecutivas "
        "e agencias sem noticias"
    ),
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
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        threshold = int(Variable.get("scraper_consecutive_failure_threshold", default_var=3))
        window_hours = int(Variable.get("scraper_failure_window_hours", default_var=2))
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        # NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
        # (find_consecutive_failures). Changes here must be replicated there and vice-versa.
        query = """
            WITH ranked AS (
                SELECT agency_key, status, error_category, scraped_at,
                       ROW_NUMBER() OVER (PARTITION BY agency_key ORDER BY scraped_at DESC) AS rn
                FROM scrape_runs
                WHERE scraped_at > NOW() - INTERVAL '1 hour' * %(window_hours)s
            ),
            recent AS (
                SELECT agency_key, status, error_category, scraped_at, rn
                FROM ranked
                WHERE rn <= %(threshold)s
            )
            SELECT
                agency_key,
                COUNT(*) AS consecutive_failures,
                MAX(error_category) FILTER (WHERE rn = 1) AS last_error,
                MAX(scraped_at) AS last_failure_at
            FROM recent
            GROUP BY agency_key
            HAVING COUNT(*) = %(threshold)s
               AND COUNT(*) FILTER (WHERE status = 'error') = %(threshold)s
        """

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, {"threshold": threshold, "window_hours": window_hours})
                results = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        if results:
            logger.warning(f"Agencias com {threshold}+ falhas consecutivas: {len(results)}")
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: {_sanitize(r['last_error'])} "
                    f"(ultima falha: {r['last_failure_at']})"
                )
        else:
            logger.info("Nenhuma agencia com falhas consecutivas.")

        return results

    @task
    def check_stale_agencies() -> list[dict]:
        """Verifica agencias sem noticias em X horas."""
        import psycopg2
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        stale_hours = int(Variable.get("scraper_stale_hours", default_var=24))
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        # NOTE: This query is duplicated in src/govbr_scraper/monitoring/health_checks.py
        # (find_stale_agencies). Changes here must be replicated there and vice-versa.
        query = """
            SELECT
                agency_key,
                MAX(scraped_at) FILTER (
                    WHERE status = 'success' AND articles_saved > 0
                ) AS last_success_at
            FROM scrape_runs
            WHERE scraped_at > NOW() - INTERVAL '90 days'
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
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: ultima noticia em {r['last_success_at']}"
                )
        else:
            logger.info("Todas as agencias com noticias recentes.")

        return results

    @task
    def check_silent_agencies() -> list[dict]:
        """Verifica agencias ativas sem nenhum scrape_run (sucesso ou erro) em X horas.

        Cada chamada da API grava um scrape_run por agencia, com sucesso ou erro.
        Agencia ativa sem nenhum na janela => a DAG dela nao esta rodando. Caso real
        (out/2026): DagRuns presos em 'running' sem task instances, numa DagVersion
        sem serialized_dag, travaram scrape_inma, scrape_inpe, scrape_aids e
        scrape_patrimonio por meses (max_active_runs=1). As outras checagens partem
        de scrape_runs e nao enxergam esse caso.
        """
        import psycopg2
        from airflow.models import Variable
        from psycopg2.extras import RealDictCursor

        silent_hours = _get_silent_hours()
        database_url = Variable.get("scraper_database_url", default_var="")
        if not database_url:
            raise ValueError("Missing required Airflow Variable: scraper_database_url")

        active_agencies = _load_active_agencies()

        conn = psycopg2.connect(database_url)
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    _SILENT_AGENCIES_QUERY,
                    {"agencies": active_agencies, "silent_hours": silent_hours},
                )
                results = [dict(row) for row in cur.fetchall()]
        finally:
            conn.close()

        if results:
            logger.warning(
                f"Agencias ativas sem nenhuma execucao em {silent_hours}h: "
                f"{len(results)} de {len(active_agencies)}"
            )
            for r in results:
                logger.warning(
                    f"  {_sanitize(r['agency_key'])}: ultima execucao em {r['last_run_at']}"
                )
        else:
            logger.info(f"Todas as {len(active_agencies)} agencias ativas executaram.")

        return results

    @task
    def send_alerts(failures: list[dict], stale: list[dict], silent: list[dict]):
        """Envia alertas agregados via Telegram (ou loga)."""
        from airflow.models import Variable
        from scraper.notify import send_alert

        message = _build_alert_message(failures, stale, silent, _get_silent_hours())
        if message is None:
            logger.info("Sem alertas para enviar.")
            return

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
    silent = check_silent_agencies()
    send_alerts(failures, stale, silent)


dag_instance = monitor_scraping_health_dag()
