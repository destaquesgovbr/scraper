"""Health check queries for scraper monitoring.

These functions execute SQL against a provided database connection (psycopg2).
They are used by the monitoring DAG and by tests.
"""

from __future__ import annotations

from collections.abc import Iterable

from psycopg2.extras import RealDictCursor

# Última execução de cada agência ativa (LATERAL top-1 pelo índice
# idx_scrape_runs_agency_scraped) e filtro das que não rodaram na janela.
# Qualquer scrape_run conta, sucesso ou erro: agência que falha é falha, não silêncio.
#
# NOTE: This query is duplicated in dags/monitor_scraping_health.py
# (_SILENT_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
SILENT_AGENCIES_QUERY = """
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


def find_consecutive_failures(conn, threshold: int = 3, window_hours: int = 2) -> list[dict]:
    """Find agencies with N or more consecutive recent failures.

    Args:
        conn: psycopg2 connection (or mock).
        threshold: Number of consecutive failures to trigger alert.
        window_hours: How many hours back to look for failures.

    Returns:
        List of dicts with agency_key, consecutive_failures, last_error, last_failure_at.

    NOTE: This query is duplicated in dags/monitor_scraping_health.py (check_consecutive_failures).
    Changes here must be replicated there and vice-versa.
    """
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
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, {"threshold": threshold, "window_hours": window_hours})
        return [dict(row) for row in cur.fetchall()]


def find_stale_agencies(conn, stale_hours: int = 24) -> list[dict]:
    """Find agencies that had successful scrapes but none with articles recently.

    Args:
        conn: psycopg2 connection (or mock).
        stale_hours: Hours without articles to consider stale.

    Returns:
        List of dicts with agency_key, last_success_at.

    NOTE: This query is duplicated in dags/monitor_scraping_health.py (check_stale_agencies).
    Changes here must be replicated there and vice-versa.
    """
    query = """
        SELECT
            agency_key,
            MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0) AS last_success_at
        FROM scrape_runs
        WHERE scraped_at > NOW() - INTERVAL '90 days'
        GROUP BY agency_key
        HAVING MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0)
               < NOW() - INTERVAL '1 hour' * %(stale_hours)s
           AND MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0) IS NOT NULL
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, {"stale_hours": stale_hours})
        return [dict(row) for row in cur.fetchall()]


def find_silent_agencies(conn, active_agencies: Iterable[str], silent_hours: int = 2) -> list[dict]:
    """Find active agencies with no scrape_run at all (success or error) in the window.

    Every call to the scraper API records one scrape_run per agency, success or
    error. An active agency without any run means its DAG is not calling the API:
    a DagRun stuck in 'running' (with max_active_runs=1, e.g. pointing to a
    DagVersion without serialized_dag), a paused DAG or a DAG missing from Airflow.
    find_consecutive_failures and find_stale_agencies start from scrape_runs, so
    they cannot see this case.

    Args:
        conn: psycopg2 connection (or mock).
        active_agencies: Agency keys expected to run (active in site_urls.yaml).
        silent_hours: Hours without any run to consider the agency silent.

    Returns:
        List of dicts with agency_key and last_run_at (None when there is no run
        within the scrape_runs retention), sorted by agency_key.
    """
    agencies = list(active_agencies)
    if not agencies:
        return []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SILENT_AGENCIES_QUERY, {"agencies": agencies, "silent_hours": silent_hours})
        return [dict(row) for row in cur.fetchall()]


def compute_coverage_report(conn, hours: int = 24) -> dict:
    """Compute scraping coverage metrics over the last N hours.

    Args:
        conn: psycopg2 connection (or mock).
        hours: Time window for coverage calculation.

    Returns:
        Dict with total_active, agencies_scraped, agencies_with_errors,
        total_articles, coverage_ratio.
    """
    query = """
        SELECT
            COUNT(DISTINCT agency_key) FILTER (WHERE status = 'success') AS agencies_scraped,
            COUNT(DISTINCT agency_key) FILTER (WHERE status = 'error') AS agencies_with_errors,
            COALESCE(SUM(articles_saved), 0) AS total_articles,
            (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs
             WHERE scraped_at > NOW() - INTERVAL '7 days') AS total_active,
            CASE
                WHEN (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs
                      WHERE scraped_at > NOW() - INTERVAL '7 days') > 0
                THEN ROUND(
                    COUNT(DISTINCT agency_key) FILTER (WHERE status = 'success')::numeric /
                    (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs
                     WHERE scraped_at > NOW() - INTERVAL '7 days')::numeric,
                    2
                )
                ELSE 0
            END AS coverage_ratio
        FROM scrape_runs
        WHERE scraped_at > NOW() - INTERVAL '1 hour' * %(hours)s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, {"hours": hours})
        row = cur.fetchall()
        if row:
            return dict(row[0])
        return {
            "total_active": 0,
            "agencies_scraped": 0,
            "agencies_with_errors": 0,
            "total_articles": 0,
            "coverage_ratio": 0,
        }
