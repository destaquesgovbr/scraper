"""Health check queries for scraper monitoring.

These functions execute SQL against a provided database connection (psycopg2).
They are used by the monitoring DAG and by tests.
"""

from __future__ import annotations

from collections.abc import Iterable

from psycopg2 import errors
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


# Agências cuja última notícia NOVA é mais antiga que stale_hours (e que tiveram alguma nos
# 90 dias de retenção). articles_new (migration 037 do data-platform) conta só as linhas
# inseridas; articles_saved soma as atualizadas por re-scrape, e a EBC e o fallback Plone 6
# (que relêem a janela inteira) nunca ficavam "sem notícias". Linhas anteriores à 037 têm
# articles_new NULL e caem no articles_saved.
#
# NOTE: This query is duplicated in dags/monitor_scraping_health.py
# (_STALE_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
STALE_AGENCIES_QUERY = """
    SELECT
        agency_key,
        MAX(scraped_at) FILTER (
            WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
        ) AS last_success_at
    FROM scrape_runs
    WHERE scraped_at > NOW() - INTERVAL '90 days'
    GROUP BY agency_key
    HAVING MAX(scraped_at) FILTER (
               WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
           ) < NOW() - INTERVAL '1 hour' * %(stale_hours)s
       AND MAX(scraped_at) FILTER (
               WHERE status = 'success' AND COALESCE(articles_new, articles_saved) > 0
           ) IS NOT NULL
"""


# Agências cujos últimos `threshold` scrape_runs da janela são todos erro (erro passageiro:
# até 3 linhas por execução, uma por tentativa) OU cujo scrape_run mais recente é um erro
# permanente (PERMANENT_ERROR_CATEGORIES de models/monitoring.py: a DAG falha sem retry e
# grava 1 linha por execução; sem esse ramo a agência quebrada só entraria depois de 3
# execuções e sumiria toda manhã, com 1 linha às 06h30 e 2 às 08h30).
# consecutive_failures = erros seguidos a partir do mais recente (até `threshold`).
#
# NOTE: This query is duplicated in dags/monitor_scraping_health.py
# (_CONSECUTIVE_FAILURES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
CONSECUTIVE_FAILURES_QUERY = """
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
        COALESCE(MIN(rn) FILTER (WHERE status <> 'error') - 1, COUNT(*)) AS consecutive_failures,
        MAX(error_category) FILTER (WHERE rn = 1) AS last_error,
        MAX(scraped_at) AS last_failure_at
    FROM recent
    GROUP BY agency_key
    HAVING (COUNT(*) = %(threshold)s AND COUNT(*) FILTER (WHERE status = 'error') = %(threshold)s)
        OR BOOL_OR(
               rn = 1 AND status = 'error'
               AND error_category IN ('url_broken', 'access_denied', 'html_changed', 'anti_bot')
           )
"""


# Agências com o disjuntor aberto (open ou half_open) em scrape_agency_state (migration 038
# do data-platform). Saem de "Agências Sem Execução" e das falhas consecutivas: a API pula a
# agência sem gravar scrape_run. No lugar delas, o monitor lista os circuitos abertos numa
# seção própria (o alerta de abertura do scraper-api sai uma vez só, sem retry). Sem a
# tabela, nada muda.
#
# NOTE: This query is duplicated in dags/monitor_scraping_health.py
# (_OPEN_CIRCUIT_AGENCIES_QUERY). tests/unit/test_dag_monitor_health.py checks they match.
OPEN_CIRCUIT_AGENCIES_QUERY = """
    SELECT agency_key, circuit_state, circuit_opened_at, next_due_at, last_error_category
    FROM scrape_agency_state
    WHERE circuit_state <> 'closed'
    ORDER BY agency_key
"""


def find_open_circuits(conn) -> list[dict]:
    """Agências com o disjuntor aberto, com estado, abertura, próxima sonda e último erro.

    Tabela ausente (038 não aplicada): rollback da transação abortada e lista vazia.
    """
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(OPEN_CIRCUIT_AGENCIES_QUERY)
            return [dict(row) for row in cur.fetchall()]
    except errors.UndefinedTable:
        conn.rollback()
        return []


def find_open_circuit_agencies(conn) -> set[str]:
    """Siglas das agências com o disjuntor aberto; vazio se scrape_agency_state não existe."""
    return {row["agency_key"] for row in find_open_circuits(conn)}


def _without_open_circuits(conn, rows: list[dict]) -> list[dict]:
    open_circuits = find_open_circuit_agencies(conn)
    return [r for r in rows if r["agency_key"] not in open_circuits]


def find_consecutive_failures(
    conn, threshold: int = 3, window_hours: int = 6, skip_open_circuits: bool = True
) -> list[dict]:
    """Find agencies with N or more consecutive recent failures, or a permanent last error.

    Args:
        conn: psycopg2 connection (or mock).
        threshold: Number of consecutive failures to trigger alert.
        window_hours: How many hours back to look for failures (default 6: three runs of
            the scrape_agencies DAG, which runs every 2 hours).
        skip_open_circuits: Tira as agências com o disjuntor aberto (como o monitor).

    Returns:
        List of dicts with agency_key, consecutive_failures, last_error, last_failure_at.

    NOTE: This query (CONSECUTIVE_FAILURES_QUERY) and the defaults are duplicated in
    dags/monitor_scraping_health.py; tests/unit/test_dag_monitor_health.py checks they match.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            CONSECUTIVE_FAILURES_QUERY, {"threshold": threshold, "window_hours": window_hours}
        )
        rows = [dict(row) for row in cur.fetchall()]
    return _without_open_circuits(conn, rows) if skip_open_circuits else rows


def find_stale_agencies(conn, stale_hours: int = 24) -> list[dict]:
    """Find agencies that had successful scrapes but no new article recently.

    Args:
        conn: psycopg2 connection (or mock).
        stale_hours: Hours without new articles to consider stale.

    Returns:
        List of dicts with agency_key, last_success_at (last success with a new article,
        or with saved articles for rows written before articles_new existed).

    NOTE: STALE_AGENCIES_QUERY is duplicated in dags/monitor_scraping_health.py
    (_STALE_AGENCIES_QUERY); tests/unit/test_dag_monitor_health.py checks they match.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(STALE_AGENCIES_QUERY, {"stale_hours": stale_hours})
        return [dict(row) for row in cur.fetchall()]


def find_silent_agencies(
    conn, active_agencies: Iterable[str], silent_hours: int = 3, skip_open_circuits: bool = True
) -> list[dict]:
    """Find active agencies with no scrape_run at all (success or error) in the window.

    Every call to the scraper API records one scrape_run per agency, success or
    error. An active agency without any run means its mapped task in the
    scrape_agencies DAG is not calling the API: a DagRun stuck in 'running' (with
    max_active_runs=1, e.g. pointing to a DagVersion without serialized_dag), a paused
    or missing DAG, or a task that did not run.
    find_consecutive_failures and find_stale_agencies start from scrape_runs, so
    they cannot see this case.

    Args:
        conn: psycopg2 connection (or mock).
        active_agencies: Agency keys expected to run (active in site_urls.yaml).
        silent_hours: Hours without any run to consider the agency silent (default 3:
            the DAG runs every 2 hours, plus 1 hour of margin).
        skip_open_circuits: Tira as agências com o disjuntor aberto, que a API pula sem
            gravar scrape_run (como o monitor).

    Returns:
        List of dicts with agency_key and last_run_at (None when there is no run
        within the scrape_runs retention), sorted by agency_key.
    """
    agencies = list(active_agencies)
    if not agencies:
        return []
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SILENT_AGENCIES_QUERY, {"agencies": agencies, "silent_hours": silent_hours})
        rows = [dict(row) for row in cur.fetchall()]
    return _without_open_circuits(conn, rows) if skip_open_circuits else rows


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
            COALESCE(SUM(COALESCE(articles_new, articles_saved)), 0) AS total_articles,
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
