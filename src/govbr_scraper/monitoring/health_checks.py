"""Health check queries for scraper monitoring.

These functions execute SQL against a provided database connection (psycopg2).
They are used by the monitoring DAG and by tests.
"""

from __future__ import annotations

from psycopg2.extras import RealDictCursor


def find_consecutive_failures(conn, threshold: int = 3) -> list[dict]:
    """Find agencies with N or more consecutive recent failures.

    Args:
        conn: psycopg2 connection (or mock).
        threshold: Number of consecutive failures to trigger alert.

    Returns:
        List of dicts with agency_key, consecutive_failures, last_error, last_failure_at.
    """
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
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, {"threshold": threshold})
        return [dict(row) for row in cur.fetchall()]


def find_stale_agencies(conn, stale_hours: int = 24) -> list[dict]:
    """Find agencies that had successful scrapes but none with articles recently.

    Args:
        conn: psycopg2 connection (or mock).
        stale_hours: Hours without articles to consider stale.

    Returns:
        List of dicts with agency_key, last_success_at.
    """
    query = """
        SELECT
            agency_key,
            MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0) AS last_success_at
        FROM scrape_runs
        GROUP BY agency_key
        HAVING MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0)
               < NOW() - INTERVAL '1 hour' * %(stale_hours)s
           AND MAX(scraped_at) FILTER (WHERE status = 'success' AND articles_saved > 0) IS NOT NULL
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, {"stale_hours": stale_hours})
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
            (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs) AS total_active,
            CASE
                WHEN (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs) > 0
                THEN ROUND(
                    COUNT(DISTINCT agency_key) FILTER (WHERE status = 'success')::numeric /
                    (SELECT COUNT(DISTINCT agency_key) FROM scrape_runs)::numeric,
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
