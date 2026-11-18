"""
Testes de integração das queries de saúde contra um Postgres descartável.

Provam a semântica de find_silent_agencies (agência ativa sem nenhum scrape_run na
janela) num banco real: LEFT JOIN LATERAL, unnest do array de agências e a
comparação de janela com NOW(). Também find_stale_agencies e
compute_coverage_report com COALESCE(articles_new, articles_saved). Mock de
cursor não pega erro de SQL.

Mesma guarda e mesmo Postgres dos testes de re-scrape (tests/integration/
test_postgres_rescrape.py): requer SCRAPER_TEST_POSTGRES_URL apontando para um
Postgres LOCAL e descartável, numa porta diferente de 5432; sem a variável, os
testes são pulados. Cada execução cria um schema próprio (scraper_it_<hex>) só com
a tabela scrape_runs e o remove ao final.
"""

import os
import uuid

import psycopg2
import pytest
from psycopg2.extensions import make_dsn

from govbr_scraper.monitoring.health_checks import (
    compute_coverage_report,
    find_silent_agencies,
    find_stale_agencies,
)
from tests.integration import postgres_guard

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"

# data-platform: scripts/migrations/008_create_scrape_runs.sql + 026 (fallback_triggered)
# + 037 (articles_new).
_DDL = """
CREATE TABLE scrape_runs (
    id SERIAL PRIMARY KEY,
    agency_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'error')),
    error_category VARCHAR(50),
    error_message TEXT,
    articles_scraped INTEGER DEFAULT 0,
    articles_saved INTEGER DEFAULT 0,
    execution_time_seconds REAL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fallback_triggered BOOLEAN DEFAULT FALSE,
    articles_new INTEGER
);
CREATE INDEX idx_scrape_runs_agency_scraped ON scrape_runs (agency_key, scraped_at DESC);
"""


@pytest.fixture(scope="module")
def base_dsn() -> str:
    dsn = os.getenv(ENV_VAR, "").strip()
    if not dsn:
        pytest.skip(f"{ENV_VAR} não definido: requer um Postgres local descartável")
    reason = postgres_guard.unsafe_dsn_reason(dsn)
    if reason:
        pytest.fail(f"{ENV_VAR} deve apontar para um Postgres LOCAL descartável: {reason}")
    return dsn


@pytest.fixture(scope="module")
def db(base_dsn):
    """Conexão (autocommit) num schema isolado com scrape_runs; remove o schema ao final."""
    schema = f"{postgres_guard.TEST_SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(base_dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        reason = postgres_guard.unsafe_database_reason(cur)
    if reason:
        admin.close()
        pytest.fail(f"{ENV_VAR} não aponta para um Postgres descartável: {reason}")
    conn = None
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET search_path TO {schema}")
            cur.execute(_DDL)
        conn = psycopg2.connect(make_dsn(base_dsn, options=f"-c search_path={schema}"))
        conn.autocommit = True
        yield conn
    finally:
        if conn is not None:
            conn.close()
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()


@pytest.fixture(autouse=True)
def _clean_runs(db):
    with db.cursor() as cur:
        cur.execute("TRUNCATE scrape_runs RESTART IDENTITY")
    yield


def _run(db, agency_key: str, hours_ago: float, status: str = "success") -> None:
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (agency_key, status, scraped_at) "
            "VALUES (%s, %s, NOW() - INTERVAL '1 hour' * %s)",
            (agency_key, status, hours_ago),
        )


def _counted_run(db, agency_key: str, hours_ago: float, saved: int, new: int | None) -> None:
    """Execução com sucesso; new=None imita uma linha anterior à migration 037."""
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO scrape_runs (agency_key, status, articles_saved, articles_new, scraped_at) "
            "VALUES (%s, 'success', %s, %s, NOW() - INTERVAL '1 hour' * %s)",
            (agency_key, saved, new, hours_ago),
        )


def _silent(db, agencies, silent_hours=2) -> dict:
    return {
        r["agency_key"]: r["last_run_at"] for r in find_silent_agencies(db, agencies, silent_hours)
    }


class TestFindSilentAgenciesPostgres:
    def test_agency_without_any_run_is_silent(self, db):
        """Caso scrape_inma/scrape_inpe: travada há mais tempo que a retenção de scrape_runs."""
        _run(db, "mec", hours_ago=0.1)
        assert _silent(db, ["inma", "mec"]) == {"inma": None}

    def test_agency_with_only_old_runs_is_silent_with_last_run(self, db):
        """Caso scrape_aids: última execução no dia em que o DagRun travou."""
        _run(db, "aids", hours_ago=24 * 61)
        _run(db, "aids", hours_ago=24 * 62)
        result = _silent(db, ["aids"])
        assert list(result) == ["aids"]
        assert result["aids"] is not None

    def test_recent_error_run_is_not_silent(self, db):
        """Agência que falha (ex.: defeso, 401) grava scrape_run de erro: não é silêncio."""
        _run(db, "coaf", hours_ago=0.2, status="error")
        _run(db, "coaf", hours_ago=30)
        assert _silent(db, ["coaf"]) == {}

    def test_window_boundary(self, db):
        _run(db, "memp", hours_ago=2.5)
        assert list(_silent(db, ["memp"], silent_hours=2)) == ["memp"]
        assert _silent(db, ["memp"], silent_hours=3) == {}

    def test_only_listed_agencies_are_checked(self, db):
        """Agência inativa (fora da lista) não alerta, mesmo sem execução."""
        _run(db, "lapoc", hours_ago=24 * 120)
        assert _silent(db, ["mec"]) == {"mec": None}

    def test_results_are_sorted_by_agency_key(self, db):
        assert list(_silent(db, ["patrimonio", "aids", "inpe"])) == ["aids", "inpe", "patrimonio"]


class TestFindStaleAgenciesPostgres:
    """Agência sem notícia NOVA em stale_hours: articles_new, com fallback para articles_saved
    nas linhas anteriores à migration 037 (articles_new NULL)."""

    def _stale(self, db, stale_hours=24) -> set[str]:
        return {r["agency_key"] for r in find_stale_agencies(db, stale_hours)}

    def test_rescrape_only_does_not_count_as_news(self, db):
        """Caso agencia_brasil: re-scrape atualiza linhas (saved > 0) mas nada é novo."""
        _counted_run(db, "agencia_brasil", hours_ago=30, saved=5, new=1)
        _counted_run(db, "agencia_brasil", hours_ago=2, saved=40, new=0)
        assert self._stale(db) == {"agencia_brasil"}

    def test_recent_new_article_is_not_stale(self, db):
        _counted_run(db, "mec", hours_ago=30, saved=5, new=1)
        _counted_run(db, "mec", hours_ago=2, saved=1, new=1)
        assert self._stale(db) == set()

    def test_rows_before_migration_fall_back_to_articles_saved(self, db):
        _counted_run(db, "mds", hours_ago=2, saved=3, new=None)
        _counted_run(db, "antaq", hours_ago=30, saved=3, new=None)
        _counted_run(db, "antaq", hours_ago=2, saved=0, new=None)
        assert self._stale(db) == {"antaq"}

    def test_agency_without_any_news_in_retention_is_not_listed(self, db):
        """HAVING ... IS NOT NULL: só entra quem já teve notícia (comportamento mantido)."""
        _counted_run(db, "quase_parada", hours_ago=2, saved=4, new=0)
        assert self._stale(db) == set()


class TestComputeCoverageReportPostgres:
    def test_total_articles_counts_new_with_fallback(self, db):
        _counted_run(db, "agencia_brasil", hours_ago=1, saved=40, new=2)
        _counted_run(db, "mec", hours_ago=3, saved=5, new=None)  # antes da 037
        _counted_run(db, "mds", hours_ago=30, saved=9, new=9)  # fora das 24 h
        report = compute_coverage_report(db, hours=24)
        assert report["total_articles"] == 7
        assert report["agencies_scraped"] == 2
