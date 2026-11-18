"""
PostgresManager.record_scrape_run contra um Postgres descartável, antes e depois da
migration 037 do data-platform (``ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS
articles_new INTEGER``).

O scraper pode subir antes da migration: o INSERT com articles_new falha com
UndefinedColumn e é refeito sem a coluna. Sem isso nenhuma linha de scrape_runs seria
gravada e o monitor acusaria "Agências Sem Execução" em todas as agências.

Mesma guarda e mesmo Postgres dos outros testes `postgres` (ver
test_postgres_rescrape.py): requer SCRAPER_TEST_POSTGRES_URL apontando para um
Postgres LOCAL e descartável, numa porta diferente de 5432. Cada schema
(scraper_it_<hex>) só tem a tabela scrape_runs e é removido ao final.
"""

import os
import uuid
from datetime import UTC, datetime

import psycopg2
import pytest
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult
from govbr_scraper.storage.postgres_manager import PostgresManager
from tests.integration import postgres_guard

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"

# data-platform: scripts/migrations/008_create_scrape_runs.sql + 026 (fallback_triggered).
_DDL_BEFORE_037 = """
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
    fallback_triggered BOOLEAN DEFAULT FALSE
);
"""

# data-platform: migration 037.
_MIGRATION_037 = "ALTER TABLE scrape_runs ADD COLUMN IF NOT EXISTS articles_new INTEGER;"


@pytest.fixture(scope="module")
def base_dsn() -> str:
    dsn = os.getenv(ENV_VAR, "").strip()
    if not dsn:
        pytest.skip(f"{ENV_VAR} não definido: requer um Postgres local descartável")
    reason = postgres_guard.unsafe_dsn_reason(dsn)
    if reason:
        pytest.fail(f"{ENV_VAR} deve apontar para um Postgres LOCAL descartável: {reason}")
    return dsn


def _make_schema(base_dsn: str, with_037: bool):
    """Cria um schema isolado com scrape_runs (com ou sem a 037); devolve (dsn, drop)."""
    schema = f"{postgres_guard.TEST_SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(base_dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        reason = postgres_guard.unsafe_database_reason(cur)
    if reason:
        admin.close()
        pytest.fail(f"{ENV_VAR} não aponta para um Postgres descartável: {reason}")
    with admin.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
        cur.execute(_DDL_BEFORE_037)
        if with_037:
            cur.execute(_MIGRATION_037)

    def drop():
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()

    return make_dsn(base_dsn, options=f"-c search_path={schema}"), drop


@pytest.fixture(params=[False, True], ids=["antes-da-037", "depois-da-037"])
def schema(request, base_dsn):
    dsn, drop = _make_schema(base_dsn, with_037=request.param)
    try:
        yield dsn, request.param
    finally:
        drop()


@pytest.fixture
def pg(schema):
    dsn, _ = schema
    manager = PostgresManager(connection_string=dsn, min_connections=1, max_connections=2)
    yield manager
    manager.close_all()


def _rows(dsn: str) -> list[dict]:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM scrape_runs ORDER BY id")
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _run(**overrides) -> ScrapeRunResult:
    data = {
        "agency_key": "mec",
        "status": "success",
        "articles_scraped": 12,
        "articles_saved": 12,
        "articles_new": 2,
        "execution_time_seconds": 3.5,
        "scraped_at": datetime(2026, 11, 19, 12, 0, tzinfo=UTC),
    }
    data.update(overrides)
    return ScrapeRunResult(**data)


class TestRecordScrapeRun:
    def test_success_row_is_always_recorded(self, pg, schema):
        dsn, with_037 = schema

        pg.record_scrape_run(_run())

        (row,) = _rows(dsn)
        assert row["agency_key"] == "mec"
        assert row["status"] == "success"
        assert row["articles_saved"] == 12
        if with_037:
            assert row["articles_new"] == 2
        else:
            assert "articles_new" not in row

    def test_error_row_is_always_recorded(self, pg, schema):
        dsn, with_037 = schema

        pg.record_scrape_run(
            _run(
                status="error",
                error_category=ErrorCategory.HTML_CHANGED,
                error_message="No articles found on first page of mec",
                articles_scraped=0,
                articles_saved=0,
                articles_new=None,
            )
        )

        (row,) = _rows(dsn)
        assert row["error_category"] == "html_changed"
        if with_037:
            assert row["articles_new"] is None

    def test_connection_stays_usable_after_fallback(self, pg, schema):
        """O rollback do INSERT abortado devolve uma conexão limpa ao pool."""
        dsn, _ = schema

        for i in range(3):
            pg.record_scrape_run(_run(agency_key=f"agencia_{i}"))

        assert [r["agency_key"] for r in _rows(dsn)] == ["agencia_0", "agencia_1", "agencia_2"]
