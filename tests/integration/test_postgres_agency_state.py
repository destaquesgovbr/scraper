"""
Estado por agência (scrape_agency_state) contra um Postgres descartável, com e sem a
migration 038 do data-platform.

- record_scrape_run grava scrape_runs e, na mesma transação, o estado da agência num
  SAVEPOINT. Sem a 038 (scraper no ar antes da migration) só o savepoint é desfeito: a linha
  de scrape_runs não pode se perder, senão o monitor acusaria "Agências Sem Execução".
- O SQL do repository (ensure_rows, get_state FOR UPDATE, upsert, mark_probe,
  mark_dispatched, list_states) e as CHECKs da tabela.

Mesma guarda e mesmo Postgres dos outros testes `postgres` (ver test_postgres_rescrape.py):
requer SCRAPER_TEST_POSTGRES_URL apontando para um Postgres LOCAL e descartável, numa porta
diferente de 5432. Cada schema (scraper_it_<hex>) é removido ao final.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import psycopg2
import pytest
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult
from govbr_scraper.state import repository
from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.repository import StateUnavailable
from govbr_scraper.storage.postgres_manager import PostgresManager
from tests.integration import postgres_guard

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"

# data-platform: scripts/migrations/008_create_scrape_runs.sql + 026 + 037.
_DDL_SCRAPE_RUNS = """
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
"""

# data-platform: scripts/migrations/038_create_scrape_agency_state.sql, sem o seed e sem os
# COMMENT ON. Manter igual à migration (cópia manual, como as 008/026/037 acima).
_MIGRATION_038 = """
CREATE TABLE IF NOT EXISTS scrape_agency_state (
    agency_key                      TEXT PRIMARY KEY,
    tier                            TEXT NOT NULL DEFAULT 'h2'
                                    CHECK (tier IN ('m30', 'h2', 'h6', 'd1')),
    poll_interval_minutes           INTEGER NOT NULL DEFAULT 120 CHECK (poll_interval_minutes > 0),
    rate_ewma_per_day               DOUBLE PRECISION,
    tier_updated_at                 TIMESTAMPTZ,
    next_due_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    boost_until                     TIMESTAMPTZ,
    last_dispatched_at              TIMESTAMPTZ,
    last_attempt_at                 TIMESTAMPTZ,
    last_success_at                 TIMESTAMPTZ,
    last_new_article_at             TIMESTAMPTZ,
    consecutive_permanent_failures  INTEGER NOT NULL DEFAULT 0,
    consecutive_transient_failures  INTEGER NOT NULL DEFAULT 0,
    failure_streak_started_at       TIMESTAMPTZ,
    circuit_state                   TEXT NOT NULL DEFAULT 'closed'
                                    CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    circuit_opened_at               TIMESTAMPTZ,
    circuit_failed_probes           INTEGER NOT NULL DEFAULT 0,
    last_error_category             TEXT,
    last_error_message              TEXT,
    created_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_scrape_agency_state_next_due ON scrape_agency_state (next_due_at);

CREATE TABLE IF NOT EXISTS scrape_dispatch_meta (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

T0 = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def base_dsn() -> str:
    dsn = os.getenv(ENV_VAR, "").strip()
    if not dsn:
        pytest.skip(f"{ENV_VAR} não definido: requer um Postgres local descartável")
    reason = postgres_guard.unsafe_dsn_reason(dsn)
    if reason:
        pytest.fail(f"{ENV_VAR} deve apontar para um Postgres LOCAL descartável: {reason}")
    return dsn


def _make_schema(base_dsn: str, with_038: bool):
    """Schema isolado com scrape_runs (e a 038, se pedida); devolve (dsn, drop)."""
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
        cur.execute(_DDL_SCRAPE_RUNS)
        if with_038:
            cur.execute(_MIGRATION_038)

    def drop():
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()

    return make_dsn(base_dsn, options=f"-c search_path={schema}"), drop


@pytest.fixture(params=[False, True], ids=["sem-038", "com-038"])
def schema(request, base_dsn):
    dsn, drop = _make_schema(base_dsn, with_038=request.param)
    try:
        yield dsn, request.param
    finally:
        drop()


@pytest.fixture
def dsn_038(base_dsn):
    dsn, drop = _make_schema(base_dsn, with_038=True)
    try:
        yield dsn
    finally:
        drop()


@pytest.fixture(autouse=True)
def _no_real_alerts(monkeypatch):
    monkeypatch.delenv("PUBSUB_TOPIC_OPS_ALERTS", raising=False)
    monkeypatch.setattr(PostgresManager, "_warned_missing_agency_state", False)


def _pg(dsn: str) -> PostgresManager:
    return PostgresManager(connection_string=dsn, min_connections=1, max_connections=2)


def _query(dsn: str, sql: str) -> list[dict]:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _run(status="success", hours=0.0, **overrides) -> ScrapeRunResult:
    data = {
        "agency_key": "mec",
        "status": status,
        "articles_scraped": 3,
        "articles_saved": 3,
        "articles_new": 1 if status == "success" else None,
        "execution_time_seconds": 4.0,
        "scraped_at": T0 + timedelta(hours=hours),
    }
    if status == "error":
        data.update(
            error_category=ErrorCategory.HTML_CHANGED,
            error_message="No articles found on first page of mec",
            articles_scraped=0,
            articles_saved=0,
        )
    data.update(overrides)
    return ScrapeRunResult(**data)


class TestRecordScrapeRunWithAndWithout038:
    def test_scrape_run_is_always_recorded(self, schema):
        dsn, with_038 = schema
        pg = _pg(dsn)
        try:
            pg.record_scrape_run(_run())
            pg.record_scrape_run(_run("error", hours=1))
        finally:
            pg.close_all()

        runs = _query(dsn, "SELECT agency_key, status FROM scrape_runs ORDER BY id")
        assert [(r["agency_key"], r["status"]) for r in runs] == [
            ("mec", "success"),
            ("mec", "error"),
        ]
        if with_038:
            (state,) = _query(dsn, "SELECT * FROM scrape_agency_state")
            assert state["consecutive_permanent_failures"] == 1
            assert state["last_success_at"] == T0 - timedelta(seconds=4)

    def test_connection_stays_usable_after_savepoint_rollback(self, schema):
        dsn, _ = schema
        pg = _pg(dsn)
        try:
            for i in range(4):
                pg.record_scrape_run(_run(agency_key=f"agencia_{i}"))
        finally:
            pg.close_all()
        assert len(_query(dsn, "SELECT id FROM scrape_runs")) == 4

    def test_list_agency_states(self, schema):
        dsn, with_038 = schema
        pg = _pg(dsn)
        try:
            pg.record_scrape_run(_run())
            if with_038:
                assert [s.agency_key for s in pg.list_agency_states()] == ["mec"]
            else:
                with pytest.raises(StateUnavailable):
                    pg.list_agency_states()
                # A conexão volta limpa ao pool depois do erro.
                pg.record_scrape_run(_run(hours=1))
        finally:
            pg.close_all()


class TestCircuitLifecycle:
    def test_two_permanent_errors_open_then_probe_success_closes(self, dsn_038):
        pg = _pg(dsn_038)
        try:
            with patch("govbr_scraper.state.alerts.publish_ops_alert") as publish:
                assert pg.record_scrape_run(_run("error")) is None
                opened = pg.record_scrape_run(_run("error", hours=2))
                state = pg.list_agency_states()[0]
                assert state.circuit_state == CircuitState.OPEN
                assert state.next_due_at == T0 + timedelta(hours=4)
                assert pg.mark_agency_probe("mec", T0 + timedelta(hours=4)) is True
                assert pg.list_agency_states()[0].circuit_state == CircuitState.HALF_OPEN
                closed = pg.record_scrape_run(_run(hours=4))
        finally:
            pg.close_all()

        assert opened.reason == "permanent_errors"
        assert closed.reason == "probe_success"
        texts = [c.kwargs["text"] for c in publish.call_args_list]
        assert len(texts) == 2
        assert texts[0].startswith("🔴") and texts[1].startswith("🟢")
        (state,) = _query(dsn_038, "SELECT * FROM scrape_agency_state")
        assert state["circuit_state"] == "closed"
        assert state["consecutive_permanent_failures"] == 0
        assert state["circuit_opened_at"] is None
        assert state["boost_until"] == T0 + timedelta(hours=10)

    def test_explicit_window_does_not_move_the_watermark(self, dsn_038):
        pg = _pg(dsn_038)
        try:
            pg.record_scrape_run(_run())
            before = pg.list_agency_states()[0]
            pg.record_scrape_run(_run(hours=3), explicit_window=True)
            pg.record_scrape_run(_run("error", hours=4), explicit_window=True)
            after = pg.list_agency_states()[0]
        finally:
            pg.close_all()
        assert after.last_success_at == before.last_success_at
        assert after.consecutive_permanent_failures == 0
        assert after.next_due_at == before.next_due_at
        assert len(_query(dsn_038, "SELECT id FROM scrape_runs")) == 3


class TestRepositorySql:
    @pytest.fixture
    def conn(self, dsn_038):
        conn = psycopg2.connect(dsn_038)
        yield conn
        conn.rollback()
        conn.close()

    def test_ensure_rows_is_idempotent_and_uses_defaults(self, conn):
        assert repository.ensure_rows(conn, ["mec", "ansn"], T0) == 2
        assert repository.ensure_rows(conn, ["mec", "ansn", "mds"], T0) == 1
        states = {s.agency_key: s for s in repository.list_states(conn)}
        assert sorted(states) == ["ansn", "mds", "mec"]
        assert states["mec"].tier == Tier.H2
        assert states["mec"].poll_interval_minutes == 120
        assert states["mec"].next_due_at == T0
        assert states["mec"].circuit_state == CircuitState.CLOSED

    def test_upsert_roundtrip_keeps_created_at(self, conn):
        repository.ensure_rows(conn, ["mec"], T0)
        state = repository.get_state(conn, "mec", for_update=True)
        changed = AgencyState(
            **{
                **state.__dict__,
                "tier": Tier.M30,
                "poll_interval_minutes": 30,
                "rate_ewma_per_day": 7.5,
                "circuit_state": CircuitState.OPEN,
                "last_error_message": "x" * 500,
                "created_at": T0 + timedelta(days=9),
                "updated_at": T0 + timedelta(hours=1),
            }
        )
        repository.upsert_state(conn, changed)
        stored = repository.get_state(conn, "mec")
        assert stored.tier == Tier.M30
        assert stored.rate_ewma_per_day == 7.5
        assert stored.circuit_state == CircuitState.OPEN
        assert stored.created_at == T0  # preservado
        assert stored.updated_at == T0 + timedelta(hours=1)

    def test_upsert_inserts_new_row(self, conn):
        repository.upsert_state(conn, AgencyState.new("tvbrasil", T0))
        assert repository.get_state(conn, "tvbrasil").created_at == T0

    def test_mark_probe_only_touches_open_circuits(self, conn):
        repository.ensure_rows(conn, ["mec", "mds"], T0)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_agency_state SET circuit_state = 'open' WHERE agency_key = 'mec'"
            )
        assert repository.mark_probe(conn, "mec", T0) is True
        assert repository.mark_probe(conn, "mds", T0) is False
        assert repository.get_state(conn, "mec").circuit_state == CircuitState.HALF_OPEN
        assert repository.get_state(conn, "mds").circuit_state == CircuitState.CLOSED

    def test_mark_dispatched(self, conn):
        repository.ensure_rows(conn, ["mec", "mds"], T0)
        assert repository.mark_dispatched(conn, ["mec", "nao_existe"], T0) == 1
        assert repository.get_state(conn, "mec").last_dispatched_at == T0
        assert repository.get_state(conn, "mds").last_dispatched_at is None

    def test_check_constraints(self, conn):
        repository.ensure_rows(conn, ["mec"], T0)
        with pytest.raises(psycopg2.errors.CheckViolation):
            with conn.cursor() as cur:
                cur.execute("UPDATE scrape_agency_state SET circuit_state = 'aberto'")

    def test_missing_table_is_state_unavailable(self, base_dsn):
        dsn, drop = _make_schema(base_dsn, with_038=False)
        conn = psycopg2.connect(dsn)
        try:
            for call in (
                lambda: repository.list_states(conn),
                lambda: repository.get_state(conn, "mec"),
                lambda: repository.ensure_rows(conn, ["mec"], T0),
            ):
                with pytest.raises(StateUnavailable):
                    call()
                conn.rollback()
        finally:
            conn.close()
            drop()
