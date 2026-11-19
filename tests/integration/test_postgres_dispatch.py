"""
Despacho por fila (/dispatch, Passos 3 e 4) contra um Postgres descartável.

- A taxa do recálculo diário das faixas é a MESMA do seed da migration 038: a contagem por
  dia BRT de ``daily_news_counts`` + ``ewma_rate`` é comparada com o SQL do seed (copiado
  abaixo, com o ``NOW()`` trocado por um parâmetro).
- ``claim_daily`` reserva o dia de forma atômica: dois ticks concorrentes, só um recalcula.
- ``update_tiers`` só mexe nas colunas da faixa (e encurta o vencimento só com o circuito
  fechado).
- ``unthemed_news`` (reconciliador): sem tema, criadas entre now − 7 d e now − 2 h, até 20.
- O ``Dispatcher`` de ponta a ponta (dry_run e live, com um cliente do Cloud Tasks falso) e a
  rota ``POST /dispatch`` com o schema na DATABASE_URL.

Mesma guarda e mesmo Postgres dos outros testes `postgres` (ver test_postgres_rescrape.py):
requer SCRAPER_TEST_POSTGRES_URL apontando para um Postgres LOCAL e descartável, numa porta
diferente de 5432. Cada schema (scraper_it_<hex>) é removido ao final.
"""

import os
import threading
import uuid
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from fastapi.testclient import TestClient
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor

from govbr_scraper.dispatch import queries
from govbr_scraper.dispatch.config import DispatchMode
from govbr_scraper.dispatch.service import (
    RECONCILED_KEY,
    TIERS_RECOMPUTED_KEY,
    Dispatcher,
)
from govbr_scraper.dispatch.tasks_client import CREATED, DUPLICATE, CreateResult
from govbr_scraper.dispatch.tiers import TierUpdate, ewma_rate
from govbr_scraper.state import repository
from govbr_scraper.state.model import Tier
from govbr_scraper.state.policy import BRT
from tests.integration import postgres_guard
from tests.integration.migration_038 import MIGRATION_038

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"

# Só as colunas de news que o /dispatch lê (data-platform: create_schema.sql).
_DDL_NEWS = """
CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    unique_id VARCHAR(120) UNIQUE NOT NULL,
    agency_key VARCHAR(100),
    most_specific_theme_id INTEGER,
    title TEXT NOT NULL DEFAULT 't',
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX idx_news_published_at ON news (published_at DESC);
"""

# Seed da 038 (data-platform, scripts/migrations/038_create_scrape_agency_state.sql): os CTEs
# da taxa, com NOW() trocado por %(now)s. Manter igual à migration.
_SEED_038_RATE_SQL = """
WITH janela AS (
    SELECT (%(now)s::timestamptz AT TIME ZONE 'America/Sao_Paulo')::date AS hoje
),
pesos AS (
    SELECT SUM(POWER(0.5::float8, idade / 7.0::float8)) AS total
    FROM generate_series(0, 27) AS idade
),
noticias_por_dia AS (
    SELECT n.agency_key,
           (n.published_at AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
           COUNT(*) AS qtd
    FROM news AS n
    WHERE n.published_at >= ((%(now)s::timestamptz AT TIME ZONE 'America/Sao_Paulo')::date - 28)::timestamp
                            AT TIME ZONE 'America/Sao_Paulo'
      AND n.published_at <  ((%(now)s::timestamptz AT TIME ZONE 'America/Sao_Paulo')::date)::timestamp
                            AT TIME ZONE 'America/Sao_Paulo'
      AND n.agency_key IS NOT NULL
    GROUP BY n.agency_key, dia
),
taxa AS (
    SELECT d.agency_key,
           SUM(d.qtd * POWER(0.5::float8, (j.hoje - 1 - d.dia) / 7.0::float8)) / p.total
               AS por_dia
    FROM noticias_por_dia AS d
    CROSS JOIN janela AS j
    CROSS JOIN pesos AS p
    GROUP BY d.agency_key, p.total
)
SELECT agency_key, por_dia FROM taxa
"""

NOW = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)  # 10:00 BRT
TODAY = date(2026, 11, 19)


@pytest.fixture(scope="module")
def base_dsn() -> str:
    dsn = os.getenv(ENV_VAR, "").strip()
    if not dsn:
        pytest.skip(f"{ENV_VAR} não definido: requer um Postgres local descartável")
    reason = postgres_guard.unsafe_dsn_reason(dsn)
    if reason:
        pytest.fail(f"{ENV_VAR} deve apontar para um Postgres LOCAL descartável: {reason}")
    return dsn


@pytest.fixture
def dsn(base_dsn):
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
        cur.execute(_DDL_NEWS)
        cur.execute(MIGRATION_038)
    try:
        yield make_dsn(base_dsn, options=f"-c search_path={schema}")
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()


@pytest.fixture
def conn(dsn):
    conn = psycopg2.connect(dsn)
    yield conn
    conn.rollback()
    conn.close()


def _query(dsn: str, sql: str, params=None) -> list[dict]:
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _insert_news(conn, rows):
    """rows: (agency_key, published_at, created_at, theme_id)."""
    with conn.cursor() as cur:
        for i, (agency, published, created, theme) in enumerate(rows):
            cur.execute(
                "INSERT INTO news (unique_id, agency_key, published_at, created_at, "
                "most_specific_theme_id) VALUES (%s, %s, %s, %s, %s)",
                (f"n{i}_{uuid.uuid4().hex[:6]}", agency, published, created or published, theme),
            )
    conn.commit()


def _brt(day: date, hour=12, minute=0):
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=BRT)


class TestRateMatchesSeed038:
    def test_daily_counts_and_ewma_equal_the_038_seed(self, conn):
        rows = []
        # mec: 3 por dia nos 28 dias; mds: rajada ontem e há 20 dias; pf: só fora da janela.
        for age in range(28):
            day = TODAY - timedelta(days=age + 1)
            rows += [("mec", _brt(day, 9 + h), None, 1) for h in range(3)]
        rows += [("mds", _brt(TODAY - timedelta(days=1), 10), None, 1)] * 1
        rows += [("mds", _brt(TODAY - timedelta(days=21), 10), None, 1) for _ in range(9)]
        rows += [("pf", _brt(TODAY, 8), None, 1), ("pf", _brt(TODAY - timedelta(days=29)), None, 1)]
        # Bordas do dia BRT: 23:59 de ontem conta como ontem; 00:00 de hoje fica fora.
        rows += [
            ("secom", _brt(TODAY - timedelta(days=1), 23, 59), None, 1),
            ("secom", _brt(TODAY, 0, 0), None, 1),
            ("secom", _brt(TODAY - timedelta(days=28), 0, 0), None, 1),  # início da janela
            (None, _brt(TODAY - timedelta(days=2)), None, 1),  # sem agency_key: fora
        ]
        _insert_news(conn, rows)

        counts = queries.daily_news_counts(conn, TODAY)
        assert counts["secom"] == {
            TODAY - timedelta(days=1): 1,
            TODAY - timedelta(days=28): 1,
        }
        assert "pf" not in counts
        python_rates = {k: ewma_rate(v, TODAY) for k, v in counts.items()}

        with conn.cursor() as cur:
            cur.execute(_SEED_038_RATE_SQL, {"now": NOW})
            seed_rates = dict(cur.fetchall())
        assert set(python_rates) == set(seed_rates) == {"mec", "mds", "secom"}
        for key, rate in seed_rates.items():
            assert python_rates[key] == pytest.approx(rate, rel=1e-12), key
        assert python_rates["mec"] == pytest.approx(3.0)


class TestClaimDaily:
    def test_once_per_day(self, conn):
        assert repository.claim_daily(conn, TIERS_RECOMPUTED_KEY, TODAY, NOW) is True
        conn.commit()
        assert repository.claim_daily(conn, TIERS_RECOMPUTED_KEY, TODAY, NOW) is False
        conn.commit()
        assert repository.claim_daily(conn, RECONCILED_KEY, TODAY, NOW) is True  # outra chave
        tomorrow = TODAY + timedelta(days=1)
        assert repository.claim_daily(conn, TIERS_RECOMPUTED_KEY, tomorrow, NOW) is True
        conn.commit()
        value = repository.get_meta(conn, TIERS_RECOMPUTED_KEY)
        assert value["date"] == "2026-11-20"
        assert value["claimed_at"] == NOW.isoformat()

    def test_rollback_releases_the_claim(self, conn):
        assert repository.claim_daily(conn, TIERS_RECOMPUTED_KEY, TODAY, NOW) is True
        conn.rollback()
        assert repository.claim_daily(conn, TIERS_RECOMPUTED_KEY, TODAY, NOW) is True

    @pytest.mark.parametrize("first_commits", [True, False], ids=["commit", "rollback"])
    @pytest.mark.parametrize("row_exists", [False, True], ids=["primeira-vez", "dia-seguinte"])
    def test_concurrent_ticks_only_one_wins(self, dsn, first_commits, row_exists):
        if row_exists:
            setup = psycopg2.connect(dsn)
            repository.claim_daily(setup, TIERS_RECOMPUTED_KEY, TODAY - timedelta(days=1), NOW)
            setup.commit()
            setup.close()
        first, second = psycopg2.connect(dsn), psycopg2.connect(dsn)
        try:
            assert repository.claim_daily(first, TIERS_RECOMPUTED_KEY, TODAY, NOW) is True
            result: dict = {}

            def other_tick():
                result["claimed"] = repository.claim_daily(second, TIERS_RECOMPUTED_KEY, TODAY, NOW)
                second.commit()

            thread = threading.Thread(target=other_tick)
            thread.start()
            thread.join(timeout=0.5)
            assert thread.is_alive(), "o segundo tick deveria esperar o lock do primeiro"
            if first_commits:
                first.commit()
            else:
                first.rollback()
            thread.join(timeout=10)
            assert not thread.is_alive()
            assert result["claimed"] is (not first_commits)
        finally:
            first.close()
            second.close()


class TestUpdateTiers:
    def test_only_tier_columns_and_capped_due(self, conn):
        repository.ensure_rows(conn, ["mec", "mds", "antaq"], NOW - timedelta(days=1))
        far = NOW + timedelta(hours=20)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_agency_state SET next_due_at = %s, last_error_category = 'x', "
                "consecutive_transient_failures = 3",
                (far,),
            )
            cur.execute(
                "UPDATE scrape_agency_state SET circuit_state = 'open' WHERE agency_key = 'antaq'"
            )
        cap = NOW + timedelta(minutes=25)
        updates = [
            TierUpdate("mec", Tier.H2, Tier.M30, 30, 7.5, next_due_cap=cap),
            TierUpdate("mds", Tier.H2, Tier.D1, 1440, 0.01, next_due_cap=None),
            TierUpdate("antaq", Tier.H2, Tier.M30, 30, 9.0, next_due_cap=cap),
            TierUpdate("sumiu", Tier.H2, Tier.H6, 360, 0.5),  # sem linha: ignorada
        ]
        assert repository.update_tiers(conn, updates, NOW) == 3
        assert repository.update_tiers(conn, [], NOW) == 0
        conn.commit()
        states = {s.agency_key: s for s in repository.list_states(conn)}
        mec = states["mec"]
        assert (mec.tier, mec.poll_interval_minutes, mec.rate_ewma_per_day) == (Tier.M30, 30, 7.5)
        assert mec.tier_updated_at == NOW
        assert mec.next_due_at == cap
        assert mec.last_error_category == "x"  # o resto da linha fica
        assert mec.consecutive_transient_failures == 3
        assert states["mds"].next_due_at == far
        assert states["antaq"].next_due_at == far  # circuito aberto: a sonda fica
        assert states["antaq"].tier == Tier.M30

    def test_cap_never_postpones(self, conn):
        repository.ensure_rows(conn, ["mec"], NOW)
        later = NOW + timedelta(hours=1)
        repository.update_tiers(conn, [TierUpdate("mec", Tier.H2, Tier.M30, 30, 6.0, later)], NOW)
        assert repository.list_states(conn)[0].next_due_at == NOW


class TestUnthemedNews:
    def test_window_theme_limit_and_order(self, conn):
        rows = [
            ("mec", NOW - timedelta(days=1), NOW - timedelta(hours=3), None),  # entra
            ("mds", NOW - timedelta(days=2), NOW - timedelta(days=2), None),  # entra
            ("mme", NOW - timedelta(hours=1), NOW - timedelta(hours=1), None),  # recente demais
            ("pf", NOW - timedelta(days=8), NOW - timedelta(days=8), None),  # velha demais
            ("secom", NOW - timedelta(days=1), NOW - timedelta(hours=5), 7),  # tem tema
            (None, NOW - timedelta(days=1), NOW - timedelta(hours=6), None),  # sem agência
        ]
        _insert_news(conn, rows)
        articles = queries.unthemed_news(conn, NOW)
        assert [a["agency_key"] for a in articles] == ["mec", "", "mds"]
        assert set(articles[0]) == {"unique_id", "agency_key", "published_at"}
        assert articles[0]["published_at"] == NOW - timedelta(days=1)

    def test_limit_of_20(self, conn):
        created = NOW - timedelta(hours=3)
        _insert_news(conn, [("mec", created, created, None)] * 25)
        assert len(queries.unthemed_news(conn, NOW)) == 20


class FakeTasks:
    def __init__(self, statuses=None):
        self.statuses = statuses or {}
        self.calls = []

    def create_task(self, task_id, body):
        self.calls.append((task_id, body))
        return CreateResult(self.statuses.get(body["agency"], CREATED), task_id)


def _dispatch(dsn, mode, now, keys, tasks=None, publisher=None):
    conn = psycopg2.connect(dsn)
    try:
        factory = (lambda: publisher) if publisher is not None else MagicMock()
        dispatcher = Dispatcher(
            conn, mode=mode, now=now, active_keys=keys, tasks=tasks, publisher_factory=factory
        )
        return dispatcher.tick()
    finally:
        conn.close()


class TestDispatcherEndToEnd:
    @pytest.fixture(autouse=True)
    def _topic(self, monkeypatch):
        monkeypatch.setenv("PUBSUB_TOPIC_NEWS_SCRAPED", "projects/p/topics/dgb.news.scraped")

    def test_dry_run_recomputes_tiers_once_and_dispatches_nothing(self, dsn, conn):
        rows = []
        for age in range(28):
            day = TODAY - timedelta(days=age + 1)
            rows += [("agencia_brasil", _brt(day, 9 + h), None, 1) for h in range(8)]
        rows += [("mec", _brt(TODAY - timedelta(days=1), h), None, 1) for h in (10, 11)]
        _insert_news(conn, rows)
        keys = ["agencia_brasil", "mec", "mds"]

        out = _dispatch(dsn, DispatchMode.DRY_RUN, NOW, keys)
        assert out.tiers_recomputed is True
        assert out.due == 3  # linhas novas vencem agora
        assert out.dispatched == 0
        states = {s["agency_key"]: s for s in _query(dsn, "SELECT * FROM scrape_agency_state")}
        assert states["agencia_brasil"]["tier"] == "m30"
        assert states["agencia_brasil"]["poll_interval_minutes"] == 30
        assert states["agencia_brasil"]["rate_ewma_per_day"] == pytest.approx(8.0)
        assert states["mds"]["tier"] == "d1"  # sem notícia: taxa 0
        assert states["mec"]["tier"] == "h6"  # 2 ontem: 2 / 9,94 ≈ 0,201/dia
        assert all(s["last_dispatched_at"] is None for s in states.values())
        (meta,) = _query(dsn, "SELECT key, value FROM scrape_dispatch_meta")
        assert (meta["key"], meta["value"]["date"]) == (TIERS_RECOMPUTED_KEY, "2026-11-19")

        again = _dispatch(dsn, DispatchMode.DRY_RUN, NOW + timedelta(minutes=15), keys)
        assert again.tiers_recomputed is False

    def test_live_marks_dispatched_and_next_tick_sees_in_flight(self, dsn):
        keys = ["mec", "mds", "mme"]
        tasks = FakeTasks({"mme": "failed", "mds": DUPLICATE})
        out = _dispatch(dsn, DispatchMode.LIVE, NOW, keys, tasks=tasks)
        assert (out.dispatched, out.duplicates, out.failed) == (1, 1, 1)
        rows = {r["agency_key"]: r for r in _query(dsn, "SELECT * FROM scrape_agency_state")}
        assert rows["mec"]["last_dispatched_at"] == NOW
        assert rows["mds"]["last_dispatched_at"] == NOW
        assert rows["mme"]["last_dispatched_at"] is None

        # 15 min depois, sem tentativa ainda: mec e mds em voo; mme (falhou) vai de novo.
        later = NOW + timedelta(minutes=15)
        tasks = FakeTasks()
        out = _dispatch(dsn, DispatchMode.LIVE, later, keys, tasks=tasks)
        assert out.skipped_inflight == 2
        assert [c[1]["agency"] for c in tasks.calls] == ["mme"]
        assert tasks.calls[0][0] == "scrape-mme-202611191015"

    def test_live_reconciles_unthemed_news_once(self, dsn, conn):
        created = NOW - timedelta(hours=3)
        _insert_news(conn, [("mec", created, created, None), ("mds", created, created, 3)])
        publisher = MagicMock()
        publisher.publish_scraped.return_value = 1
        out = _dispatch(
            dsn, DispatchMode.LIVE, NOW, ["mec"], tasks=FakeTasks(), publisher=publisher
        )
        assert out.reconciled == 1
        (articles,) = publisher.publish_scraped.call_args.args
        assert [a["agency_key"] for a in articles] == ["mec"]
        publisher.close.assert_called_once()
        again = _dispatch(
            dsn,
            DispatchMode.LIVE,
            NOW + timedelta(minutes=15),
            ["mec"],
            tasks=FakeTasks(),
            publisher=publisher,
        )
        assert again.reconciled == 0
        meta = {r["key"] for r in _query(dsn, "SELECT key FROM scrape_dispatch_meta")}
        assert meta == {TIERS_RECOMPUTED_KEY, RECONCILED_KEY}

    def test_backfill_respects_circuit_and_does_not_mark(self, dsn, conn):
        repository.ensure_rows(conn, ["mec", "antaq"], NOW)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE scrape_agency_state SET circuit_state = 'open', next_due_at = %s "
                "WHERE agency_key = 'antaq'",
                (NOW + timedelta(hours=3),),
            )
        conn.commit()
        tasks = FakeTasks()
        c = psycopg2.connect(dsn)
        try:
            out = Dispatcher(
                c, mode=DispatchMode.LIVE, now=NOW, active_keys=["mec", "antaq"], tasks=tasks
            ).backfill(agencies=None, start_date=date(2026, 11, 1), end_date=date(2026, 11, 2))
        finally:
            c.close()
        assert out.skipped_circuit == ["antaq"]
        assert [b["agency"] for _, b in tasks.calls] == ["mec"]
        rows = _query(dsn, "SELECT last_dispatched_at FROM scrape_agency_state")
        assert all(r["last_dispatched_at"] is None for r in rows)


class TestDispatchRoute:
    def test_dry_run_tick_through_the_api(self, dsn, monkeypatch):
        from govbr_scraper.api import app

        monkeypatch.setenv("DATABASE_URL", dsn)
        monkeypatch.setenv("SCRAPE_DISPATCH_MODE", "dry_run")
        log = MagicMock()
        with patch("govbr_scraper.api.log_dispatch_run", log):
            response = TestClient(app).post("/dispatch", json={"reason": "tick"})
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["mode"] == "dry_run"
        # Todas as ativas (gov.br + EBC) ganham linha e vencem agora, até o teto de 200.
        rows = _query(dsn, "SELECT agency_key FROM scrape_agency_state")
        assert {"mec", "agencia_brasil", "tvbrasil"} <= {r["agency_key"] for r in rows}
        assert body["due"] == min(len(rows), 200)
        assert log.call_args.kwargs["mode"] == "dry_run"

    def test_without_038_is_503(self, base_dsn, monkeypatch):
        from govbr_scraper.api import app

        schema = f"{postgres_guard.TEST_SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}"
        admin = psycopg2.connect(base_dsn)
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
        try:
            monkeypatch.setenv(
                "DATABASE_URL", make_dsn(base_dsn, options=f"-c search_path={schema}")
            )
            monkeypatch.setenv("SCRAPE_DISPATCH_MODE", "dry_run")
            response = TestClient(app).post("/dispatch", json={"reason": "tick"})
        finally:
            with admin.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            admin.close()
        assert response.status_code == 503
        assert response.json() == {"detail": "scrape_agency_state indisponível"}
