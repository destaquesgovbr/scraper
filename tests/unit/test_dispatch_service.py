"""Fluxo do /dispatch (Dispatcher) com o repositório e as consultas em memória.

O SQL de verdade é coberto em tests/integration/test_postgres_dispatch.py.
"""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from govbr_scraper.dispatch import service as service_module
from govbr_scraper.dispatch.config import DispatchMode
from govbr_scraper.dispatch.service import (
    MAX_CONSECUTIVE_CREATE_FAILURES,
    RECONCILED_KEY,
    TIERS_RECOMPUTED_KEY,
    Dispatcher,
    DispatchFailed,
)
from govbr_scraper.dispatch.tasks_client import CREATED, DUPLICATE, FAILED, CreateResult
from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import BRT
from govbr_scraper.state.repository import StateUnavailable


def _brt(hour, minute=0, day=19):
    return datetime(2026, 11, day, hour, minute, tzinfo=BRT).astimezone(UTC)


DAY = _brt(10)  # 10:00 BRT


class FakeRepo:
    """scrape_agency_state e scrape_dispatch_meta em memória (mesma API do repository)."""

    def __init__(self, states=(), unavailable=False):
        self.states = {s.agency_key: s for s in states}
        self.meta: dict[str, str] = {}
        self.unavailable = unavailable
        self.dispatched: list[tuple[list[str], datetime]] = []
        self.tier_updates: list = []
        self.fail_mark = False

    def _check(self):
        if self.unavailable:
            raise StateUnavailable("scrape_agency_state indisponível")

    def ensure_rows(self, conn, keys, now):
        self._check()
        for k in keys:
            self.states.setdefault(k, AgencyState.new(k, now))
        return 0

    def list_states(self, conn):
        self._check()
        return [self.states[k] for k in sorted(self.states)]

    def claim_daily(self, conn, key, day, now):
        if self.meta.get(key) == day.isoformat():
            return False
        self.meta[key] = day.isoformat()
        return True

    def update_tiers(self, conn, updates, now):
        self.tier_updates.extend(updates)
        for u in updates:
            s = self.states[u.agency_key]
            s.tier, s.poll_interval_minutes = u.tier, u.poll_interval_minutes
            s.rate_ewma_per_day, s.tier_updated_at = u.rate_ewma_per_day, now
        return len(updates)

    def mark_dispatched(self, conn, keys, now):
        if self.fail_mark:
            raise RuntimeError("lock timeout")
        self.dispatched.append((list(keys), now))
        for k in keys:
            self.states[k].last_dispatched_at = now
        return len(keys)


class FakeTasks:
    def __init__(self, statuses=None, default=CREATED):
        self.statuses = dict(statuses or {})
        self.default = default
        self.calls: list[tuple[str, dict]] = []

    def create_task(self, task_id, body):
        self.calls.append((task_id, body))
        status = self.statuses.get(body["agency"], self.default)
        if status == FAILED:
            return CreateResult(status, task_id, http_status=403, error="PERMISSION_DENIED")
        return CreateResult(status, task_id, http_status=409 if status == DUPLICATE else 200)


@pytest.fixture
def env(monkeypatch):
    """Repositório e consultas em memória dentro do módulo service."""
    holder = SimpleNamespace(repo=FakeRepo(), counts={}, unthemed=[])

    def setup(states=(), unavailable=False, counts=None, unthemed=None):
        holder.repo = FakeRepo(states, unavailable)
        holder.counts = counts or {}
        holder.unthemed = unthemed or []
        monkeypatch.setattr(service_module, "repository", holder.repo)
        monkeypatch.setattr(
            service_module,
            "queries",
            SimpleNamespace(
                daily_news_counts=lambda conn, today: holder.counts,
                unthemed_news=lambda conn, now: holder.unthemed,
            ),
        )
        return holder

    monkeypatch.setenv("PUBSUB_TOPIC_NEWS_SCRAPED", "projects/p/topics/dgb.news.scraped")
    return setup


def _state(key, due_in_min=-1.0, now=DAY, **overrides) -> AgencyState:
    state = AgencyState.new(key, now - timedelta(days=2))
    state.next_due_at = now + timedelta(minutes=due_in_min)
    state.tier_updated_at = now - timedelta(days=1)
    state.last_success_at = now - timedelta(minutes=30)
    return replace(state, **overrides)


def _dispatcher(mode, now=DAY, keys=("mec", "mds", "mme"), tasks=None, publisher=None):
    conn = MagicMock(name="conn")
    if mode == DispatchMode.LIVE and tasks is None:
        tasks = FakeTasks()
    factory = (lambda: publisher) if publisher is not None else MagicMock()
    return (
        Dispatcher(
            conn, mode=mode, now=now, active_keys=keys, tasks=tasks, publisher_factory=factory
        ),
        conn,
        tasks,
    )


class TestConstruction:
    def test_off_mode_is_rejected(self):
        with pytest.raises(ValueError):
            Dispatcher(MagicMock(), mode=DispatchMode.OFF, now=DAY, active_keys=[])

    def test_live_requires_tasks_client(self):
        with pytest.raises(ValueError):
            Dispatcher(MagicMock(), mode=DispatchMode.LIVE, now=DAY, active_keys=[])


class TestTickDryRun:
    def test_selects_without_creating_or_marking(self, env):
        h = env([_state("mec"), _state("mds", due_in_min=60), _state("mme", due_in_min=-90)])
        dispatcher, conn, _ = _dispatcher(DispatchMode.DRY_RUN)
        out = dispatcher.tick()
        assert (out.mode, out.reason, out.night_skip) == ("dry_run", "tick", False)
        assert out.due == 2
        assert (out.dispatched, out.duplicates, out.failed) == (0, 0, 0)
        assert [a["agency"] for a in out.agencies] == ["mme", "mec"]
        assert {a["result"] for a in out.agencies} == {"dry_run"}
        assert out.agencies[0]["task"] == "scrape-mme-202611191000"
        assert h.repo.dispatched == []
        conn.commit.assert_called()

    def test_recomputes_tiers_but_does_not_reconcile(self, env):
        h = env(
            [_state("mec")],
            counts={"mec": {date(2026, 11, 18): 50}},
            unthemed=[{"unique_id": "x", "agency_key": "mec", "published_at": None}],
        )
        publisher = MagicMock()
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, publisher=publisher)
        out = dispatcher.tick()
        assert out.tiers_recomputed is True
        assert h.repo.meta == {TIERS_RECOMPUTED_KEY: "2026-11-19"}
        assert out.reconciled == 0
        publisher.publish_scraped.assert_not_called()

    def test_ensure_rows_creates_active_agencies(self, env):
        h = env([])
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, keys=["mec", "agencia_brasil"])
        out = dispatcher.tick()
        assert set(h.repo.states) == {"mec", "agencia_brasil"}
        assert out.due == 2

    def test_state_unavailable_propagates(self, env):
        env(unavailable=True)
        dispatcher, conn, _ = _dispatcher(DispatchMode.DRY_RUN)
        with pytest.raises(StateUnavailable):
            dispatcher.tick()
        conn.rollback.assert_called()


class TestTickLive:
    def test_creates_tasks_and_marks_created_and_duplicates(self, env):
        h = env([_state("mec"), _state("mds"), _state("mme")])
        tasks = FakeTasks({"mds": DUPLICATE, "mme": FAILED})
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, tasks=tasks)
        out = dispatcher.tick()
        assert (out.due, out.dispatched, out.duplicates, out.failed) == (3, 1, 1, 1)
        # Mesmo vencimento: ordem da sigla.
        at_iso = "2026-11-19T13:00:00Z"
        assert [c[1] for c in tasks.calls] == [
            {"agency": "mds", "trigger": "task", "dispatched_at": at_iso},
            {"agency": "mec", "trigger": "task", "dispatched_at": at_iso},
            {"agency": "mme", "trigger": "task", "dispatched_at": at_iso},
        ]
        ((marked, at),) = h.repo.dispatched
        assert sorted(marked) == ["mds", "mec"]  # a que falhou não é marcada
        assert at == DAY
        results = {a["agency"]: a["result"] for a in out.agencies}
        assert results == {"mec": CREATED, "mds": DUPLICATE, "mme": FAILED}
        errors = {a["agency"]: a.get("error") for a in out.agencies}
        assert errors == {"mec": None, "mds": None, "mme": "HTTP 403: PERMISSION_DENIED"}

    def test_task_body_and_name(self, env):
        env([_state("mec")])
        tasks = FakeTasks()
        dispatcher, _, _ = _dispatcher(
            DispatchMode.LIVE, now=_brt(14, 7), keys=["mec"], tasks=tasks
        )
        dispatcher.tick()
        assert tasks.calls == [
            (
                "scrape-mec-202611191400",
                {"agency": "mec", "trigger": "task", "dispatched_at": "2026-11-19T17:07:00Z"},
            )
        ]

    def test_inflight_agency_is_not_dispatched_again(self, env):
        env([_state("mec", last_dispatched_at=DAY - timedelta(minutes=5))])
        tasks = FakeTasks()
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], tasks=tasks)
        out = dispatcher.tick()
        assert (out.due, out.skipped_inflight) == (0, 1)
        assert tasks.calls == []

    def test_stops_trying_after_consecutive_failures(self, env):
        keys = [f"ag{i:02d}" for i in range(10)]
        env([_state(k, due_in_min=-i) for i, k in enumerate(keys)])
        tasks = FakeTasks(default=FAILED)
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=keys, tasks=tasks)
        with pytest.raises(DispatchFailed) as excinfo:
            dispatcher.tick()
        out = excinfo.value.outcome
        assert len(tasks.calls) == MAX_CONSECUTIVE_CREATE_FAILURES
        assert (out.due, out.failed, out.dispatched) == (10, 10, 0)

    def test_all_creations_failing_is_fatal_after_freshness_and_reconcile(self, env, monkeypatch):
        # 403 em todas (actAs revogado, fila apagada): sem tarefa nenhuma agência é raspada.
        # O tick não pode terminar como um heartbeat normal.
        articles = [{"unique_id": "n1", "agency_key": "mec", "published_at": None}]
        h = env([_state("mec"), _state("mds")], unthemed=articles)
        logged = []
        monkeypatch.setattr(service_module, "log_freshness", lambda **kw: logged.append(kw))
        publisher = MagicMock()
        publisher.publish_scraped.return_value = 1
        tasks = FakeTasks(default=FAILED)
        dispatcher, _, _ = _dispatcher(
            DispatchMode.LIVE, keys=["mec", "mds"], tasks=tasks, publisher=publisher
        )
        with pytest.raises(DispatchFailed, match="recusou 2 de 2") as excinfo:
            dispatcher.tick()
        out = excinfo.value.outcome
        assert (out.due, out.dispatched, out.duplicates, out.failed) == (2, 0, 0, 2)
        assert "HTTP 403: PERMISSION_DENIED" in str(excinfo.value)
        assert h.repo.dispatched == []
        # O frescor e o reconciliador não dependem da fila: rodam antes da falha.
        assert logged
        assert out.reconciled == 1

    def test_all_duplicates_is_not_fatal(self, env):
        env([_state("mec")])
        tasks = FakeTasks(default=DUPLICATE)
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], tasks=tasks)
        out = dispatcher.tick()
        assert (out.dispatched, out.duplicates, out.failed) == (0, 1, 0)

    def test_nothing_due_is_not_fatal(self, env):
        env([_state("mec", due_in_min=60)])
        tasks = FakeTasks(default=FAILED)
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], tasks=tasks)
        out = dispatcher.tick()
        assert (out.due, out.failed) == (0, 0)
        assert tasks.calls == []

    def test_a_success_resets_the_failure_streak(self, env):
        keys = [f"ag{i:02d}" for i in range(8)]
        env([_state(k, due_in_min=-(10 - i)) for i, k in enumerate(keys)])
        statuses = dict.fromkeys(keys, FAILED)
        statuses["ag04"] = CREATED
        tasks = FakeTasks(statuses)
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=keys, tasks=tasks)
        out = dispatcher.tick()
        assert len(tasks.calls) == 8
        assert (out.dispatched, out.failed) == (1, 7)

    def test_mark_failure_does_not_break_the_tick(self, env):
        h = env([_state("mec")])
        h.repo.fail_mark = True
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        out = dispatcher.tick()
        assert out.dispatched == 1

    def test_reconciles_once_per_day(self, env):
        articles = [
            {"unique_id": f"n{i}", "agency_key": "mec", "published_at": None} for i in range(3)
        ]
        h = env([_state("mec")], unthemed=articles)
        publisher = MagicMock()
        publisher.publish_scraped.return_value = 3
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], publisher=publisher)
        assert dispatcher.tick().reconciled == 3
        publisher.publish_scraped.assert_called_once_with(articles)
        publisher.close.assert_called_once()
        assert h.repo.meta[RECONCILED_KEY] == "2026-11-19"
        # Segundo tick do mesmo dia: não reconcilia de novo.
        again, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], publisher=publisher)
        assert again.tick().reconciled == 0
        assert publisher.publish_scraped.call_count == 1

    def test_reconcile_without_topic_does_not_claim_the_day(self, env, monkeypatch):
        h = env([_state("mec")], unthemed=[{"unique_id": "x", "agency_key": "mec"}])
        monkeypatch.delenv("PUBSUB_TOPIC_NEWS_SCRAPED")
        publisher = MagicMock()
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], publisher=publisher)
        assert dispatcher.tick().reconciled == 0
        assert RECONCILED_KEY not in h.repo.meta
        publisher.publish_scraped.assert_not_called()

    def test_reconcile_with_nothing_to_republish(self, env):
        env([_state("mec")], unthemed=[])
        publisher = MagicMock()
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], publisher=publisher)
        assert dispatcher.tick().reconciled == 0
        publisher.publish_scraped.assert_not_called()


class TestDailyJobs:
    def test_not_before_three_am(self, env):
        h = env([_state("mec", now=_brt(2, 45))])
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, now=_brt(2, 45), keys=["mec"])
        out = dispatcher.tick()
        assert out.tiers_recomputed is False
        assert out.reconciled == 0
        assert h.repo.meta == {}

    def test_three_am_pass_recomputes_reconciles_and_dispatches(self, env):
        now = _brt(3, 0)
        h = env(
            [_state("mec", now=now, due_in_min=-200), _state("mds", now=now, due_in_min=300)],
            counts={"mec": {date(2026, 11, 18): 140}},
            unthemed=[{"unique_id": "n1", "agency_key": "mec", "published_at": None}],
        )
        publisher = MagicMock()
        publisher.publish_scraped.return_value = 1
        dispatcher, _, tasks = _dispatcher(
            DispatchMode.LIVE, now=now, keys=["mec", "mds"], publisher=publisher
        )
        out = dispatcher.tick()
        assert out.night_skip is False
        assert out.tiers_recomputed is True
        assert out.reconciled == 1
        assert [c[1]["agency"] for c in tasks.calls] == ["mec"]
        assert h.repo.states["mec"].tier == Tier.M30  # 140 ontem ≈ 9,6/dia
        assert out.tiers_changed == 2  # mec h2→m30, mds h2→d1
        assert set(h.repo.meta) == {TIERS_RECOMPUTED_KEY, RECONCILED_KEY}

    def test_recompute_runs_once_per_day(self, env):
        h = env([_state("mec")])
        first, _, _ = _dispatcher(DispatchMode.DRY_RUN, now=_brt(3, 15), keys=["mec"])
        assert first.tick().tiers_recomputed is True
        second, _, _ = _dispatcher(DispatchMode.DRY_RUN, now=_brt(3, 30), keys=["mec"])
        assert second.tick().tiers_recomputed is False
        assert len(h.repo.tier_updates) == 1
        next_day, _, _ = _dispatcher(DispatchMode.DRY_RUN, now=_brt(3, 0, day=20), keys=["mec"])
        assert next_day.tick().tiers_recomputed is True

    def test_recompute_failure_is_a_warning(self, env, monkeypatch):
        h = env([_state("mec")])

        def boom(conn, updates, now):
            raise RuntimeError("deadlock")

        monkeypatch.setattr(h.repo, "update_tiers", boom)
        dispatcher, conn, _ = _dispatcher(DispatchMode.DRY_RUN, keys=["mec"])
        out = dispatcher.tick()
        assert out.tiers_recomputed is False
        assert out.due == 1
        conn.rollback.assert_called()


class TestNightAndFreshness:
    def test_night_tick_only_logs(self, env, monkeypatch):
        logged = []
        monkeypatch.setattr(service_module, "log_freshness", lambda **kw: logged.append(kw))
        now = _brt(1, 30, day=20)
        env([_state("mec", now=now, due_in_min=-120)])
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, now=now, keys=["mec"])
        out = dispatcher.tick()
        assert out.night_skip is True
        assert out.due == 0
        assert tasks.calls == []
        assert logged == []  # frescor só nos ticks diurnos

    def test_daytime_tick_logs_freshness_per_tier(self, env, monkeypatch):
        logged = []
        monkeypatch.setattr(service_module, "log_freshness", lambda **kw: logged.append(kw))
        h = env(
            [
                _state("mec", due_in_min=60),
                _state("agencia_brasil", due_in_min=10, tier=Tier.M30, poll_interval_minutes=30),
                _state("tvbrasil", due_in_min=10, tier=Tier.M30, poll_interval_minutes=30),
                _state("antaq", due_in_min=60, circuit_state=CircuitState.OPEN),
            ]
        )
        h.repo.meta[TIERS_RECOMPUTED_KEY] = "2026-11-19"  # faixas de hoje já calculadas
        keys = ["mec", "agencia_brasil", "tvbrasil", "antaq"]
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, keys=keys)
        out = dispatcher.tick()
        assert {e["tier"]: e["agencies"] for e in logged} == {"m30": 1, "h2": 1}
        assert all(e["slo_ok_ratio"] == 1.0 for e in logged)
        assert out.slo_ok_ratio == 1.0

    @pytest.mark.parametrize(
        ("mode", "expected"), [(DispatchMode.DRY_RUN, 1.0), (DispatchMode.LIVE, 0.0)]
    )
    def test_slo_floor_of_the_dag_cadence_outside_live(self, env, monkeypatch, mode, expected):
        # m30 com 100 min desde o sucesso: fora de 2 × 30, dentro de 2 × 120. Em dry_run as
        # DAGs de 2 h ainda são o relógio (o mesmo piso do GET /agencies/state).
        logged = []
        monkeypatch.setattr(service_module, "log_freshness", lambda **kw: logged.append(kw))
        h = env(
            [
                _state(
                    "agencia_brasil",
                    due_in_min=60,
                    tier=Tier.M30,
                    poll_interval_minutes=30,
                    last_success_at=DAY - timedelta(minutes=100),
                )
            ]
        )
        h.repo.meta[TIERS_RECOMPUTED_KEY] = "2026-11-19"
        dispatcher, _, _ = _dispatcher(mode, keys=["agencia_brasil"])
        out = dispatcher.tick()
        assert out.slo_ok_ratio == expected
        assert [e["slo_ok_ratio"] for e in logged] == [expected]

    def test_three_am_pass_has_no_freshness(self, env, monkeypatch):
        logged = []
        monkeypatch.setattr(service_module, "log_freshness", lambda **kw: logged.append(kw))
        env([_state("mec", now=_brt(3, 0))])
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, now=_brt(3, 0), keys=["mec"])
        dispatcher.tick()
        assert logged == []


class TestBackfill:
    def test_live_creates_one_task_per_agency_with_window(self, env):
        h = env([_state("mec", due_in_min=600), _state("mds")])
        tasks = FakeTasks()
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, now=_brt(14, 42), tasks=tasks)
        out = dispatcher.backfill(
            agencies=["mec", "mds"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 3)
        )
        assert (out.reason, out.due, out.dispatched) == ("backfill", 2, 2)
        assert tasks.calls == [
            (
                "backfill-mec-20261101-20261103-1442",
                {
                    "agency": "mec",
                    "start_date": "2026-11-01",
                    "end_date": "2026-11-03",
                    "trigger": "backfill",
                },
            ),
            (
                "backfill-mds-20261101-20261103-1442",
                {
                    "agency": "mds",
                    "start_date": "2026-11-01",
                    "end_date": "2026-11-03",
                    "trigger": "backfill",
                },
            ),
        ]
        assert h.repo.dispatched == []  # o backfill não marca last_dispatched_at
        body = out.response()
        assert (body["start_date"], body["end_date"]) == ("2026-11-01", "2026-11-03")

    def test_dedup_key_replaces_the_clock_in_the_task_name(self, env):
        env([])
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, now=_brt(14, 42), keys=["mec"])
        dispatcher.backfill(
            agencies=["mec"],
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 3),
            dedup_key="execucao-01",
        )
        assert tasks.calls[0][0] == "backfill-mec-20261101-20261103-execucao-01"

    def test_empty_agencies_means_all_active(self, env):
        env([])
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, keys=["mec", "mds"])
        out = dispatcher.backfill(
            agencies=[], start_date=date(2026, 11, 1), end_date=date(2026, 11, 1)
        )
        assert [c[1]["agency"] for c in tasks.calls] == ["mds", "mec"]  # ativas, em ordem
        assert out.dispatched == 2

    def test_unknown_agencies_are_reported(self, env):
        env([])
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        out = dispatcher.backfill(
            agencies=["mec", "sri", "mec"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 2)
        )
        assert out.unknown_agencies == ["sri"]
        assert [c[1]["agency"] for c in tasks.calls] == ["mec"]

    def test_open_circuit_is_skipped_unless_forced(self, env):
        open_state = _state("mec", due_in_min=120, circuit_state=CircuitState.OPEN)
        env([open_state])
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        out = dispatcher.backfill(
            agencies=["mec"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 1)
        )
        assert out.skipped_circuit == ["mec"]
        assert tasks.calls == []

        forced, _, tasks = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        out = forced.backfill(
            agencies=["mec"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 1), force=True
        )
        assert out.skipped_circuit == []
        assert tasks.calls[0][1]["force"] is True

    def test_dry_run_only_lists(self, env):
        env([])
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, keys=["mec", "mds"])
        out = dispatcher.backfill(
            agencies=None, start_date=date(2026, 11, 1), end_date=date(2026, 11, 2)
        )
        assert out.due == 2
        assert out.dispatched == 0
        assert {a["result"] for a in out.agencies} == {"dry_run"}

    def test_state_unavailable_ignores_circuit(self, env):
        env(unavailable=True)
        dispatcher, _, tasks = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        out = dispatcher.backfill(
            agencies=["mec"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 1)
        )
        assert out.dispatched == 1

    def test_all_creations_failing_is_not_fatal_in_backfill(self, env):
        # A DAG lê failed e falha listando as siglas; o heartbeat é dos ticks.
        env([])
        tasks = FakeTasks(default=FAILED)
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"], tasks=tasks)
        out = dispatcher.backfill(
            agencies=["mec"], start_date=date(2026, 11, 1), end_date=date(2026, 11, 1)
        )
        assert (out.dispatched, out.failed) == (0, 1)
        assert out.agencies[0]["error"] == "HTTP 403: PERMISSION_DENIED"


class TestOutcome:
    def test_log_fields_have_the_contract_names(self, env):
        env([_state("mec")])
        dispatcher, _, _ = _dispatcher(DispatchMode.LIVE, keys=["mec"])
        fields = dispatcher.tick().log_fields()
        for name in (
            "mode",
            "reason",
            "night_skip",
            "due",
            "dispatched",
            "duplicates",
            "skipped_inflight",
            "tiers_recomputed",
            "reconciled",
        ):
            assert name in fields
        assert fields["agencies"] == ["mec"]
        # O runbook manda conferir o slo_ok_ratio no dispatch_run do dia a seco.
        assert "slo_ok_ratio" in fields

    def test_response_has_the_contract_keys(self, env):
        env([_state("mec")])
        dispatcher, _, _ = _dispatcher(DispatchMode.DRY_RUN, keys=["mec"])
        body = dispatcher.tick().response()
        assert {
            "mode",
            "night_skip",
            "due",
            "dispatched",
            "duplicates",
            "skipped_inflight",
            "tiers_recomputed",
            "reconciled",
            "agencies",
        } <= set(body)
        assert "unknown_agencies" not in body
