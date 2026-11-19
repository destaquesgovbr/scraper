"""repository (SQL com conexão mockada) e report (corpo de GET /agencies/state)."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from psycopg2 import errors

from govbr_scraper.state import repository
from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.report import build_state_report, iso_utc, slo_interval_floor
from govbr_scraper.state.repository import StateUnavailable

T0 = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)


def _conn(rows=None, rowcount=1, error=None):
    cur = MagicMock()
    cur.fetchall.return_value = rows or []
    cur.fetchone.return_value = rows[0] if rows else None
    cur.rowcount = rowcount
    if error is not None:
        cur.execute.side_effect = error
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _row(state: AgencyState) -> tuple:
    return tuple(
        getattr(state, c).value if c in ("tier", "circuit_state") else getattr(state, c)
        for c in repository.COLUMNS
    )


class TestRepository:
    def test_columns_match_the_migration(self):
        assert repository.COLUMNS == (
            "agency_key",
            "next_due_at",
            "tier",
            "poll_interval_minutes",
            "rate_ewma_per_day",
            "tier_updated_at",
            "boost_until",
            "last_dispatched_at",
            "last_attempt_at",
            "last_success_at",
            "last_new_article_at",
            "consecutive_permanent_failures",
            "consecutive_transient_failures",
            "failure_streak_started_at",
            "circuit_state",
            "circuit_opened_at",
            "circuit_failed_probes",
            "last_error_category",
            "last_error_message",
            "created_at",
            "updated_at",
        )

    def test_get_state_maps_enums(self):
        state = replace(AgencyState.new("mec", T0), circuit_state=CircuitState.OPEN, tier=Tier.H6)
        conn, cur = _conn([_row(state)])
        result = repository.get_state(conn, "mec")
        assert result == state
        assert isinstance(result.tier, Tier)
        sql, params = cur.execute.call_args[0]
        assert "FROM scrape_agency_state WHERE agency_key = %s" in sql
        assert "FOR UPDATE" not in sql
        assert params == ("mec",)
        conn.commit.assert_not_called()

    def test_get_state_for_update_and_missing_row(self):
        conn, cur = _conn([])
        assert repository.get_state(conn, "mec", for_update=True) is None
        assert cur.execute.call_args[0][0].endswith("FOR UPDATE")

    @pytest.mark.parametrize(
        "error",
        [
            errors.UndefinedTable('relation "scrape_agency_state" does not exist'),
            errors.UndefinedColumn('column "boost_until" does not exist'),
        ],
    )
    def test_missing_table_raises_state_unavailable(self, error):
        conn, _ = _conn(error=error)
        with pytest.raises(StateUnavailable) as exc:
            repository.list_states(conn)
        assert exc.value.__cause__ is error

    def test_other_errors_propagate(self):
        conn, _ = _conn(error=RuntimeError("conexão caiu"))
        with pytest.raises(RuntimeError):
            repository.get_state(conn, "mec")

    def test_upsert_sends_enum_values_and_keeps_created_at(self):
        state = replace(AgencyState.new("mec", T0), circuit_state=CircuitState.HALF_OPEN)
        conn, cur = _conn()
        repository.upsert_state(conn, state)
        sql, params = cur.execute.call_args[0]
        assert "ON CONFLICT (agency_key) DO UPDATE SET" in sql
        assert "created_at = EXCLUDED" not in sql
        assert "agency_key = EXCLUDED" not in sql
        values = dict(zip(repository.COLUMNS, params, strict=True))
        assert values["circuit_state"] == "half_open"
        assert type(values["circuit_state"]) is str
        assert values["tier"] == "h2"

    def test_ensure_rows_dedups_and_counts(self):
        conn, cur = _conn(rowcount=2)
        assert repository.ensure_rows(conn, ["mec", "mds", "mec"], T0) == 2
        sql, params = cur.execute.call_args[0]
        assert "ON CONFLICT (agency_key) DO NOTHING" in sql
        assert params == (T0, T0, T0, ["mds", "mec"])

    def test_ensure_rows_empty_is_noop(self):
        conn, cur = _conn()
        assert repository.ensure_rows(conn, [], T0) == 0
        cur.execute.assert_not_called()

    def test_mark_probe_only_open_circuits(self):
        conn, cur = _conn(rowcount=1)
        assert repository.mark_probe(conn, "mec", T0) is True
        sql, params = cur.execute.call_args[0]
        assert "circuit_state = 'half_open'" in sql
        assert "circuit_state IN ('open', 'half_open')" in sql
        assert params == (T0, "mec")
        conn, _ = _conn(rowcount=0)
        assert repository.mark_probe(conn, "mec", T0) is False

    def test_mark_dispatched(self):
        conn, cur = _conn(rowcount=2)
        assert repository.mark_dispatched(conn, ["mec", "mds"], T0) == 2
        assert cur.execute.call_args[0][1] == (T0, T0, ["mds", "mec"])


def _st(key, **overrides):
    return replace(AgencyState.new(key, T0 - timedelta(days=2)), **overrides)


class TestBuildStateReport:
    def test_entries_and_summary(self):
        states = [
            _st(
                "mec",
                circuit_state=CircuitState.OPEN,
                last_error_category="html_changed",
                last_success_at=T0 - timedelta(days=22),
            ),
            _st(
                "mds",
                last_success_at=T0 - timedelta(minutes=30),
                tier=Tier.H6,
                poll_interval_minutes=360,
                boost_until=T0 + timedelta(hours=1),
            ),
            _st(
                "pf",
                tier=Tier.M30,
                poll_interval_minutes=30,
                # 14:00 BRT de ontem: fora do SLO mesmo com o piso de 2 h das DAGs.
                last_success_at=T0 - timedelta(hours=20),
            ),
            _st("sri", last_success_at=T0 - timedelta(days=9)),  # inativa
        ]
        report = build_state_report(states, ["mec", "mds", "pf", "ansn"], T0, "off")

        assert report["generated_at"] == "2026-11-19T13:00:00Z"
        assert report["dispatch_mode"] == "off"
        by_key = {a["agency_key"]: a for a in report["agencies"]}
        mds = by_key["mds"]
        assert mds["active"] is True
        assert mds["tier"] == "h6"
        assert mds["poll_interval_minutes"] == 360
        assert mds["effective_interval_minutes"] == 180
        assert mds["lag_minutes"] == 30.0
        assert mds["within_slo"] is True
        assert mds["boost_until"] == "2026-11-19T14:00:00Z"
        assert mds["circuit_state"] == "closed"
        assert by_key["mec"]["within_slo"] is None
        assert by_key["pf"]["within_slo"] is False
        assert by_key["sri"]["active"] is False
        for key in (
            "rate_ewma_per_day",
            "next_due_at",
            "last_dispatched_at",
            "last_attempt_at",
            "last_new_article_at",
            "consecutive_permanent_failures",
            "consecutive_transient_failures",
            "failure_streak_started_at",
            "circuit_opened_at",
            "circuit_failed_probes",
            "last_error_category",
            "last_error_message",
        ):
            assert key in mds
        json.dumps(report)  # serializável

        assert report["summary"] == {
            "total": 4,
            "active": 3,
            "by_tier": {"m30": 1, "h2": 1, "h6": 1, "d1": 0},
            "circuit_open": 1,
            "overdue": 1,
            "slo_ok_ratio": 0.5,
        }

    @pytest.mark.parametrize(
        ("mode", "expected"), [("off", True), ("dry_run", True), ("live", False)]
    )
    def test_slo_floor_is_the_dag_cadence_until_live(self, mode, expected):
        """Até a virada as DAGs raspam a cada 2 h: a m30 com sucesso há 70 min e a h2 com
        boost há 130 min não estão atrasadas. Em live vale o intervalo efetivo da faixa
        (2 × 30 e 2 × 60 min)."""
        states = [
            _st(
                "pf",
                tier=Tier.M30,
                poll_interval_minutes=30,
                last_success_at=T0 - timedelta(minutes=70),
            ),
            _st(
                "mec",
                boost_until=T0 + timedelta(hours=2),
                last_success_at=T0 - timedelta(minutes=130),
            ),
        ]
        report = build_state_report(states, ["pf", "mec"], T0, mode)
        by_key = {a["agency_key"]: a for a in report["agencies"]}
        assert by_key["pf"]["within_slo"] is expected
        assert by_key["mec"]["within_slo"] is expected
        assert report["summary"]["overdue"] == (0 if expected else 2)
        assert by_key["pf"]["effective_interval_minutes"] == 30
        assert by_key["mec"]["effective_interval_minutes"] == 60

    def test_slo_interval_floor(self):
        assert slo_interval_floor("live") == 0
        assert slo_interval_floor("off") == slo_interval_floor("dry_run") == 120

    def test_empty(self):
        report = build_state_report([], ["mec"], T0, "dry_run")
        assert report["summary"]["total"] == 0
        assert report["summary"]["slo_ok_ratio"] is None

    def test_iso_utc(self):
        assert iso_utc(None) is None
        assert iso_utc(datetime(2026, 11, 19, 10, 0)) == "2026-11-19T10:00:00Z"
