"""Seleção pura do /dispatch (select_due): vencidas, em voo, disjuntor, noite, teto e ordem."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from govbr_scraper.dispatch.selection import (
    MAX_DISPATCH_PER_TICK,
    in_flight,
    is_night,
    is_night_pass,
    is_night_skip,
    select_due,
)
from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import BRT

NOW = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)  # 10:00 BRT


def _state(key="mec", due_in_min=-1.0, **overrides) -> AgencyState:
    base = AgencyState.new(key, NOW - timedelta(days=2))
    base.next_due_at = NOW + timedelta(minutes=due_in_min)
    return replace(base, **overrides)


def _brt(hour, minute=0):
    return datetime(2026, 11, 19, hour, minute, tzinfo=BRT).astimezone(UTC)


class TestDue:
    def test_due_includes_lookahead_of_seven_minutes(self):
        states = [
            _state("mec", due_in_min=-30),
            _state("mds", due_in_min=7),  # vence dentro do meio tick
            _state("mme", due_in_min=7.5),  # fica para o próximo tick
        ]
        selection = select_due(states, ["mec", "mds", "mme"], NOW)
        assert selection.agency_keys == ["mec", "mds"]
        assert not selection.night_skip

    def test_inactive_rows_are_ignored(self):
        selection = select_due([_state("sri")], ["mec"], NOW)
        assert selection.agency_keys == ["mec"]  # mec sem linha entra como nova

    def test_active_without_row_is_due_now(self):
        selection = select_due([], ["mec", "mds"], NOW)
        assert selection.agency_keys == ["mds", "mec"]  # mesmo vencimento: ordem da sigla
        assert all(d.state.tier == Tier.H2 for d in selection.due)

    def test_naive_datetimes_are_utc(self):
        state = _state("mec", next_due_at=NOW.replace(tzinfo=None) - timedelta(minutes=1))
        assert select_due([state], ["mec"], NOW.replace(tzinfo=None)).agency_keys == ["mec"]


class TestInFlight:
    def test_dispatched_recently_without_attempt_is_in_flight(self):
        state = _state(
            last_dispatched_at=NOW - timedelta(minutes=10),
            last_attempt_at=NOW - timedelta(hours=2),
        )
        assert in_flight(state, NOW)
        selection = select_due([state], ["mec"], NOW)
        assert selection.agency_keys == []
        assert selection.skipped_inflight == ["mec"]

    def test_never_attempted_and_dispatched_recently_is_in_flight(self):
        state = _state(last_dispatched_at=NOW - timedelta(minutes=5), last_attempt_at=None)
        assert in_flight(state, NOW)

    def test_attempt_after_dispatch_is_not_in_flight(self):
        state = _state(
            last_dispatched_at=NOW - timedelta(minutes=10),
            last_attempt_at=NOW - timedelta(minutes=9),
        )
        assert not in_flight(state, NOW)
        assert select_due([state], ["mec"], NOW).agency_keys == ["mec"]

    def test_dispatch_older_than_thirty_minutes_is_not_in_flight(self):
        """Tarefa perdida (esgotou as tentativas sem gravar): o tick seguinte despacha de novo."""
        state = _state(last_dispatched_at=NOW - timedelta(minutes=30), last_attempt_at=None)
        assert not in_flight(state, NOW)
        assert select_due([state], ["mec"], NOW).agency_keys == ["mec"]

    def test_never_dispatched_is_not_in_flight(self):
        assert not in_flight(_state(last_dispatched_at=None), NOW)


class TestCircuit:
    def test_open_circuit_not_due_is_not_selected(self):
        state = _state(
            "antaq",
            due_in_min=90,
            circuit_state=CircuitState.OPEN,
            circuit_opened_at=NOW - timedelta(minutes=30),
        )
        selection = select_due([state], ["antaq"], NOW)
        assert selection.agency_keys == []
        assert selection.skipped_inflight == []

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_open_circuit_due_is_the_probe(self, circuit):
        state = _state("antaq", due_in_min=3, circuit_state=circuit)
        (due,) = select_due([state], ["antaq"], NOW).due
        assert due.agency_key == "antaq"
        assert due.probe is True

    def test_closed_circuit_is_not_a_probe(self):
        (due,) = select_due([_state("mec")], ["mec"], NOW).due
        assert due.probe is False


class TestNight:
    @pytest.mark.parametrize(
        ("hour", "minute", "night", "night_pass", "skip"),
        [
            (22, 59, False, False, False),
            (23, 0, True, False, True),
            (2, 45, True, False, True),
            (3, 0, True, True, False),
            (3, 14, True, True, False),
            (3, 15, True, False, True),
            (5, 59, True, False, True),
            (6, 0, False, False, False),
        ],
    )
    def test_night_rules(self, hour, minute, night, night_pass, skip):
        now = _brt(hour, minute)
        assert is_night(now) is night
        assert is_night_pass(now) is night_pass
        assert is_night_skip(now) is skip

    def test_night_tick_selects_nothing(self):
        now = _brt(1, 30)
        selection = select_due([_state("mec", next_due_at=now - timedelta(hours=3))], ["mec"], now)
        assert selection.night_skip is True
        assert selection.due == []

    def test_three_am_pass_dispatches_everything_due(self):
        now = _brt(3, 0)
        states = [
            _state("mec", tier=Tier.M30, next_due_at=now - timedelta(hours=4)),
            _state("mds", tier=Tier.D1, next_due_at=now - timedelta(minutes=1)),
            _state("mme", tier=Tier.H2, next_due_at=now + timedelta(hours=1)),
        ]
        selection = select_due(states, ["mec", "mds", "mme"], now)
        assert selection.night_skip is False
        assert selection.agency_keys == ["mec", "mds"]


class TestOrderAndCap:
    def test_ordered_by_next_due_then_key(self):
        states = [
            _state("c", due_in_min=-5),
            _state("a", due_in_min=-60),
            _state("b", due_in_min=-5),
        ]
        assert select_due(states, ["a", "b", "c"], NOW).agency_keys == ["a", "b", "c"]

    def test_cap_of_200_keeps_the_oldest_and_reports_overflow(self):
        keys = [f"ag{i:03d}" for i in range(250)]
        # ag249 é a mais atrasada; ag000, a menos.
        states = [_state(k, due_in_min=-(i + 1)) for i, k in enumerate(keys)]
        selection = select_due(states, keys, NOW)
        assert MAX_DISPATCH_PER_TICK == 200
        assert len(selection.due) == 200
        assert selection.due[0].agency_key == "ag249"
        assert selection.due[-1].agency_key == "ag050"
        assert sorted(selection.overflow) == keys[:50]

    def test_custom_limit(self):
        states = [_state(k, due_in_min=-i) for i, k in enumerate(["a", "b", "c"])]
        selection = select_due(states, ["a", "b", "c"], NOW, limit=2)
        assert selection.agency_keys == ["c", "b"]
        assert selection.overflow == ["a"]

    def test_inflight_does_not_take_cap_slots(self):
        states = [
            _state("a", due_in_min=-60, last_dispatched_at=NOW - timedelta(minutes=5)),
            _state("b", due_in_min=-5),
        ]
        selection = select_due(states, ["a", "b"], NOW, limit=1)
        assert selection.agency_keys == ["b"]
        assert selection.skipped_inflight == ["a"]
        assert selection.overflow == []
