"""Regras do estado por agência: apply_attempt, intervalo efetivo e disjuntor (circuit_gate)."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from govbr_scraper.models.monitoring import ScrapeRunResult
from govbr_scraper.state.model import (
    AgencyState,
    AttemptOutcome,
    CircuitState,
    Tier,
)
from govbr_scraper.state.policy import (
    BOOST_DURATION,
    BOOST_TIERS,
    BRT,
    DUE_SLACK,
    PERMANENT_FAILURES_TO_OPEN,
    PROBE_DELAYS,
    TRANSIENT_STREAK_TO_OPEN,
    WATERMARK_MAX_DAYS,
)
from govbr_scraper.state.transitions import (
    apply_attempt,
    circuit_gate,
    effective_interval_minutes,
    is_permanent,
)

T0 = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)  # 10:00 BRT


def _state(**overrides) -> AgencyState:
    state = AgencyState.new("mec", T0 - timedelta(days=1))
    return replace(state, **overrides)


def _outcome(success=True, *, started=T0, duration=timedelta(seconds=30), **overrides):
    data = {
        "agency_key": "mec",
        "started_at": started,
        "finished_at": started + duration,
        "success": success,
    }
    if not success:
        data.setdefault("category", "html_changed")
        data.setdefault("error_message", "No articles found on first page of mec")
    data.update(overrides)
    return AttemptOutcome(**data)


def _apply(prev, outcome, now=None):
    return apply_attempt(prev, outcome, now or outcome.finished_at)


class TestPolicyConstants:
    def test_values_from_the_design(self):
        assert PERMANENT_FAILURES_TO_OPEN == 2
        assert TRANSIENT_STREAK_TO_OPEN == timedelta(hours=24)
        assert PROBE_DELAYS == (timedelta(hours=2), timedelta(hours=6), timedelta(hours=24))
        assert DUE_SLACK == timedelta(minutes=5)
        assert BOOST_DURATION == timedelta(hours=6)
        assert BOOST_TIERS == {Tier.H2, Tier.H6, Tier.D1}
        assert WATERMARK_MAX_DAYS == 7
        assert str(BRT) == "America/Sao_Paulo"


class TestAgencyStateNew:
    def test_default_is_h2_due_now_and_closed(self):
        state = AgencyState.new("ansn", T0)
        assert state.tier == Tier.H2
        assert state.poll_interval_minutes == 120
        assert state.next_due_at == T0
        assert state.circuit_state == CircuitState.CLOSED
        assert state.consecutive_permanent_failures == 0
        assert state.created_at == state.updated_at == T0


class TestEffectiveInterval:
    @pytest.mark.parametrize(
        ("tier", "interval", "expected"),
        [(Tier.M30, 30, 30), (Tier.H2, 120, 60), (Tier.H6, 360, 180), (Tier.D1, 1440, 720)],
    )
    def test_boost_halves_only_h2_h6_d1(self, tier, interval, expected):
        state = _state(tier=tier, poll_interval_minutes=interval, boost_until=T0 + timedelta(1))
        assert effective_interval_minutes(state, T0) == expected

    def test_expired_boost_keeps_interval(self):
        state = _state(boost_until=T0 - timedelta(seconds=1))
        assert effective_interval_minutes(state, T0) == 120

    def test_without_boost(self):
        assert (
            effective_interval_minutes(_state(tier=Tier.H6, poll_interval_minutes=360), T0) == 360
        )


class TestExplicitWindow:
    @pytest.mark.parametrize("success", [True, False])
    def test_only_updated_at_changes(self, success):
        prev = _state(
            circuit_state=CircuitState.OPEN,
            consecutive_permanent_failures=3,
            last_success_at=T0 - timedelta(days=3),
        )
        new, transition = _apply(prev, _outcome(success, explicit_window=True), now=T0)
        assert transition is None
        assert new == replace(prev, updated_at=T0)


class TestSuccess:
    def test_resets_counters_and_advances_watermark_to_start(self):
        prev = _state(
            consecutive_permanent_failures=1,
            consecutive_transient_failures=4,
            failure_streak_started_at=T0 - timedelta(hours=3),
            last_error_category="network_error",
            last_error_message="timeout",
        )
        new, transition = _apply(prev, _outcome(True, articles_new=0))
        assert transition is None
        assert new.consecutive_permanent_failures == 0
        assert new.consecutive_transient_failures == 0
        assert new.failure_streak_started_at is None
        assert new.last_attempt_at == T0
        assert new.last_success_at == T0  # início, não o fim
        # Histórico do último erro preservado.
        assert (new.last_error_category, new.last_error_message) == ("network_error", "timeout")

    def test_next_due_from_start_minus_slack(self):
        new, _ = _apply(_state(), _outcome(True, articles_new=0))
        assert new.next_due_at == T0 + timedelta(minutes=120) - timedelta(minutes=5)

    def test_new_article_sets_boost_and_halves_next_due(self):
        new, _ = _apply(_state(), _outcome(True, articles_new=2))
        finished = T0 + timedelta(seconds=30)
        assert new.last_new_article_at == finished
        assert new.boost_until == finished + timedelta(hours=6)
        assert new.next_due_at == T0 + timedelta(minutes=60) - timedelta(minutes=5)

    def test_new_article_in_m30_does_not_shorten(self):
        prev = _state(tier=Tier.M30, poll_interval_minutes=30)
        new, _ = _apply(prev, _outcome(True, articles_new=5))
        assert new.boost_until is not None
        assert new.next_due_at == T0 + timedelta(minutes=25)

    @pytest.mark.parametrize("articles_new", [0, None])
    def test_no_new_article_keeps_previous_boost_fields(self, articles_new):
        boost = T0 + timedelta(hours=1)
        prev = _state(last_new_article_at=T0 - timedelta(hours=5), boost_until=boost)
        new, _ = _apply(prev, _outcome(True, articles_new=articles_new))
        assert new.last_new_article_at == T0 - timedelta(hours=5)
        assert new.boost_until == boost

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_probe_success_closes_circuit(self, circuit):
        prev = _state(
            circuit_state=circuit,
            circuit_opened_at=T0 - timedelta(hours=8),
            circuit_failed_probes=2,
            consecutive_permanent_failures=4,
            last_error_category="html_changed",
        )
        new, transition = _apply(prev, _outcome(True, articles_new=0))
        assert new.circuit_state == CircuitState.CLOSED
        assert new.circuit_failed_probes == 0
        assert new.circuit_opened_at is None
        assert transition.from_state == circuit
        assert transition.to_state == CircuitState.CLOSED
        assert transition.reason == "probe_success"
        assert transition.category == "html_changed"
        assert transition.agency_key == "mec"

    def test_forced_success_reason(self):
        prev = _state(circuit_state=CircuitState.OPEN)
        _, transition = _apply(prev, _outcome(True, forced=True))
        assert transition.reason == "forced_success"

    def test_success_with_closed_circuit_has_no_transition(self):
        assert _apply(_state(), _outcome(True, forced=True))[1] is None


class TestFailure:
    def test_records_attempt_streak_and_truncated_error(self):
        new, transition = _apply(
            _state(), _outcome(False, category="network_error", error_message="x" * 900)
        )
        assert transition is None
        assert new.last_attempt_at == T0
        assert new.failure_streak_started_at == T0
        assert new.last_error_category == "network_error"
        assert new.last_error_message == "x" * 500
        assert new.consecutive_transient_failures == 1
        assert new.consecutive_permanent_failures == 0
        assert new.last_success_at is None

    def test_streak_start_is_kept(self):
        prev = _state(failure_streak_started_at=T0 - timedelta(hours=2))
        new, _ = _apply(prev, _outcome(False, category="network_error"))
        assert new.failure_streak_started_at == T0 - timedelta(hours=2)

    @pytest.mark.parametrize(
        "category", ["network_error", "unknown", "empty_content", None, "unknown_agency"]
    )
    def test_transient_categories(self, category):
        assert not is_permanent(category)
        new, _ = _apply(_state(), _outcome(False, category=category))
        assert new.consecutive_transient_failures == 1
        assert new.consecutive_permanent_failures == 0

    @pytest.mark.parametrize(
        "category", ["html_changed", "url_broken", "access_denied", "anti_bot"]
    )
    def test_first_permanent_does_not_open(self, category):
        assert is_permanent(category)
        new, transition = _apply(_state(), _outcome(False, category=category))
        assert transition is None
        assert new.consecutive_permanent_failures == 1
        assert new.circuit_state == CircuitState.CLOSED
        assert new.next_due_at == T0 + timedelta(minutes=115)

    def test_second_permanent_opens(self):
        prev = _state(consecutive_permanent_failures=1, failure_streak_started_at=T0 - timedelta(2))
        outcome = _outcome(False, category="html_changed")
        new, transition = _apply(prev, outcome)
        assert new.circuit_state == CircuitState.OPEN
        assert new.circuit_opened_at == outcome.finished_at
        assert new.circuit_failed_probes == 0
        assert new.next_due_at == outcome.finished_at + timedelta(hours=2)
        assert transition.from_state == CircuitState.CLOSED
        assert transition.to_state == CircuitState.OPEN
        assert transition.reason == "permanent_errors"
        assert transition.category == "html_changed"

    def test_permanent_interleaved_with_transient_still_counts(self):
        """Caso mec: html_changed intercalado com network_error ainda abre (desde o sucesso)."""
        prev = _state(consecutive_permanent_failures=1, consecutive_transient_failures=1)
        new, transition = _apply(prev, _outcome(False, category="html_changed"))
        assert transition.reason == "permanent_errors"

    def test_transient_streak_of_24h_opens(self):
        prev = _state(
            consecutive_transient_failures=11, failure_streak_started_at=T0 - timedelta(hours=24)
        )
        new, transition = _apply(prev, _outcome(False, category="network_error"))
        assert new.circuit_state == CircuitState.OPEN
        assert transition.reason == "transient_24h"
        assert transition.category == "network_error"

    def test_transient_streak_under_24h_does_not_open(self):
        prev = _state(failure_streak_started_at=T0 - timedelta(hours=23, minutes=59))
        new, transition = _apply(prev, _outcome(False, category="network_error"))
        assert transition is None
        assert new.circuit_state == CircuitState.CLOSED

    def test_permanent_reason_wins_over_24h(self):
        prev = _state(
            consecutive_permanent_failures=1, failure_streak_started_at=T0 - timedelta(days=2)
        )
        _, transition = _apply(prev, _outcome(False, category="access_denied"))
        assert transition.reason == "permanent_errors"

    @pytest.mark.parametrize(
        ("failed_probes", "expected_probes", "delay"),
        [(0, 1, timedelta(hours=6)), (1, 2, timedelta(hours=24)), (2, 3, timedelta(hours=24))],
    )
    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_failed_probe_backs_off_without_transition(
        self, circuit, failed_probes, expected_probes, delay
    ):
        opened = T0 - timedelta(hours=3)
        prev = _state(
            circuit_state=circuit,
            circuit_opened_at=opened,
            circuit_failed_probes=failed_probes,
            consecutive_permanent_failures=2,
        )
        outcome = _outcome(False, category="html_changed")
        new, transition = _apply(prev, outcome)
        assert transition is None
        assert new.circuit_state == CircuitState.OPEN
        assert new.circuit_failed_probes == expected_probes
        assert new.circuit_opened_at == opened
        assert new.next_due_at == outcome.finished_at + delay
        assert new.consecutive_permanent_failures == 3

    def test_probe_sequence_is_2h_6h_24h_24h(self):
        state = _state(consecutive_permanent_failures=1)
        started = T0
        delays = []
        for _ in range(5):
            outcome = _outcome(False, started=started, category="html_changed")
            state, _ = apply_attempt(state, outcome, outcome.finished_at)
            delays.append(state.next_due_at - outcome.finished_at)
            started = state.next_due_at
        assert delays == [
            timedelta(hours=2),
            timedelta(hours=6),
            timedelta(hours=24),
            timedelta(hours=24),
            timedelta(hours=24),
        ]


class TestAttemptOutcomeFromRun:
    def _run(self, **overrides):
        data = {
            "agency_key": "mec",
            "status": "success",
            "articles_new": 3,
            "execution_time_seconds": 12.5,
            "scraped_at": T0,
        }
        data.update(overrides)
        return ScrapeRunResult(**data)

    def test_start_is_end_minus_execution_time(self):
        outcome = AttemptOutcome.from_run(self._run())
        assert outcome.finished_at == T0
        assert outcome.started_at == T0 - timedelta(seconds=12.5)
        assert outcome.success is True
        assert outcome.category is None
        assert outcome.retryable is None
        assert outcome.articles_new == 3

    def test_explicit_start_and_flags(self):
        start = T0 - timedelta(minutes=3)
        outcome = AttemptOutcome.from_run(
            self._run(), started_at=start, explicit_window=True, forced=True
        )
        assert outcome.started_at == start
        assert outcome.explicit_window and outcome.forced

    def test_error_run(self):
        outcome = AttemptOutcome.from_run(
            self._run(
                status="error",
                error_category="html_changed",
                error_message="boom",
                articles_new=None,
                execution_time_seconds=None,
            )
        )
        assert outcome.success is False
        assert outcome.category == "html_changed"
        assert outcome.retryable is False
        assert outcome.error_message == "boom"
        assert outcome.started_at == T0


class TestCircuitGate:
    def test_no_state_runs(self):
        assert circuit_gate(None, T0).action == "run"

    def test_closed_runs(self):
        assert circuit_gate(_state(next_due_at=T0 + timedelta(hours=5)), T0).action == "run"

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_open_not_due_is_skipped(self, circuit):
        due = T0 + timedelta(hours=1)
        decision = circuit_gate(_state(circuit_state=circuit, next_due_at=due), T0)
        assert decision.skip
        assert decision.next_probe_at == due

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_open_and_due_is_a_probe(self, circuit):
        decision = circuit_gate(_state(circuit_state=circuit, next_due_at=T0), T0)
        assert decision.probe
        assert decision.next_probe_at is None

    def test_probe_accepts_half_tick_lookahead(self):
        """O /dispatch despacha o que vence até now + 7 min: a sonda não pode ser pulada."""
        state = _state(circuit_state=CircuitState.OPEN, next_due_at=T0 + timedelta(minutes=7))
        assert circuit_gate(state, T0).probe
        state = _state(circuit_state=CircuitState.OPEN, next_due_at=T0 + timedelta(minutes=8))
        assert circuit_gate(state, T0).skip

    def test_force_ignores_the_circuit(self):
        state = _state(circuit_state=CircuitState.OPEN, next_due_at=T0 + timedelta(hours=20))
        assert circuit_gate(state, T0, force=True).action == "run"
