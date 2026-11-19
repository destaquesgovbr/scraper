"""Recálculo das faixas: EWMA igual ao seed da 038, histerese de 20% e vencimento novo."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest

from govbr_scraper.dispatch.tiers import (
    EWMA_WINDOW_DAYS,
    ewma_rate,
    ewma_weight,
    recompute_tier,
    tier_for_rate,
    tier_with_hysteresis,
    window_bounds,
)
from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import BRT

TODAY = date(2026, 11, 19)
NOW = datetime(2026, 11, 19, 6, 5, tzinfo=UTC)  # 03:05 BRT


class TestEwma:
    def test_weights_halve_every_seven_days(self):
        assert ewma_weight(0) == 1.0
        assert ewma_weight(7) == pytest.approx(0.5)
        assert ewma_weight(14) == pytest.approx(0.25)

    def test_constant_rate_gives_the_rate(self):
        counts = {TODAY - timedelta(days=d): 3 for d in range(1, 29)}
        assert ewma_rate(counts, TODAY) == pytest.approx(3.0)

    def test_no_news_is_zero(self):
        assert ewma_rate({}, TODAY) == 0.0

    def test_single_day_uses_the_age_weight_over_all_28_weights(self):
        total = sum(0.5 ** (age / 7) for age in range(28))
        # ontem = idade 0
        assert ewma_rate({TODAY - timedelta(days=1): 10}, TODAY) == pytest.approx(10 / total)
        # 8 dias atrás = idade 7
        assert ewma_rate({TODAY - timedelta(days=8): 10}, TODAY) == pytest.approx(5 / total)

    def test_today_and_days_older_than_28_do_not_count(self):
        counts = {
            TODAY: 100,  # hoje (incompleto) fica fora
            TODAY - timedelta(days=29): 100,  # idade 28: fora da janela
            TODAY - timedelta(days=28): 1,  # idade 27: o último dia
        }
        total = sum(0.5 ** (age / 7) for age in range(28))
        assert ewma_rate(counts, TODAY) == pytest.approx(0.5 ** (27 / 7) / total)

    def test_recent_days_weigh_more(self):
        recent = ewma_rate({TODAY - timedelta(days=1): 7}, TODAY)
        old = ewma_rate({TODAY - timedelta(days=20): 7}, TODAY)
        assert recent > old

    def test_window_bounds_are_brt_midnights(self):
        start, end = window_bounds(TODAY)
        assert start == datetime(2026, 10, 22, 0, 0, tzinfo=BRT)
        assert end == datetime(2026, 11, 19, 0, 0, tzinfo=BRT)
        assert (end - start).days == EWMA_WINDOW_DAYS


class TestTierForRate:
    @pytest.mark.parametrize(
        ("rate", "tier"),
        [
            (40.0, Tier.M30),
            (5.0, Tier.M30),
            (4.99, Tier.H2),
            (1.0, Tier.H2),
            (0.99, Tier.H6),
            (0.2, Tier.H6),
            (0.19, Tier.D1),
            (0.0, Tier.D1),
        ],
    )
    def test_thresholds_like_the_038_seed(self, rate, tier):
        assert tier_for_rate(rate) == tier


class TestHysteresis:
    @pytest.mark.parametrize(
        ("rate", "current", "expected"),
        [
            # sobe só passando o limite × 1,2
            (1.1, Tier.H6, Tier.H6),
            (1.2, Tier.H6, Tier.H2),
            (5.5, Tier.H2, Tier.H2),
            (6.0, Tier.H2, Tier.M30),
            # pula faixas quando a taxa passa o limite × 1,2 da mais rápida alcançável
            (7.0, Tier.D1, Tier.M30),
            (1.1, Tier.D1, Tier.H6),  # h2 exigiria 1,2; h6 exige 0,24
            # desce só abaixo do limite × 0,8
            (0.85, Tier.H2, Tier.H2),
            (0.79, Tier.H2, Tier.H6),
            (4.1, Tier.M30, Tier.M30),
            (3.9, Tier.M30, Tier.H2),
            (0.17, Tier.H6, Tier.H6),
            (0.15, Tier.H6, Tier.D1),
            (0.0, Tier.M30, Tier.D1),
            # dentro da faixa
            (2.5, Tier.H2, Tier.H2),
        ],
    )
    def test_moves_only_beyond_twenty_percent(self, rate, current, expected):
        assert tier_with_hysteresis(rate, current) == expected

    def test_without_current_tier_uses_plain_thresholds(self):
        assert tier_with_hysteresis(1.1, None) == Tier.H2
        assert tier_with_hysteresis(0.85, None) == Tier.H6


def _state(**overrides) -> AgencyState:
    base = AgencyState.new("mec", NOW - timedelta(days=5))
    base.tier_updated_at = NOW - timedelta(days=1)
    return replace(base, **overrides)


class TestRecomputeTier:
    def test_unchanged_tier(self):
        update = recompute_tier(_state(), 2.0, NOW)
        assert (update.tier, update.poll_interval_minutes, update.changed) == (Tier.H2, 120, False)
        assert update.rate_ewma_per_day == 2.0
        assert update.next_due_cap is None

    def test_never_computed_row_has_no_hysteresis(self):
        update = recompute_tier(_state(tier_updated_at=None), 0.85, NOW)
        assert update.tier == Tier.H6
        assert update.changed

    def test_hysteresis_against_computed_tier(self):
        assert recompute_tier(_state(), 0.85, NOW).tier == Tier.H2

    def test_faster_tier_caps_next_due_from_last_attempt(self):
        last = NOW - timedelta(minutes=50)
        state = _state(
            tier=Tier.D1,
            poll_interval_minutes=1440,
            last_attempt_at=last,
            next_due_at=last + timedelta(minutes=1435),
        )
        update = recompute_tier(state, 8.0, NOW)
        assert (update.previous_tier, update.tier, update.poll_interval_minutes) == (
            Tier.D1,
            Tier.M30,
            30,
        )
        assert update.next_due_cap == last + timedelta(minutes=25)

    def test_faster_tier_cap_uses_boost(self):
        last = NOW - timedelta(minutes=10)
        state = _state(
            tier=Tier.H6,
            poll_interval_minutes=360,
            last_attempt_at=last,
            boost_until=NOW + timedelta(hours=2),
        )
        update = recompute_tier(state, 1.5, NOW)  # h6 → h2 (≥ 1,2)
        assert update.tier == Tier.H2
        assert update.next_due_cap == last + timedelta(minutes=60 - 5)  # 120 / 2 − 5

    def test_faster_tier_without_attempt_caps_from_now(self):
        state = _state(tier=Tier.D1, poll_interval_minutes=1440, last_attempt_at=None)
        assert recompute_tier(state, 8.0, NOW).next_due_cap == NOW + timedelta(minutes=25)

    def test_slower_tier_keeps_next_due(self):
        update = recompute_tier(_state(tier=Tier.M30, poll_interval_minutes=30), 0.1, NOW)
        assert update.tier == Tier.D1
        assert update.next_due_cap is None

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_open_circuit_keeps_the_probe_time(self, circuit):
        state = _state(tier=Tier.D1, poll_interval_minutes=1440, circuit_state=circuit)
        update = recompute_tier(state, 8.0, NOW)
        assert update.tier == Tier.M30
        assert update.next_due_cap is None
