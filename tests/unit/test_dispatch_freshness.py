"""Frescor por faixa do /dispatch: percentis, SLO e as agências que ficam de fora."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from govbr_scraper.dispatch.freshness import compute_freshness, percentile
from govbr_scraper.state.model import AgencyState, CircuitState, Tier

NOW = datetime(2026, 11, 19, 15, 0, tzinfo=UTC)  # 12:00 BRT


def _state(key, tier=Tier.H2, lag_min=None, **overrides) -> AgencyState:
    interval = {Tier.M30: 30, Tier.H2: 120, Tier.H6: 360, Tier.D1: 1440}[tier]
    state = AgencyState.new(key, NOW - timedelta(days=3))
    state.tier = tier
    state.poll_interval_minutes = interval
    if lag_min is not None:
        state.last_success_at = NOW - timedelta(minutes=lag_min)
    return replace(state, **overrides)


class TestPercentile:
    def test_empty_is_none(self):
        assert percentile([], 90) is None

    def test_single_value(self):
        assert percentile([7.0], 50) == 7.0

    @pytest.mark.parametrize(
        ("q", "expected"),
        [(0, 1.0), (50, 3.0), (90, 4.6), (100, 5.0), (25, 2.0)],
    )
    def test_linear_interpolation_like_numpy(self, q, expected):
        assert percentile([5.0, 1.0, 3.0, 2.0, 4.0], q) == pytest.approx(expected)


class TestComputeFreshness:
    def test_per_tier_percentiles_and_ratios(self):
        states = [
            _state("agencia_brasil", Tier.M30, lag_min=10),
            _state("pf", Tier.M30, lag_min=50),
            _state("secom", Tier.M30, lag_min=70),  # fora do SLO (> 2 × 30)
            _state("mec", Tier.H2, lag_min=100),
            _state("mds", Tier.H2, lag_min=None),  # sem sucesso: fora do SLO, sem atraso
        ]
        keys = [s.agency_key for s in states]
        fresh = compute_freshness(states, keys, NOW)
        by_tier = {t.tier: t for t in fresh.tiers}
        assert set(by_tier) == {"m30", "h2"}  # faixas sem agência ficam de fora
        m30 = by_tier["m30"]
        assert (m30.agencies, m30.lag_p50_min, m30.lag_max_min) == (3, 50.0, 70.0)
        assert m30.lag_p90_min == pytest.approx(66.0)
        assert m30.tier_slo_ok_ratio == pytest.approx(0.6667)
        h2 = by_tier["h2"]
        assert (h2.agencies, h2.lag_p50_min, h2.lag_p90_min, h2.lag_max_min) == (
            2,
            100.0,
            100.0,
            100.0,
        )
        assert h2.tier_slo_ok_ratio == 0.5
        assert fresh.agencies == 5
        assert fresh.slo_ok_ratio == 0.6  # 3 de 5

    def test_excludes_tvbrasil_open_circuits_and_inactive(self):
        states = [
            _state("tvbrasil", Tier.M30, lag_min=900),
            _state("antaq", Tier.H2, lag_min=3000, circuit_state=CircuitState.OPEN),
            _state("mme", Tier.H2, lag_min=3000, circuit_state=CircuitState.HALF_OPEN),
            _state("sri", Tier.H2, lag_min=5000),  # inativa
            _state("mec", Tier.H2, lag_min=30),
        ]
        fresh = compute_freshness(states, ["tvbrasil", "antaq", "mme", "mec"], NOW)
        (h2,) = fresh.tiers
        assert (h2.tier, h2.agencies, h2.lag_max_min) == ("h2", 1, 30.0)
        assert fresh.slo_ok_ratio == 1.0

    def test_floor_of_the_dag_cadence_only_changes_the_slo(self):
        # Fora de live (piso de 120 min, slo_interval_floor): m30 com 100 min e h2 com boost
        # (intervalo efetivo de 60 min) com 200 min ainda estão dentro de 2 × 120.
        states = [
            _state("agencia_brasil", Tier.M30, lag_min=100),
            _state("mec", Tier.H2, lag_min=200, boost_until=NOW + timedelta(hours=1)),
        ]
        keys = [s.agency_key for s in states]
        no_floor = compute_freshness(states, keys, NOW)
        floor = compute_freshness(states, keys, NOW, min_interval_minutes=120)
        assert no_floor.slo_ok_ratio == 0.0
        assert floor.slo_ok_ratio == 1.0
        assert [t.tier_slo_ok_ratio for t in floor.tiers] == [1.0, 1.0]
        assert [(t.lag_p50_min, t.lag_p90_min) for t in floor.tiers] == [
            (t.lag_p50_min, t.lag_p90_min) for t in no_floor.tiers
        ]

    def test_nothing_eligible(self):
        fresh = compute_freshness([], ["mec"], NOW)
        assert fresh.tiers == []
        assert fresh.slo_ok_ratio is None
        assert fresh.agencies == 0

    def test_tier_without_any_success_has_no_lags(self):
        (d1,) = compute_freshness([_state("x", Tier.D1)], ["x"], NOW).tiers
        assert (d1.lag_p50_min, d1.lag_p90_min, d1.lag_max_min) == (None, None, None)
        assert d1.tier_slo_ok_ratio == 0.0
