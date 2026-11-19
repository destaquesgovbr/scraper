"""Janela pela marca d'água (watermark_window) e SLO de frescor (lag_minutes, within_slo)."""

from dataclasses import replace
from datetime import UTC, date, datetime

import pytest

from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import BRT
from govbr_scraper.state.slo import lag_minutes, night_minutes, within_slo
from govbr_scraper.state.window import watermark_window


def _brt(day, hour, minute=0, month=11):
    return datetime(2026, month, day, hour, minute, tzinfo=BRT)


class TestWatermarkWindow:
    def test_without_history_is_yesterday_to_today(self):
        assert watermark_window(_brt(19, 10), None) == (date(2026, 11, 18), date(2026, 11, 19))

    def test_same_day_watermark(self):
        assert watermark_window(_brt(19, 10), _brt(19, 8)) == (
            date(2026, 11, 19),
            date(2026, 11, 19),
        )

    def test_one_hour_margin_crosses_midnight(self):
        """Sucesso às 00:30 BRT: a janela volta ao dia anterior (23:30)."""
        assert watermark_window(_brt(19, 10), _brt(19, 0, 30))[0] == date(2026, 11, 18)
        assert watermark_window(_brt(19, 10), _brt(19, 1, 0))[0] == date(2026, 11, 19)

    def test_day_turn_uses_brt_not_utc(self):
        """22:30 BRT de 18/11 já é 19/11 em UTC: a janela começa em 18/11 (BRT)."""
        last = datetime(2026, 11, 19, 1, 30, tzinfo=UTC)  # 22:30 BRT de 18/11
        now = datetime(2026, 11, 19, 2, 30, tzinfo=UTC)  # 23:30 BRT de 18/11
        assert watermark_window(now, last) == (date(2026, 11, 18), date(2026, 11, 18))

    def test_limited_to_seven_days(self):
        """Caso mec: último sucesso em 28/10; a janela para em hoje − 7."""
        assert watermark_window(_brt(19, 10), _brt(28, 15, month=10)) == (
            date(2026, 11, 12),
            date(2026, 11, 19),
        )

    def test_exactly_seven_days_is_kept(self):
        assert watermark_window(_brt(19, 10), _brt(12, 12))[0] == date(2026, 11, 12)

    def test_future_watermark_is_clamped_to_today(self):
        assert watermark_window(_brt(19, 10), _brt(21, 10)) == (
            date(2026, 11, 19),
            date(2026, 11, 19),
        )

    def test_naive_datetimes_are_utc(self):
        now = datetime(2026, 11, 19, 2, 30)  # 23:30 BRT de 18/11
        assert watermark_window(now, None) == (date(2026, 11, 17), date(2026, 11, 18))


def _state(**overrides) -> AgencyState:
    return replace(AgencyState.new("mec", _brt(19, 10)), **overrides)


class TestNightMinutes:
    def test_daytime_has_no_night(self):
        assert night_minutes(_brt(19, 8), _brt(19, 20)) == 0

    def test_full_night(self):
        assert night_minutes(_brt(18, 22), _brt(19, 7)) == 7 * 60

    def test_partial_overlaps(self):
        assert night_minutes(_brt(18, 23, 30), _brt(19, 1)) == 90
        assert night_minutes(_brt(19, 5), _brt(19, 9)) == 60

    def test_several_nights(self):
        assert night_minutes(_brt(17, 12), _brt(19, 12)) == 2 * 7 * 60

    def test_reversed_interval_is_zero(self):
        assert night_minutes(_brt(19, 12), _brt(19, 1)) == 0


class TestLagAndSlo:
    def test_lag_minutes(self):
        state = _state(last_success_at=_brt(19, 9, 30))
        assert lag_minutes(state, _brt(19, 10)) == pytest.approx(30)
        assert lag_minutes(_state(), _brt(19, 10)) is None

    def test_within_twice_the_interval(self):
        state = _state(last_success_at=_brt(19, 8))  # h2: 2 × 120 = 240 min
        assert within_slo(state, _brt(19, 12)) is True
        assert within_slo(state, _brt(19, 12, 1)) is False

    def test_uses_effective_interval_with_boost(self):
        state = _state(last_success_at=_brt(19, 8), boost_until=_brt(19, 14))
        assert within_slo(state, _brt(19, 10)) is True  # 2 × 60
        assert within_slo(state, _brt(19, 10, 1)) is False

    def test_night_does_not_count(self):
        """m30 com último sucesso às 22:30: às 06:30 o atraso fora da noite é 60 min."""
        state = _state(tier=Tier.M30, poll_interval_minutes=30, last_success_at=_brt(18, 22, 30))
        assert within_slo(state, _brt(19, 6, 30)) is True
        assert within_slo(state, _brt(19, 6, 31)) is False

    @pytest.mark.parametrize("circuit", [CircuitState.OPEN, CircuitState.HALF_OPEN])
    def test_open_circuit_is_out_of_slo(self, circuit):
        state = _state(circuit_state=circuit, last_success_at=_brt(19, 9))
        assert within_slo(state, _brt(19, 10)) is None

    def test_never_succeeded_is_not_within(self):
        assert within_slo(_state(), _brt(19, 10)) is False
