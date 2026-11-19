"""Atraso e SLO de frescor por agência (painel e /dispatch).

SLO: a agência ativa é raspada com sucesso dentro de 2× o próprio intervalo efetivo. A
noite (23:00–06:00 BRT) não conta: de madrugada quase não há notícia e o despacho é mínimo.
"""

from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

from govbr_scraper.state.model import AgencyState, CircuitState
from govbr_scraper.state.policy import BRT
from govbr_scraper.state.transitions import effective_interval_minutes

NIGHT_START = time(23, 0)
NIGHT_END = time(6, 0)
SLO_INTERVAL_MULTIPLIER = 2


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def lag_minutes(state: AgencyState, now: datetime) -> float | None:
    """Minutos desde a marca d'água (``now − last_success_at``), ou None sem sucesso."""
    if state.last_success_at is None:
        return None
    return (_aware(now) - _aware(state.last_success_at)).total_seconds() / 60


def night_minutes(start: datetime, end: datetime) -> float:
    """Minutos de noite (23:00–06:00 BRT) entre ``start`` e ``end``."""
    start, end = _aware(start), _aware(end)
    if end <= start:
        return 0.0
    total = timedelta()
    day = start.astimezone(BRT).date() - timedelta(days=1)
    last_day = end.astimezone(BRT).date()
    while day <= last_day:
        night_from = datetime.combine(day, NIGHT_START, tzinfo=BRT)
        night_to = datetime.combine(day + timedelta(days=1), NIGHT_END, tzinfo=BRT)
        overlap = min(end, night_to) - max(start, night_from)
        if overlap > timedelta():
            total += overlap
        day += timedelta(days=1)
    return total.total_seconds() / 60


def within_slo(state: AgencyState, now: datetime) -> bool | None:
    """True se o último sucesso está dentro da tolerância do SLO.

    Tolerância = 2 × intervalo efetivo + os minutos de noite entre ``last_success_at`` e
    ``now``. Circuito aberto (open/half_open) devolve None: fica fora do SLO (o alerta do
    disjuntor já cobre). Sem nenhum sucesso registrado: False.
    """
    if state.circuit_state != CircuitState.CLOSED:
        return None
    lag = lag_minutes(state, now)
    if lag is None:
        return False
    tolerance = SLO_INTERVAL_MULTIPLIER * effective_interval_minutes(state, _aware(now))
    tolerance += night_minutes(state.last_success_at, now)
    return lag <= tolerance
