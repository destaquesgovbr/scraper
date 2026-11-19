"""Frescor da ingestão por faixa, calculado pelo ``/dispatch`` a cada tick diurno (puro).

Entram as agências ativas com o circuito fechado, sem a ``tvbrasil``. Por faixa: p50, p90 e
máximo do atraso desde o último sucesso (``lag_minutes``), quantas agências e a fração dentro
do SLO; mais a fração geral dentro do SLO. O evento ``freshness`` (um por faixa) alimenta a
métrica ``scraper_freshness_lag_p90`` do infra.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime

from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.slo import lag_minutes, within_slo

# Fora do frescor: o published_at da TV Brasil é o horário da grade, não o da publicação
# (só 6 horários distintos em 7 dias), e a medida dela é ruído na faixa m30.
FRESHNESS_EXCLUDED = frozenset({"tvbrasil"})


def percentile(values: Sequence[float], q: float) -> float | None:
    """Percentil ``q`` (0–100) com interpolação linear (o padrão do numpy); None se vazio."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (len(ordered) - 1) * q / 100
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return float(ordered[low])
    return ordered[low] + (ordered[high] - ordered[low]) * (rank - low)


def _round(value: float | None) -> float | None:
    return round(value, 1) if value is not None else None


@dataclass(frozen=True)
class TierFreshness:
    """Frescor de uma faixa (campos do evento ``freshness``)."""

    tier: str
    agencies: int
    lag_p50_min: float | None
    lag_p90_min: float | None
    lag_max_min: float | None
    tier_slo_ok_ratio: float | None


@dataclass(frozen=True)
class Freshness:
    """Frescor de todas as faixas com agências e a fração geral dentro do SLO."""

    tiers: list[TierFreshness]
    slo_ok_ratio: float | None
    agencies: int


def compute_freshness(
    states: Iterable[AgencyState],
    active_keys: Iterable[str],
    now: datetime,
    *,
    min_interval_minutes: int = 0,
) -> Freshness:
    """Frescor por faixa das agências ativas de circuito fechado, sem a ``tvbrasil``.

    Agência sem nenhum sucesso conta em ``agencies`` e fora do SLO, mas não entra nos
    percentis (não tem atraso). Faixa sem agência fica de fora.

    ``min_interval_minutes``: piso do intervalo no SLO (``slo_interval_floor`` do modo), o
    mesmo do ``GET /agencies/state``. Fora de ``live`` as DAGs de 2 h ainda são o relógio, e
    sem o piso as m30 e as agências com boost contariam como fora do SLO sem estar. Os
    percentis de atraso não mudam.
    """
    active = set(active_keys) - FRESHNESS_EXCLUDED
    eligible = [
        s for s in states if s.agency_key in active and s.circuit_state == CircuitState.CLOSED
    ]
    tiers: list[TierFreshness] = []
    ok_total = 0
    for tier in Tier:
        group = [s for s in eligible if s.tier == tier]
        if not group:
            continue
        lags = [lag for s in group if (lag := lag_minutes(s, now)) is not None]
        ok = sum(
            1
            for s in group
            if within_slo(s, now, min_interval_minutes=min_interval_minutes) is True
        )
        ok_total += ok
        tiers.append(
            TierFreshness(
                tier=tier.value,
                agencies=len(group),
                lag_p50_min=_round(percentile(lags, 50)),
                lag_p90_min=_round(percentile(lags, 90)),
                lag_max_min=_round(max(lags) if lags else None),
                tier_slo_ok_ratio=round(ok / len(group), 4),
            )
        )
    ratio = round(ok_total / len(eligible), 4) if eligible else None
    return Freshness(tiers=tiers, slo_ok_ratio=ratio, agencies=len(eligible))
