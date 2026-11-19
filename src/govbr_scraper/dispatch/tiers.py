"""Recálculo diário das faixas de raspagem pela taxa de notícias (funções puras).

A taxa é a mesma do seed da migration 038 do data-platform:

- notícias de ``news`` por ``agency_key`` e dia BRT de ``published_at``;
- os 28 dias BRT completos anteriores a hoje (``[hoje − 28, hoje)``), com 0 nos dias sem
  notícia;
- ``rate = sum(n_d · w_d) / sum(w_d)``, ``w_d = 0,5 ^ (idade_d / 7)``, idade 0 = ontem.

Faixas: ≥ 5/dia ``m30`` (30 min); ≥ 1 ``h2`` (2 h); ≥ 0,2 ``h6`` (6 h); senão ``d1``
(1× por dia). Histerese de 20%: a agência só sobe de faixa se a taxa passar o limite da faixa
nova × 1,2, e só desce se ficar abaixo do limite da faixa atual × 0,8.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta

from govbr_scraper.state.model import TIER_INTERVAL_MINUTES, AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import BRT, DUE_SLACK
from govbr_scraper.state.transitions import effective_interval_minutes

EWMA_WINDOW_DAYS = 28
EWMA_HALF_LIFE_DAYS = 7

# Limite inferior de cada faixa (notícias/dia), da mais rápida para a mais lenta; abaixo de
# todos, d1.
TIER_THRESHOLDS: tuple[tuple[Tier, float], ...] = (
    (Tier.M30, 5.0),
    (Tier.H2, 1.0),
    (Tier.H6, 0.2),
)
HYSTERESIS = 0.2

# 0 = a mais rápida.
_SPEED = {Tier.M30: 0, Tier.H2: 1, Tier.H6: 2, Tier.D1: 3}


def ewma_weight(age_days: int) -> float:
    """Peso de um dia com ``age_days`` de idade (0 = ontem): meia-vida de 7 dias."""
    return 0.5 ** (age_days / EWMA_HALF_LIFE_DAYS)


_TOTAL_WEIGHT = sum(ewma_weight(age) for age in range(EWMA_WINDOW_DAYS))


def ewma_rate(daily_counts: Mapping[date, int], today: date) -> float:
    """Notícias por dia (EWMA) a partir das contagens por dia BRT.

    Só contam os dias de ``[today − 28, today)``; dia ausente vale 0.
    """
    total = 0.0
    for day, count in daily_counts.items():
        age = (today - timedelta(days=1) - day).days
        if 0 <= age < EWMA_WINDOW_DAYS:
            total += count * ewma_weight(age)
    return total / _TOTAL_WEIGHT


def window_bounds(today: date) -> tuple[datetime, datetime]:
    """Início e fim (exclusivo) da janela da taxa: 00:00 BRT de hoje − 28 e de hoje."""
    start = datetime.combine(today - timedelta(days=EWMA_WINDOW_DAYS), time(0), tzinfo=BRT)
    end = datetime.combine(today, time(0), tzinfo=BRT)
    return start, end


def tier_for_rate(rate: float, factor: float = 1.0) -> Tier:
    """Faixa da taxa com os limites multiplicados por ``factor`` (1 = sem histerese)."""
    for tier, threshold in TIER_THRESHOLDS:
        if rate >= threshold * factor:
            return tier
    return Tier.D1


def tier_with_hysteresis(rate: float, current: Tier | None) -> Tier:
    """Faixa nova com histerese de 20% em relação à atual (``None``: sem histerese).

    Sobe só para a faixa cujo limite × 1,2 a taxa alcança; desce só para a faixa cujo
    limite × 0,8 a taxa ainda alcança. Entre um e outro, fica onde está.
    """
    if current is None:
        return tier_for_rate(rate)
    up = tier_for_rate(rate, 1 + HYSTERESIS)
    if _SPEED[up] < _SPEED[current]:
        return up
    down = tier_for_rate(rate, 1 - HYSTERESIS)
    if _SPEED[down] > _SPEED[current]:
        return down
    return current


def is_faster(new: Tier, old: Tier) -> bool:
    return _SPEED[new] < _SPEED[old]


@dataclass(frozen=True)
class TierUpdate:
    """Faixa recalculada de uma agência.

    ``next_due_cap``: com a faixa mais rápida e o circuito fechado, o ``next_due_at`` passa a
    ser no máximo ``última tentativa (ou agora) + intervalo efetivo novo − 5 min``; senão
    None (o vencimento fica como está e se ajusta na próxima tentativa).
    """

    agency_key: str
    previous_tier: Tier
    tier: Tier
    poll_interval_minutes: int
    rate_ewma_per_day: float
    next_due_cap: datetime | None = None

    @property
    def changed(self) -> bool:
        return self.tier != self.previous_tier


def recompute_tier(state: AgencyState, rate: float, now: datetime) -> TierUpdate:
    """Faixa nova da agência para a taxa ``rate``.

    A histerese vale contra uma faixa já calculada (``tier_updated_at`` preenchido). Uma
    linha que nunca passou pelo cálculo (criada pelo ``ensure_rows`` com o padrão h2) recebe
    a faixa da taxa, sem histerese, como no seed da 038.
    """
    current = state.tier if state.tier_updated_at is not None else None
    tier = tier_with_hysteresis(rate, current)
    interval = TIER_INTERVAL_MINUTES[tier]
    cap = None
    if is_faster(tier, state.tier) and state.circuit_state == CircuitState.CLOSED:
        faster = replace(state, tier=tier, poll_interval_minutes=interval)
        base = state.last_attempt_at or now
        cap = base + timedelta(minutes=effective_interval_minutes(faster, now)) - DUE_SLACK
    return TierUpdate(
        agency_key=state.agency_key,
        previous_tier=state.tier,
        tier=tier,
        poll_interval_minutes=interval,
        rate_ewma_per_day=rate,
        next_due_cap=cap,
    )
