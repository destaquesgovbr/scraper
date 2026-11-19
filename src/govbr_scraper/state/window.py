"""Janela de raspagem pela marca d'água (``last_success_at``), em datas de Brasília."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime, timedelta

from govbr_scraper.state.model import AgencyState
from govbr_scraper.state.policy import BRT, WATERMARK_MAX_ARTICLES, WATERMARK_MAX_DAYS

# A janela começa 1 h antes da marca d'água: só importa perto da meia-noite (a granularidade
# das listagens é o dia), quando a notícia das 23h50 do dia anterior ainda é coberta.
_WATERMARK_MARGIN = timedelta(hours=1)


def _to_brt(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BRT)


def watermark_start(last_success_at: datetime) -> date:
    """Data BRT em que a marca d'água pede para a janela começar, sem nenhum limite."""
    return (_to_brt(last_success_at) - _WATERMARK_MARGIN).date()


def watermark_max_days(rate_ewma_per_day: float | None) -> int:
    """Quantos dias antes de hoje a janela automática pode voltar, pela taxa da agência.

    Sem taxa (agência nova, ou sem notícia nos 28 dias do cálculo): ``WATERMARK_MAX_DAYS``.
    Com taxa: o maior ``n`` em [1, ``WATERMARK_MAX_DAYS``] com
    ``(n + 1) × taxa ≤ WATERMARK_MAX_ARTICLES`` (a janela cobre ``n`` dias inteiros mais
    hoje). O mínimo de 1 é a janela antiga, de ontem a hoje.
    """
    if not rate_ewma_per_day or rate_ewma_per_day <= 0:
        return WATERMARK_MAX_DAYS
    days = math.floor(WATERMARK_MAX_ARTICLES / rate_ewma_per_day) - 1
    return max(1, min(WATERMARK_MAX_DAYS, days))


def watermark_window(
    now: datetime, last_success_at: datetime | None, *, max_days: int = WATERMARK_MAX_DAYS
) -> tuple[date, date]:
    """(início, fim) da janela de uma agência, em datas BRT.

    - ``fim`` = hoje.
    - Sem ``last_success_at`` (agência nova, ou o estado indisponível): ``início`` = ontem.
    - Com ``last_success_at``: ``início`` = (``last_success_at`` em BRT − 1 h).date(),
      limitado a no mínimo hoje − ``max_days`` (padrão 7) e no máximo hoje.

    Datetime sem fuso é tratado como UTC.
    """
    today = _to_brt(now).date()
    if last_success_at is None:
        return today - timedelta(days=1), today
    start = max(watermark_start(last_success_at), today - timedelta(days=max_days))
    return min(start, today), today


def agency_window(now: datetime, state: AgencyState | None) -> tuple[date, date, bool]:
    """Janela automática da agência e se ela foi cortada pelo limite de dias.

    O limite é ``watermark_max_days(state.rate_ewma_per_day)``. Cortada (``True``) quer
    dizer que a marca d'água pedia um início anterior: os dias de fora não são raspados
    sozinhos e, depois do sucesso, a marca d'água passa por cima deles (recuperação com
    datas explícitas, runbook).
    """
    if state is None or state.last_success_at is None:
        start, end = watermark_window(now, None)
        return start, end, False
    start, end = watermark_window(
        now, state.last_success_at, max_days=watermark_max_days(state.rate_ewma_per_day)
    )
    return start, end, watermark_start(state.last_success_at) < start
