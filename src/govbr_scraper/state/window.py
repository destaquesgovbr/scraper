"""Janela de raspagem pela marca d'água (``last_success_at``), em datas de Brasília."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from govbr_scraper.state.policy import BRT, WATERMARK_MAX_DAYS

# A janela começa 1 h antes da marca d'água: só importa perto da meia-noite (a granularidade
# das listagens é o dia), quando a notícia das 23h50 do dia anterior ainda é coberta.
_WATERMARK_MARGIN = timedelta(hours=1)


def _to_brt(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(BRT)


def watermark_window(now: datetime, last_success_at: datetime | None) -> tuple[date, date]:
    """(início, fim) da janela de uma agência, em datas BRT.

    - ``fim`` = hoje.
    - Sem ``last_success_at`` (agência nova, ou o estado indisponível): ``início`` = ontem.
    - Com ``last_success_at``: ``início`` = (``last_success_at`` em BRT − 1 h).date(),
      limitado a no mínimo hoje − 7 dias e no máximo hoje.

    Datetime sem fuso é tratado como UTC.
    """
    today = _to_brt(now).date()
    if last_success_at is None:
        return today - timedelta(days=1), today
    start = (_to_brt(last_success_at) - _WATERMARK_MARGIN).date()
    start = max(start, today - timedelta(days=WATERMARK_MAX_DAYS))
    return min(start, today), today
