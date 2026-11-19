"""Consultas a ``news`` do ``/dispatch``: contagem diária para as faixas e o reconciliador.

Recebem uma conexão psycopg2 e não fazem commit nem rollback (a transação é do chamador).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta

from govbr_scraper.dispatch.tiers import window_bounds

# Reconciliador: notícias sem tema criadas entre now − 7 dias e now − 2 h (antes de 2 h o
# enriquecimento ainda pode estar em andamento), no máximo 20 por dia (liveness do
# enrichment-worker: rajadas de ~40 mensagens derrubam a instância).
RECONCILE_LIMIT = 20
RECONCILE_MAX_AGE = timedelta(days=7)
RECONCILE_MIN_AGE = timedelta(hours=2)

# Mesmo agrupamento do seed da 038: dia BRT de published_at. Os limites vêm prontos (com
# fuso) para a consulta usar o índice idx_news_published_at.
_DAILY_COUNTS_SQL = """
    SELECT agency_key,
           (published_at AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
           COUNT(*)
    FROM news
    WHERE published_at >= %s
      AND published_at < %s
      AND agency_key IS NOT NULL
    GROUP BY agency_key, dia
"""

# A mais recente primeiro: com mais candidatas que o lote, vale mais a notícia nova.
_UNTHEMED_SQL = """
    SELECT unique_id, agency_key, published_at
    FROM news
    WHERE most_specific_theme_id IS NULL
      AND created_at >= %s
      AND created_at < %s
    ORDER BY created_at DESC
    LIMIT %s
"""


def daily_news_counts(conn, today: date) -> dict[str, dict[date, int]]:
    """Notícias por agência e dia BRT nos 28 dias completos anteriores a ``today``."""
    start, end = window_bounds(today)
    counts: dict[str, dict[date, int]] = defaultdict(dict)
    with conn.cursor() as cur:
        cur.execute(_DAILY_COUNTS_SQL, (start, end))
        for agency_key, day, count in cur.fetchall():
            counts[agency_key][day] = int(count)
    return dict(counts)


def unthemed_news(conn, now: datetime, limit: int = RECONCILE_LIMIT) -> list[dict]:
    """Notícias sem tema para republicar, no formato de ``EventPublisher.publish_scraped``.

    ``[{"unique_id", "agency_key", "published_at"}]``, criadas entre now − 7 d e now − 2 h.
    """
    with conn.cursor() as cur:
        cur.execute(_UNTHEMED_SQL, (now - RECONCILE_MAX_AGE, now - RECONCILE_MIN_AGE, limit))
        return [
            {"unique_id": uid, "agency_key": agency_key or "", "published_at": published_at}
            for uid, agency_key, published_at in cur.fetchall()
        ]
