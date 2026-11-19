"""Despacho da raspagem por fila (Passos 3 e 4 da rearquitetura da ingestão).

``POST /dispatch`` (Cloud Scheduler a cada 15 min, DAG ``scrape_backfill``) escolhe as
agências vencidas em ``scrape_agency_state`` e cria uma tarefa por agência na fila
``scrape-govbr`` do Cloud Tasks, que chama ``POST /scrape/agency``. Regras puras em
``selection``, ``tiers`` e ``freshness``; o cliente REST do Cloud Tasks em ``tasks_client``;
o fluxo em ``service``.

Ref: scraper/_plan/rearquitetura-ingestao.md (Passos 3 e 4) e o desenho de 19/11/2026 (§3).
"""

from govbr_scraper.dispatch.config import (
    DispatchConfigError,
    DispatchMode,
    TasksConfig,
    dispatch_mode_from_env,
    effective_mode,
)
from govbr_scraper.dispatch.selection import Selection, select_due

__all__ = [
    "DispatchConfigError",
    "DispatchMode",
    "Selection",
    "TasksConfig",
    "dispatch_mode_from_env",
    "effective_mode",
    "select_due",
]
