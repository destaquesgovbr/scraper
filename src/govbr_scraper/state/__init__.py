"""Estado por agência da ingestão: marca d'água, disjuntor e ritmo de raspagem.

Ref: scraper/_plan/rearquitetura-ingestao.md (Passo 2). As regras são funções puras
(``transitions``, ``window``, ``slo``); o SQL fica em ``repository``.
"""

from govbr_scraper.state.model import (
    TIER_INTERVAL_MINUTES,
    AgencyState,
    AttemptContext,
    AttemptOutcome,
    CircuitState,
    Tier,
    Transition,
)
from govbr_scraper.state.repository import StateUnavailable
from govbr_scraper.state.transitions import (
    GateDecision,
    apply_attempt,
    circuit_gate,
    effective_interval_minutes,
)
from govbr_scraper.state.window import watermark_window

__all__ = [
    "TIER_INTERVAL_MINUTES",
    "AgencyState",
    "AttemptContext",
    "AttemptOutcome",
    "CircuitState",
    "GateDecision",
    "StateUnavailable",
    "Tier",
    "Transition",
    "apply_attempt",
    "circuit_gate",
    "effective_interval_minutes",
    "watermark_window",
]
