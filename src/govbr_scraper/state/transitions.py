"""Regras do estado por agência: intervalo efetivo, disjuntor e próxima raspagem.

Funções puras (sem banco, sem relógio): ``now`` vem sempre de fora.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from govbr_scraper.models.monitoring import PERMANENT_ERROR_CATEGORIES
from govbr_scraper.state.model import (
    REASON_FORCED_SUCCESS,
    REASON_PERMANENT_ERRORS,
    REASON_PROBE_SUCCESS,
    REASON_TRANSIENT_24H,
    AgencyState,
    AttemptOutcome,
    CircuitState,
    Transition,
)
from govbr_scraper.state.policy import (
    BOOST_DURATION,
    BOOST_TIERS,
    DUE_LOOKAHEAD,
    DUE_SLACK,
    ERROR_MESSAGE_MAX_CHARS,
    PERMANENT_FAILURES_TO_OPEN,
    PROBE_DELAYS,
    TRANSIENT_STREAK_TO_OPEN,
)

_PERMANENT = frozenset(str(c) for c in PERMANENT_ERROR_CATEGORIES)


def is_permanent(category: str | None) -> bool:
    """Erro permanente para o disjuntor: só ``PERMANENT_ERROR_CATEGORIES``.

    O resto, incluindo ``unknown`` e erro sem categoria, é passageiro.
    """
    return category is not None and str(category) in _PERMANENT


def effective_interval_minutes(state: AgencyState, now: datetime) -> int:
    """Intervalo da agência, pela metade enquanto ``boost_until > now`` nas faixas
    ``BOOST_TIERS`` (h2, h6, d1); a m30 fica no piso de 30 min."""
    interval = state.poll_interval_minutes
    if state.boost_until is not None and state.boost_until > now and state.tier in BOOST_TIERS:
        return max(1, interval // 2)
    return interval


def _next_regular_due(state: AgencyState, started_at: datetime, now: datetime) -> datetime:
    return started_at + timedelta(minutes=effective_interval_minutes(state, now)) - DUE_SLACK


def _probe_delay(failed_probes: int) -> timedelta:
    return PROBE_DELAYS[min(failed_probes, len(PROBE_DELAYS) - 1)]


def apply_attempt(
    prev: AgencyState, outcome: AttemptOutcome, now: datetime
) -> tuple[AgencyState, Transition | None]:
    """Novo estado da agência depois de uma tentativa, e a transição do disjuntor (se houve).

    - Janela explícita (backfill/recuperação): só ``updated_at`` muda, sem transição.
    - Sucesso: zera os contadores, avança a marca d'água (``last_success_at`` = início da
      tentativa), encurta o intervalo por 6 h se achou notícia nova e fecha o circuito
      aberto (transição ``probe_success``, ou ``forced_success`` com ``force``).
    - Falha: conta permanente ou passageira; com o circuito fechado, abre com 2 permanentes
      desde o último sucesso ou com 24 h de falhas seguidas; com o circuito aberto (a sonda
      falhou), adia a próxima sonda (2 h → 6 h → 24 h), sem transição.
    """
    state = replace(prev, updated_at=now)
    if outcome.explicit_window:
        return state, None

    started = outcome.started_at
    finished = outcome.finished_at
    state.last_attempt_at = started

    if outcome.success:
        state.consecutive_permanent_failures = 0
        state.consecutive_transient_failures = 0
        state.failure_streak_started_at = None
        state.last_success_at = started
        if (outcome.articles_new or 0) > 0:
            state.last_new_article_at = finished
            state.boost_until = finished + BOOST_DURATION
        transition = None
        if prev.circuit_state != CircuitState.CLOSED:
            state.circuit_state = CircuitState.CLOSED
            state.circuit_failed_probes = 0
            state.circuit_opened_at = None
            transition = Transition(
                agency_key=prev.agency_key,
                from_state=prev.circuit_state,
                to_state=CircuitState.CLOSED,
                reason=REASON_FORCED_SUCCESS if outcome.forced else REASON_PROBE_SUCCESS,
                category=prev.last_error_category,
                at=now,
            )
        state.next_due_at = _next_regular_due(state, started, now)
        return state, transition

    if state.failure_streak_started_at is None:
        state.failure_streak_started_at = started
    state.last_error_category = outcome.category
    message = outcome.error_message
    state.last_error_message = message[:ERROR_MESSAGE_MAX_CHARS] if message else None
    if is_permanent(outcome.category):
        state.consecutive_permanent_failures += 1
    else:
        state.consecutive_transient_failures += 1

    if prev.circuit_state == CircuitState.CLOSED:
        reason = None
        if state.consecutive_permanent_failures >= PERMANENT_FAILURES_TO_OPEN:
            reason = REASON_PERMANENT_ERRORS
        elif started - state.failure_streak_started_at >= TRANSIENT_STREAK_TO_OPEN:
            reason = REASON_TRANSIENT_24H
        if reason is None:
            state.next_due_at = _next_regular_due(state, started, now)
            return state, None
        state.circuit_state = CircuitState.OPEN
        state.circuit_opened_at = finished
        state.circuit_failed_probes = 0
        state.next_due_at = finished + PROBE_DELAYS[0]
        return state, Transition(
            agency_key=prev.agency_key,
            from_state=CircuitState.CLOSED,
            to_state=CircuitState.OPEN,
            reason=reason,
            category=outcome.category,
            at=now,
        )

    # Circuito open ou half_open: a sonda falhou. Fica aberto, sem transição nem alerta.
    state.circuit_state = CircuitState.OPEN
    state.circuit_failed_probes += 1
    state.next_due_at = finished + _probe_delay(state.circuit_failed_probes)
    return state, None


# Ações do disjuntor antes de raspar (circuit_gate).
GATE_RUN = "run"
GATE_PROBE = "probe"
GATE_SKIP = "skip"


@dataclass(frozen=True)
class GateDecision:
    """O que fazer com a agência antes de raspar: ``run``, ``probe`` ou ``skip``.

    ``next_probe_at`` só vem preenchido no ``skip``.
    """

    action: str
    next_probe_at: datetime | None = None

    @property
    def skip(self) -> bool:
        return self.action == GATE_SKIP

    @property
    def probe(self) -> bool:
        return self.action == GATE_PROBE


def circuit_gate(state: AgencyState | None, now: datetime, *, force: bool = False) -> GateDecision:
    """Decide se a agência é raspada, sondada ou pulada pelo disjuntor.

    - Sem estado (agência nova ou tabela indisponível), circuito fechado ou ``force``: raspa.
    - Circuito aberto (open/half_open) que vence até ``now + DUE_LOOKAHEAD``: é a sonda.
    - Circuito aberto que ainda não venceu: pula, sem raspar nem gravar scrape_runs.
    """
    if state is None or force or state.circuit_state == CircuitState.CLOSED:
        return GateDecision(GATE_RUN)
    if state.next_due_at <= now + DUE_LOOKAHEAD:
        return GateDecision(GATE_PROBE)
    return GateDecision(GATE_SKIP, next_probe_at=state.next_due_at)
