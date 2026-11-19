"""Seleção das agências vencidas a cada tick do ``/dispatch`` (função pura, sem banco).

Regras (desenho dos Passos 2–4, §3.3):

- **Modo noturno:** das 23:00 às 05:59 BRT o tick só despacha na passada das 03:00 (o tick
  que cai entre 03:00 e 03:14); nos outros ticks da noite, nada é selecionado.
- **Vencida:** ``next_due_at <= now + DUE_LOOKAHEAD`` (meio tick: 7 min).
- **Em voo:** despachada há menos de 30 min e ainda sem tentativa depois do despacho
  (``last_dispatched_at > coalesce(last_attempt_at, -inf)``): fica fora, para não criar uma
  segunda tarefa enquanto a primeira espera na fila.
- **Disjuntor:** circuito fechado, ou aberto com a sonda já vencida (a tarefa é a sonda).
- **Ordem e teto:** por ``next_due_at`` (a mais atrasada primeiro), no máximo 200 por tick.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, time, timedelta

from govbr_scraper.state.model import AgencyState
from govbr_scraper.state.policy import BRT, DUE_LOOKAHEAD
from govbr_scraper.state.transitions import circuit_gate

# Noite: [23:00, 06:00) BRT. Só a passada das 03:00 despacha.
NIGHT_START_HOUR = 23
NIGHT_END_HOUR = 6
NIGHT_PASS_START = time(3, 0)
NIGHT_PASS_END = time(3, 15)  # exclusivo: o tick das 03:00 (o Scheduler é */15)

# Despachada há menos disso e sem tentativa depois: em voo.
INFLIGHT_WINDOW = timedelta(minutes=30)

# Teto de tarefas por tick.
MAX_DISPATCH_PER_TICK = 200


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def is_night(now: datetime) -> bool:
    """True das 23:00 às 05:59 BRT."""
    hour = _aware(now).astimezone(BRT).hour
    return hour >= NIGHT_START_HOUR or hour < NIGHT_END_HOUR


def is_night_pass(now: datetime) -> bool:
    """True no tick da passada noturna (entre 03:00 e 03:14 BRT)."""
    local = _aware(now).astimezone(BRT).time()
    return NIGHT_PASS_START <= local < NIGHT_PASS_END


def is_night_skip(now: datetime) -> bool:
    """True num tick noturno que não despacha (noite, fora da passada das 03:00)."""
    return is_night(now) and not is_night_pass(now)


def in_flight(state: AgencyState, now: datetime) -> bool:
    """Despachada há menos de 30 min e sem tentativa iniciada depois do despacho."""
    dispatched = state.last_dispatched_at
    if dispatched is None:
        return False
    dispatched = _aware(dispatched)
    if dispatched <= _aware(now) - INFLIGHT_WINDOW:
        return False
    attempt = state.last_attempt_at
    return attempt is None or dispatched > _aware(attempt)


def is_due(state: AgencyState, now: datetime) -> bool:
    """Vence até ``now + DUE_LOOKAHEAD`` (a mesma tolerância da sonda em ``circuit_gate``)."""
    return _aware(state.next_due_at) <= _aware(now) + DUE_LOOKAHEAD


@dataclass(frozen=True)
class DueAgency:
    """Agência selecionada: o estado e se a tarefa é a sonda de um circuito aberto."""

    state: AgencyState
    probe: bool

    @property
    def agency_key(self) -> str:
        return self.state.agency_key


@dataclass(frozen=True)
class Selection:
    """Resultado de ``select_due``.

    - ``due``: as selecionadas, em ordem, até o teto;
    - ``skipped_inflight``: vencidas, mas em voo;
    - ``overflow``: vencidas que passaram do teto (ficam para o próximo tick);
    - ``night_skip``: tick noturno fora da passada das 03:00 (nada selecionado).
    """

    due: list[DueAgency] = field(default_factory=list)
    skipped_inflight: list[str] = field(default_factory=list)
    overflow: list[str] = field(default_factory=list)
    night_skip: bool = False

    @property
    def agency_keys(self) -> list[str]:
        return [d.agency_key for d in self.due]


def select_due(
    states: Iterable[AgencyState],
    active_keys: Iterable[str],
    now: datetime,
    *,
    limit: int = MAX_DISPATCH_PER_TICK,
) -> Selection:
    """Agências ativas a despachar neste tick, ordenadas por ``next_due_at``.

    Agência ativa sem linha em ``states`` entra como nova (``AgencyState.new``: vencida
    agora). Linha de agência inativa é ignorada.
    """
    now = _aware(now)
    if is_night_skip(now):
        return Selection(night_skip=True)

    by_key = {s.agency_key: s for s in states}
    candidates: list[DueAgency] = []
    inflight: list[str] = []
    for key in sorted(set(active_keys)):
        state = by_key.get(key) or AgencyState.new(key, now)
        if not is_due(state, now):
            continue
        gate = circuit_gate(state, now)
        if gate.skip:  # não acontece com a mesma tolerância; fica explícito
            continue
        if in_flight(state, now):
            inflight.append(key)
            continue
        candidates.append(DueAgency(state=state, probe=gate.probe))

    candidates.sort(key=lambda d: (_aware(d.state.next_due_at), d.agency_key))
    return Selection(
        due=candidates[:limit],
        skipped_inflight=inflight,
        overflow=[d.agency_key for d in candidates[limit:]],
    )
