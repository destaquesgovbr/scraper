"""Modelos do estado por agência da ingestão (tabela ``scrape_agency_state``, migration 038).

Só dados, sem E/S: as regras ficam em ``transitions.py``, ``window.py`` e ``slo.py``; o SQL,
em ``repository.py``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any


class Tier(str, Enum):
    """Faixa de raspagem conforme o ritmo de publicação da agência."""

    M30 = "m30"
    H2 = "h2"
    H6 = "h6"
    D1 = "d1"


TIER_INTERVAL_MINUTES: dict[Tier, int] = {Tier.M30: 30, Tier.H2: 120, Tier.H6: 360, Tier.D1: 1440}


class CircuitState(str, Enum):
    """Estado do disjuntor da agência."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# Motivos de uma transição do disjuntor (Transition.reason).
REASON_PERMANENT_ERRORS = "permanent_errors"
REASON_TRANSIENT_24H = "transient_24h"
REASON_PROBE_SUCCESS = "probe_success"
REASON_FORCED_SUCCESS = "forced_success"


@dataclass
class AgencyState:
    """Uma linha de ``scrape_agency_state``: os mesmos campos, com os mesmos nomes."""

    agency_key: str
    next_due_at: datetime
    tier: Tier = Tier.H2
    poll_interval_minutes: int = TIER_INTERVAL_MINUTES[Tier.H2]
    rate_ewma_per_day: float | None = None
    tier_updated_at: datetime | None = None
    boost_until: datetime | None = None
    last_dispatched_at: datetime | None = None
    # Início da última tentativa.
    last_attempt_at: datetime | None = None
    # Marca d'água: início da última tentativa bem-sucedida que cobriu até hoje (BRT).
    last_success_at: datetime | None = None
    last_new_article_at: datetime | None = None
    consecutive_permanent_failures: int = 0
    consecutive_transient_failures: int = 0
    # Início da sequência atual de falhas (qualquer tipo); NULL depois de um sucesso.
    failure_streak_started_at: datetime | None = None
    circuit_state: CircuitState = CircuitState.CLOSED
    circuit_opened_at: datetime | None = None
    circuit_failed_probes: int = 0
    last_error_category: str | None = None
    # Truncado a 500 caracteres.
    last_error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def new(cls, agency_key: str, now: datetime) -> AgencyState:
        """Estado padrão de uma agência sem linha: faixa h2 (120 min), vencida agora."""
        return cls(agency_key=agency_key, next_due_at=now, created_at=now, updated_at=now)

    @property
    def circuit_closed(self) -> bool:
        return self.circuit_state == CircuitState.CLOSED


@dataclass(frozen=True)
class AttemptOutcome:
    """Resultado de uma tentativa de raspagem de uma agência, a entrada de ``apply_attempt``.

    ``explicit_window`` é True quando a requisição trouxe ``start_date``/``end_date``
    (backfill ou recuperação): a tentativa não mexe no estado. ``forced`` é True quando a
    requisição trouxe ``force: true`` (ignora o disjuntor): um sucesso que fecha o circuito
    leva o motivo ``forced_success``.
    """

    agency_key: str
    started_at: datetime
    finished_at: datetime
    success: bool
    category: str | None = None
    retryable: bool | None = None
    articles_new: int | None = None
    error_message: str | None = None
    explicit_window: bool = False
    forced: bool = False

    @classmethod
    def from_run(
        cls,
        run: Any,
        *,
        started_at: datetime | None = None,
        explicit_window: bool = False,
        forced: bool = False,
    ) -> AttemptOutcome:
        """Monta o resultado a partir de um ``ScrapeRunResult``.

        ``scraped_at`` é o fim da raspagem; sem ``started_at`` explícito, o início é
        ``scraped_at − execution_time_seconds``.
        """
        from govbr_scraper.models.monitoring import is_retryable

        finished_at = run.scraped_at
        if started_at is None:
            started_at = finished_at - timedelta(seconds=run.execution_time_seconds or 0)
        success = run.status == "success"
        category = None if success or run.error_category is None else str(run.error_category)
        return cls(
            agency_key=run.agency_key,
            started_at=started_at,
            finished_at=finished_at,
            success=success,
            category=category,
            retryable=None if success else is_retryable(category),
            articles_new=run.articles_new,
            error_message=None if success else run.error_message,
            explicit_window=explicit_window,
            forced=forced,
        )


@dataclass(frozen=True)
class Transition:
    """Mudança do disjuntor que gera alerta: ``closed → open`` ou ``open/half_open → closed``."""

    agency_key: str
    from_state: CircuitState
    to_state: CircuitState
    reason: str
    category: str | None
    at: datetime


@dataclass(frozen=True)
class AttemptContext:
    """Contexto de uma requisição de raspagem, aplicado a todas as agências dela."""

    explicit_window: bool = False
    forced: bool = False
