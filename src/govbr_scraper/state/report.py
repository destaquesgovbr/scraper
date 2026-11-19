"""Corpo de ``GET /agencies/state`` (painel "Ingestão" do streamlit-infra-health)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import fields
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from govbr_scraper.state.model import AgencyState, CircuitState, Tier
from govbr_scraper.state.policy import DAG_CADENCE_MINUTES
from govbr_scraper.state.slo import lag_minutes, within_slo
from govbr_scraper.state.transitions import effective_interval_minutes


def iso_utc(value: datetime | None) -> str | None:
    """ISO 8601 em UTC com sufixo ``Z`` (datetime sem fuso é tratado como UTC)."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return iso_utc(value)
    return value


def slo_interval_floor(dispatch_mode: str) -> int:
    """Piso do intervalo do SLO: a cadência das DAGs enquanto o /dispatch não está em live.

    No Passo 2 (e com o /dispatch em off ou dry_run) as DAGs de 2 h ainda são o relógio:
    as faixas m30 e o boost só passam a valer na virada para ``live``.
    """
    return 0 if dispatch_mode == "live" else DAG_CADENCE_MINUTES


def agency_entry(
    state: AgencyState, now: datetime, active: bool, slo_floor_minutes: int = 0
) -> dict[str, Any]:
    """Linha da agência: as colunas da tabela + ``active``, intervalo efetivo, atraso e SLO.

    ``slo_floor_minutes``: piso do intervalo no cálculo de ``within_slo``
    (``slo_interval_floor``).
    """
    entry: dict[str, Any] = {"agency_key": state.agency_key, "active": active}
    for f in fields(AgencyState):
        if f.name in ("agency_key", "created_at", "updated_at"):
            continue
        entry[f.name] = _json_value(getattr(state, f.name))
        if f.name == "poll_interval_minutes":
            entry["effective_interval_minutes"] = effective_interval_minutes(state, now)
    lag = lag_minutes(state, now)
    entry["lag_minutes"] = round(lag, 1) if lag is not None else None
    entry["within_slo"] = within_slo(state, now, min_interval_minutes=slo_floor_minutes)
    entry["updated_at"] = iso_utc(state.updated_at)
    return entry


def build_state_report(
    states: Iterable[AgencyState],
    active_keys: Iterable[str],
    now: datetime,
    dispatch_mode: str,
) -> dict[str, Any]:
    """Estado de todas as agências com linha em ``scrape_agency_state`` e o resumo.

    Resumo (só agências ativas no site_urls.yaml/ebc_urls.yaml):
    - ``by_tier``: quantas por faixa;
    - ``circuit_open``: com o circuito aberto (open ou half_open);
    - ``overdue``: com circuito fechado e ``within_slo == False``;
    - ``slo_ok_ratio``: fração das de circuito fechado dentro do SLO (None sem nenhuma).

    Fora do modo ``live`` o SLO usa como piso a cadência das DAGs (``slo_interval_floor``).
    """
    active = set(active_keys)
    floor = slo_interval_floor(dispatch_mode)
    agencies = [agency_entry(s, now, s.agency_key in active, floor) for s in states]
    active_entries = [a for a in agencies if a["active"]]
    closed = [a for a in active_entries if a["circuit_state"] == CircuitState.CLOSED.value]
    ok = sum(1 for a in closed if a["within_slo"] is True)
    by_tier = {tier.value: 0 for tier in Tier}
    for a in active_entries:
        by_tier[a["tier"]] = by_tier.get(a["tier"], 0) + 1
    return {
        "generated_at": iso_utc(now),
        "dispatch_mode": dispatch_mode,
        "agencies": agencies,
        "summary": {
            "total": len(agencies),
            "active": len(active_entries),
            "by_tier": by_tier,
            "circuit_open": len(active_entries) - len(closed),
            "overdue": sum(1 for a in closed if a["within_slo"] is False),
            "slo_ok_ratio": round(ok / len(closed), 4) if closed else None,
        },
    }
