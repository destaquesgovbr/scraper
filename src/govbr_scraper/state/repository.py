"""SQL de ``scrape_agency_state`` (migration 038 do data-platform).

Cada função recebe uma conexão psycopg2 e **não** faz commit nem rollback: a transação é do
chamador. Se a tabela (ou uma coluna) não existir, a função levanta ``StateUnavailable`` e a
transação fica abortada; o chamador desfaz (``ROLLBACK`` ou ``ROLLBACK TO SAVEPOINT``).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import fields
from datetime import datetime
from typing import Any

from psycopg2 import errors

from govbr_scraper.state.model import AgencyState, CircuitState, Tier

TABLE = "scrape_agency_state"

# Colunas na ordem do dataclass (os nomes são os mesmos da tabela).
COLUMNS: tuple[str, ...] = tuple(f.name for f in fields(AgencyState))
_COLUMN_LIST = ", ".join(COLUMNS)
_UPDATABLE = tuple(c for c in COLUMNS if c not in ("agency_key", "created_at"))


class StateUnavailable(RuntimeError):
    """``scrape_agency_state`` indisponível: a migration 038 não foi aplicada.

    Embrulha o ``UndefinedTable`` (ou ``UndefinedColumn``) do psycopg2 em ``__cause__``.
    """


def _execute(conn, sql: str, params: Any = None):
    """Executa e devolve o cursor; tabela ou coluna ausente vira StateUnavailable."""
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
    except (errors.UndefinedTable, errors.UndefinedColumn) as e:
        cur.close()
        raise StateUnavailable(f"{TABLE} indisponível: {e}".strip()) from e
    except Exception:
        cur.close()
        raise
    return cur


def _row_to_state(row: tuple) -> AgencyState:
    values = dict(zip(COLUMNS, row, strict=True))
    values["tier"] = Tier(values["tier"])
    values["circuit_state"] = CircuitState(values["circuit_state"])
    return AgencyState(**values)


def _state_params(state: AgencyState) -> list[Any]:
    params = []
    for column in COLUMNS:
        value = getattr(state, column)
        if isinstance(value, Tier | CircuitState):
            value = value.value
        params.append(value)
    return params


def get_state(conn, agency_key: str, for_update: bool = False) -> AgencyState | None:
    """Estado de uma agência, ou None se não há linha. ``for_update`` trava a linha."""
    sql = f"SELECT {_COLUMN_LIST} FROM {TABLE} WHERE agency_key = %s"  # nosec B608
    if for_update:
        sql += " FOR UPDATE"
    cur = _execute(conn, sql, (agency_key,))
    try:
        row = cur.fetchone()
    finally:
        cur.close()
    return _row_to_state(row) if row is not None else None


def list_states(conn) -> list[AgencyState]:
    """Todas as linhas, ordenadas por ``agency_key``."""
    sql = f"SELECT {_COLUMN_LIST} FROM {TABLE} ORDER BY agency_key"  # nosec B608
    cur = _execute(conn, sql)
    try:
        return [_row_to_state(row) for row in cur.fetchall()]
    finally:
        cur.close()


def upsert_state(conn, state: AgencyState) -> None:
    """Grava a linha inteira (``INSERT … ON CONFLICT (agency_key) DO UPDATE``).

    ``created_at`` da linha existente é preservado.
    """
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    assignments = ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATABLE)
    params = _state_params(state)
    if state.created_at is None:
        params[COLUMNS.index("created_at")] = state.updated_at
    sql = (
        f"INSERT INTO {TABLE} ({_COLUMN_LIST}) VALUES ({placeholders}) "  # nosec B608
        f"ON CONFLICT (agency_key) DO UPDATE SET {assignments}"
    )
    _execute(conn, sql, params).close()


def ensure_rows(conn, agency_keys: Iterable[str], now: datetime) -> int:
    """Cria a linha padrão (h2, vencida em ``now``) das agências que ainda não têm.

    Devolve quantas linhas foram criadas. ``INSERT … ON CONFLICT DO NOTHING``: idempotente.
    """
    keys = sorted(set(agency_keys))
    if not keys:
        return 0
    sql = (
        f"INSERT INTO {TABLE} (agency_key, next_due_at, created_at, updated_at) "  # nosec B608
        "SELECT k, %s, %s, %s FROM unnest(%s::text[]) AS k "
        "ON CONFLICT (agency_key) DO NOTHING"
    )
    cur = _execute(conn, sql, (now, now, now, keys))
    try:
        return max(cur.rowcount, 0)
    finally:
        cur.close()


def mark_probe(conn, agency_key: str, now: datetime) -> bool:
    """Põe o circuito aberto em ``half_open`` (a sonda vai rodar). False se não estava aberto."""
    sql = (
        f"UPDATE {TABLE} SET circuit_state = 'half_open', updated_at = %s "  # nosec B608
        "WHERE agency_key = %s AND circuit_state IN ('open', 'half_open')"
    )
    cur = _execute(conn, sql, (now, agency_key))
    try:
        return cur.rowcount > 0
    finally:
        cur.close()


def mark_dispatched(conn, agency_keys: Iterable[str], now: datetime) -> int:
    """Registra o despacho (``last_dispatched_at``) das agências; devolve quantas mudaram."""
    keys = sorted(set(agency_keys))
    if not keys:
        return 0
    sql = (
        f"UPDATE {TABLE} SET last_dispatched_at = %s, updated_at = %s "  # nosec B608
        "WHERE agency_key = ANY(%s)"
    )
    cur = _execute(conn, sql, (now, now, keys))
    try:
        return max(cur.rowcount, 0)
    finally:
        cur.close()
