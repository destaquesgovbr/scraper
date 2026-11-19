"""SQL de ``scrape_agency_state`` e ``scrape_dispatch_meta`` (migration 038 do data-platform).

Cada função recebe uma conexão psycopg2 e **não** faz commit nem rollback: a transação é do
chamador. Se a tabela (ou uma coluna) não existir, a função levanta ``StateUnavailable`` e a
transação fica abortada; o chamador desfaz (``ROLLBACK`` ou ``ROLLBACK TO SAVEPOINT``).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import fields
from datetime import date, datetime
from typing import Any

from psycopg2 import errors
from psycopg2.extras import Json, execute_values

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


# =============================================================================
# /dispatch: recálculo das faixas e chave-valor do despachante (scrape_dispatch_meta)
# =============================================================================

META_TABLE = "scrape_dispatch_meta"


def claim_daily(conn, key: str, day: date, now: datetime) -> bool:
    """Reserva a tarefa diária ``key`` para o dia BRT ``day``; True só para quem reservou.

    ``INSERT … ON CONFLICT (key) DO UPDATE … WHERE`` a data gravada é outra, ``RETURNING``:
    devolve a linha só se gravou o dia novo. Dois ticks concorrentes ficam em série no lock
    da linha (ou do índice, na primeira vez): o segundo espera o commit do primeiro, reavalia
    a condição com a data já gravada e recebe False. Se o primeiro desfizer (ROLLBACK), o
    segundo reserva. A reserva vale só depois do commit do chamador.
    """
    value = Json({"date": day.isoformat(), "claimed_at": now.isoformat()})
    sql = (
        f"INSERT INTO {META_TABLE} (key, value, updated_at) VALUES (%s, %s, %s) "  # nosec B608
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at "
        f"WHERE {META_TABLE}.value->>'date' IS DISTINCT FROM EXCLUDED.value->>'date' "
        "RETURNING key"
    )
    cur = _execute(conn, sql, (key, value, now))
    try:
        return cur.fetchone() is not None
    finally:
        cur.close()


def get_meta(conn, key: str) -> dict[str, Any] | None:
    """Valor de ``scrape_dispatch_meta[key]``, ou None."""
    cur = _execute(conn, f"SELECT value FROM {META_TABLE} WHERE key = %s", (key,))  # nosec B608
    try:
        row = cur.fetchone()
    finally:
        cur.close()
    return row[0] if row is not None else None


def update_tiers(conn, updates: Iterable[Any], now: datetime) -> int:
    """Grava as faixas recalculadas (``TierUpdate``); devolve quantas linhas mudaram.

    Só as colunas da faixa (``tier``, ``poll_interval_minutes``, ``rate_ewma_per_day``,
    ``tier_updated_at``, ``updated_at``), num UPDATE por lote: não sobrescreve o resto da
    linha, que o ``record_scrape_run`` pode estar gravando ao mesmo tempo. Com
    ``next_due_cap`` e o circuito ainda fechado, ``next_due_at`` vira o menor dos dois.
    """
    rows = [
        (
            u.agency_key,
            u.tier.value,
            u.poll_interval_minutes,
            u.rate_ewma_per_day,
            u.next_due_cap,
            now,
        )
        for u in updates
    ]
    if not rows:
        return 0
    sql = (
        f"UPDATE {TABLE} AS s SET "  # nosec B608
        "tier = v.tier, poll_interval_minutes = v.poll_interval_minutes, "
        "rate_ewma_per_day = v.rate, tier_updated_at = v.now, updated_at = v.now, "
        "next_due_at = CASE WHEN v.due_cap IS NOT NULL AND s.circuit_state = 'closed' "
        "THEN LEAST(s.next_due_at, v.due_cap) ELSE s.next_due_at END "
        "FROM (VALUES %s) AS v(agency_key, tier, poll_interval_minutes, rate, due_cap, now) "
        "WHERE s.agency_key = v.agency_key"
    )
    template = "(%s, %s, %s::integer, %s::double precision, %s::timestamptz, %s::timestamptz)"
    cur = conn.cursor()
    try:
        execute_values(cur, sql, rows, template=template, page_size=500)
        return max(cur.rowcount, 0)
    except (errors.UndefinedTable, errors.UndefinedColumn) as e:
        raise StateUnavailable(f"{TABLE} indisponível: {e}".strip()) from e
    finally:
        cur.close()
