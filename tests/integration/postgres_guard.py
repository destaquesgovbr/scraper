"""
Guarda do Postgres descartável usado pelos testes `postgres` (SCRAPER_TEST_POSTGRES_URL).

Os testes criam e removem um schema próprio com DDL (CREATE EXTENSION, CREATE
SCHEMA, TRUNCATE, DROP SCHEMA ... CASCADE). A guarda recusa qualquer DSN que não
aponte para um Postgres local.
"""

from psycopg2 import ProgrammingError
from psycopg2.extensions import parse_dsn

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def unsafe_dsn_reason(dsn: str) -> str | None:
    """Motivo para recusar o DSN antes de conectar, ou None se ele for aceitável."""
    try:
        params = parse_dsn(dsn)
    except ProgrammingError as e:
        return f"DSN inválido: {e}"

    host = params.get("host", "")
    if host not in LOCAL_HOSTS and not host.startswith("/"):
        return f"host não local (host={host!r})"
    return None
