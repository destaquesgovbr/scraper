"""
Guarda do Postgres descartável usado pelos testes `postgres` (SCRAPER_TEST_POSTGRES_URL).

Os testes criam e removem um schema próprio com DDL (CREATE EXTENSION, CREATE
SCHEMA, TRUNCATE, DROP SCHEMA ... CASCADE). "Host local" não basta: o acesso
padrão ao banco de produção é o Cloud SQL Proxy em 127.0.0.1:5432
(PostgresManager._get_connection_string). Por isso a guarda tem duas etapas:

1. unsafe_dsn_reason(dsn), antes de conectar: host local explícito, sem nada
   que desvie a conexão real (hostaddr, service, PGHOSTADDR, PGSERVICE), porta
   diferente da padrão 5432 e dbname que não seja de produção;
2. unsafe_database_reason(cursor), depois de conectar e antes de qualquer DDL:
   banco que não é de produção, não é Cloud SQL e não tem tabela `news` fora
   dos schemas de teste (scraper_it_*).
"""

import os

from psycopg2 import ProgrammingError
from psycopg2.extensions import parse_dsn

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
DEFAULT_PORT = "5432"
PRODUCTION_DATABASES = frozenset({"destaquesgovbr", "govbrnews"})
TEST_SCHEMA_PREFIX = "scraper_it_"

# Parâmetros do libpq que levam a conexão para outro endereço que não o `host`.
_REDIRECT_PARAMS = ("hostaddr", "service")
_REDIRECT_ENV_VARS = ("PGHOSTADDR", "PGSERVICE")

_DATABASE_CHECK_SQL = f"""
    SELECT current_database(),
           EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cloudsqlsuperuser'),
           EXISTS (
               SELECT 1
               FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
               WHERE c.relname = 'news'
                 AND NOT starts_with(n.nspname, '{TEST_SCHEMA_PREFIX}')
           )
"""


def unsafe_dsn_reason(dsn: str) -> str | None:
    """Motivo para recusar o DSN antes de conectar, ou None se ele for aceitável."""
    try:
        params = parse_dsn(dsn)
    except ProgrammingError as e:
        return f"DSN inválido: {e}"

    host = params.get("host", "")
    if host not in LOCAL_HOSTS and not host.startswith("/"):
        return f"host não local (host={host!r})"

    for param in _REDIRECT_PARAMS:
        if param in params:
            return f"{param}= no DSN desvia a conexão real do host ({param}={params[param]!r})"
    for var in _REDIRECT_ENV_VARS:
        if os.environ.get(var):
            return f"{var} definido no ambiente desvia a conexão real do host"

    port = params.get("port") or os.environ.get("PGPORT") or DEFAULT_PORT
    if port == DEFAULT_PORT:
        return (
            f"porta {DEFAULT_PORT} (padrão, usada pelo Cloud SQL Proxy): suba o Postgres "
            "descartável em outra porta, ex. -p 127.0.0.1:55432:5432"
        )

    dbname = params.get("dbname", "")
    if dbname in PRODUCTION_DATABASES:
        return f"dbname de produção ({dbname!r})"
    return None


def unsafe_database_reason(cursor) -> str | None:
    """Motivo para recusar o banco já conectado (antes de qualquer DDL), ou None."""
    cursor.execute(_DATABASE_CHECK_SQL)
    database, is_cloud_sql, has_news_table = cursor.fetchone()
    if database in PRODUCTION_DATABASES:
        return f"banco de produção (current_database()={database!r})"
    if is_cloud_sql:
        return "instância Cloud SQL (papel cloudsqlsuperuser existe)"
    if has_news_table:
        return (
            f"o banco já tem uma tabela news fora dos schemas {TEST_SCHEMA_PREFIX}* (dados reais)"
        )
    return None
