"""
Guarda do Postgres descartável dos testes `postgres` (tests/integration/postgres_guard.py).

Roda no CI sem banco: a guarda é o que impede o fixture de executar DDL
(CREATE SCHEMA, TRUNCATE, DROP SCHEMA ... CASCADE) num banco que não seja descartável.
"Host local" não basta: o acesso padrão ao banco de produção é o Cloud SQL Proxy em
127.0.0.1:5432 (PostgresManager._get_connection_string).
"""

from unittest.mock import MagicMock

import pytest

from tests.integration import postgres_guard

CLOUD_SQL_PROXY_DSN = (
    "postgresql://destaquesgovbr_app:x@127.0.0.1:5432/destaquesgovbr"  # pragma: allowlist secret
)


@pytest.fixture(autouse=True)
def _clean_libpq_env(monkeypatch):
    """Variáveis do libpq do ambiente do dev não podem influenciar os testes."""
    for var in ("PGHOSTADDR", "PGSERVICE", "PGPORT"):
        monkeypatch.delenv(var, raising=False)


class TestUnsafeDsnReason:
    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://postgres@127.0.0.1:55432/postgres",
            "postgresql://postgres@localhost:55432/postgres",
            "host=::1 port=55432 dbname=postgres user=postgres",
            "host=/var/run/postgresql port=55432 dbname=postgres user=postgres",
        ],
        ids=["ipv4", "localhost", "ipv6", "socket"],
    )
    def test_accepts_local_disposable_dsn(self, dsn):
        assert postgres_guard.unsafe_dsn_reason(dsn) is None

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://postgres@34.39.145.55:55432/postgres",
            "postgresql://postgres@localhost,10.0.0.1:55432/postgres",
            "postgresql:///postgres",
        ],
        ids=["remoto", "multi-host", "sem-host"],
    )
    def test_refuses_non_local_host(self, dsn):
        assert "host" in postgres_guard.unsafe_dsn_reason(dsn)

    def test_refuses_invalid_dsn(self):
        assert "inválido" in postgres_guard.unsafe_dsn_reason("isto não é um dsn")

    def test_refuses_cloud_sql_proxy_dsn(self):
        """O DSN que o PostgresManager monta quando detecta o cloud-sql-proxy."""
        assert postgres_guard.unsafe_dsn_reason(CLOUD_SQL_PROXY_DSN) is not None

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://postgres@127.0.0.1:5432/postgres",
            "postgresql://postgres@127.0.0.1/postgres",
            "host=/var/run/postgresql dbname=postgres user=postgres",
        ],
        ids=["explicita", "omitida", "socket-omitida"],
    )
    def test_refuses_default_port(self, dsn):
        """5432 é a porta do Cloud SQL Proxy (inclusive quando a porta é omitida)."""
        assert "5432" in postgres_guard.unsafe_dsn_reason(dsn)

    def test_refuses_default_port_from_pgport(self, monkeypatch):
        monkeypatch.setenv("PGPORT", "5432")
        reason = postgres_guard.unsafe_dsn_reason("postgresql://postgres@127.0.0.1/postgres")
        assert "5432" in reason

    def test_accepts_non_default_port_from_pgport(self, monkeypatch):
        monkeypatch.setenv("PGPORT", "55432")
        assert postgres_guard.unsafe_dsn_reason("postgresql://postgres@127.0.0.1/postgres") is None

    @pytest.mark.parametrize(
        "dsn",
        [
            "postgresql://postgres@localhost:55432/postgres?hostaddr=10.1.2.3",
            "host=localhost hostaddr=10.1.2.3 port=55432 dbname=postgres",
        ],
        ids=["uri", "chave-valor"],
    )
    def test_refuses_hostaddr(self, dsn):
        """hostaddr= desvia a conexão real enquanto host=localhost passa na checagem."""
        assert "hostaddr" in postgres_guard.unsafe_dsn_reason(dsn)

    def test_refuses_service(self):
        """Um service do pg_service.conf pode trazer hostaddr/porta de outro banco."""
        dsn = "postgresql://postgres@localhost:55432/postgres?service=prod"
        assert "service" in postgres_guard.unsafe_dsn_reason(dsn)

    @pytest.mark.parametrize("var", ["PGHOSTADDR", "PGSERVICE"])
    def test_refuses_libpq_redirect_env(self, monkeypatch, var):
        monkeypatch.setenv(var, "10.1.2.3" if var == "PGHOSTADDR" else "prod")
        reason = postgres_guard.unsafe_dsn_reason("postgresql://postgres@127.0.0.1:55432/postgres")
        assert var in reason

    @pytest.mark.parametrize("dbname", ["destaquesgovbr", "govbrnews"])
    def test_refuses_production_database_name(self, dbname):
        reason = postgres_guard.unsafe_dsn_reason(f"postgresql://postgres@127.0.0.1:55432/{dbname}")
        assert dbname in reason


def _cursor(database: str, is_cloud_sql: bool, has_news_table: bool) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone.return_value = (database, is_cloud_sql, has_news_table)
    return cursor


class TestUnsafeDatabaseReason:
    """Checagem depois de conectar e antes de qualquer DDL."""

    def test_accepts_empty_disposable_database(self):
        assert postgres_guard.unsafe_database_reason(_cursor("postgres", False, False)) is None

    @pytest.mark.parametrize("database", ["destaquesgovbr", "govbrnews"])
    def test_refuses_production_database(self, database):
        reason = postgres_guard.unsafe_database_reason(_cursor(database, False, False))
        assert database in reason

    def test_refuses_cloud_sql_instance(self):
        """Toda instância Cloud SQL tem o papel cloudsqlsuperuser."""
        reason = postgres_guard.unsafe_database_reason(_cursor("postgres", True, False))
        assert "Cloud SQL" in reason

    def test_refuses_database_with_real_news_table(self):
        """Uma tabela news fora dos schemas scraper_it_* indica dados reais."""
        reason = postgres_guard.unsafe_database_reason(_cursor("postgres", False, True))
        assert "news" in reason

    def test_runs_a_single_read_only_query(self):
        cursor = _cursor("postgres", False, False)
        postgres_guard.unsafe_database_reason(cursor)

        cursor.execute.assert_called_once()
        sql = " ".join(cursor.execute.call_args[0][0].split())
        assert sql.startswith("SELECT current_database()")
        assert "cloudsqlsuperuser" in sql
        assert "scraper_it_" in sql
