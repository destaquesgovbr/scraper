"""
Guarda do Postgres descartável dos testes `postgres` (tests/integration/postgres_guard.py).

Roda no CI sem banco: a guarda é o que impede o fixture de executar DDL
(CREATE SCHEMA, TRUNCATE, DROP SCHEMA ... CASCADE) num banco que não seja descartável.
"""

import pytest

from tests.integration.postgres_guard import unsafe_dsn_reason


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
        assert unsafe_dsn_reason(dsn) is None

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
        assert "host" in unsafe_dsn_reason(dsn)

    def test_refuses_invalid_dsn(self):
        assert "inválido" in unsafe_dsn_reason("isto não é um dsn")
