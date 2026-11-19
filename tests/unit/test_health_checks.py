"""Tests for health check functions — consecutive failures, stale, silent agencies, coverage."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from psycopg2 import errors

from govbr_scraper.monitoring.health_checks import (
    OPEN_CIRCUIT_AGENCIES_QUERY,
    SILENT_AGENCIES_QUERY,
    STALE_AGENCIES_QUERY,
    compute_coverage_report,
    find_consecutive_failures,
    find_open_circuit_agencies,
    find_open_circuits,
    find_silent_agencies,
    find_stale_agencies,
)


def _make_conn(rows, open_circuits=(), state_error=None):
    """Conexão mockada: a query de scrape_agency_state devolve as agências com o circuito
    aberto (ou levanta state_error); as outras devolvem ``rows``."""
    mock_cursor = MagicMock()
    last = {"sql": ""}

    def execute(sql, params=None):
        last["sql"] = sql
        if "scrape_agency_state" in sql and state_error is not None:
            raise state_error

    def fetchall():
        if "scrape_agency_state" in last["sql"]:
            return [{"agency_key": k} for k in open_circuits]
        return rows

    mock_cursor.execute.side_effect = execute
    mock_cursor.fetchall.side_effect = fetchall
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


@pytest.fixture
def mock_cursor_with_rows():
    """Fixture factory for creating mock cursors with custom rows."""
    return _make_conn


class TestFindConsecutiveFailures:
    def test_3_errors_returns_agency(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows(
            [
                {
                    "agency_key": "mec",
                    "consecutive_failures": 3,
                    "last_error": "network_error",
                    "last_failure_at": datetime(2026, 4, 6, 14, 30, tzinfo=UTC),
                },
            ]
        )
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert len(result) == 1
        assert result[0]["agency_key"] == "mec"

    def test_default_window_is_6_hours(self, mock_cursor_with_rows):
        """Três execuções de 2 em 2 h da scrape_agencies."""
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_consecutive_failures(mock_conn)
        assert mock_cursor.execute.call_args_list[0][0][1] == {"threshold": 3, "window_hours": 6}

    def test_custom_window_hours(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_consecutive_failures(mock_conn, threshold=3, window_hours=6)
        params = mock_cursor.execute.call_args_list[0][0][1]
        assert params["window_hours"] == 6

    def test_2_errors_1_success_returns_empty(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows([])
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert result == []

    def test_empty_table_returns_empty(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows([])
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert result == []

    def test_query_uses_most_recent_error_not_max(self, mock_cursor_with_rows):
        """last_error must come from the most recent row (rn=1), not alphabetical MAX."""
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_consecutive_failures(mock_conn)
        sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "MAX(error_category) AS last_error" not in sql


class TestFindStaleAgencies:
    def test_no_articles_in_24h_returns_agency(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows(
            [
                {"agency_key": "mds", "last_success_at": datetime(2026, 4, 4, 10, 0, tzinfo=UTC)},
            ]
        )
        result = find_stale_agencies(mock_conn, stale_hours=24)
        assert len(result) == 1
        assert result[0]["agency_key"] == "mds"

    def test_recent_success_returns_empty(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows([])
        result = find_stale_agencies(mock_conn, stale_hours=24)
        assert result == []

    def test_executes_shared_query_with_stale_hours(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_stale_agencies(mock_conn, stale_hours=12)
        sql, params = mock_cursor.execute.call_args_list[0][0]
        assert sql == STALE_AGENCIES_QUERY
        assert params == {"stale_hours": 12}


class TestFindSilentAgencies:
    """Agência ativa sem nenhum scrape_run na janela = DAG que não chama a API.

    Caso real (out/2026): scrape_inma, scrape_inpe, scrape_aids e scrape_patrimonio
    ficaram meses com um DagRun preso em 'running' (DagVersion sem serialized_dag) e
    não geraram scrape_runs; as checagens que partem de scrape_runs não os viam.
    """

    def test_returns_active_agencies_without_recent_runs(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows(
            [
                {
                    "agency_key": "aids",
                    "last_run_at": datetime(2026, 8, 21, 11, 32, tzinfo=UTC),
                },
                {"agency_key": "inma", "last_run_at": None},
            ]
        )
        result = find_silent_agencies(mock_conn, ["aids", "inma", "mec"], silent_hours=2)
        assert [r["agency_key"] for r in result] == ["aids", "inma"]
        assert result[1]["last_run_at"] is None

    def test_passes_active_agencies_and_window_as_params(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_silent_agencies(mock_conn, ("mec", "aids"), silent_hours=3)
        sql, params = mock_cursor.execute.call_args_list[0][0]
        assert sql == SILENT_AGENCIES_QUERY
        assert params == {"agencies": ["mec", "aids"], "silent_hours": 3}

    def test_default_window_is_3_hours(self, mock_cursor_with_rows):
        """scrape_agencies roda de 2 em 2 h: 2 h + 1 h de folga."""
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_silent_agencies(mock_conn, ["mec"])
        assert mock_cursor.execute.call_args_list[0][0][1]["silent_hours"] == 3

    def test_empty_active_list_skips_query(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        assert find_silent_agencies(mock_conn, []) == []
        mock_cursor.execute.assert_not_called()

    def test_any_run_counts_regardless_of_status(self):
        """Agência que falha grava scrape_run de erro: é falha, não silêncio."""
        assert "status" not in SILENT_AGENCIES_QUERY

    def test_uses_latest_run_per_agency_via_lateral(self):
        """Top-1 por agência usa o índice idx_scrape_runs_agency_scraped."""
        normalized = " ".join(SILENT_AGENCIES_QUERY.split())
        assert "LEFT JOIN LATERAL" in normalized
        assert "ORDER BY sr.scraped_at DESC LIMIT 1" in normalized


FAILURE_ROWS = [
    {"agency_key": "mec", "consecutive_failures": 1, "last_error": "html_changed"},
    {"agency_key": "mds", "consecutive_failures": 3, "last_error": "network_error"},
]
SILENT_ROWS = [
    {"agency_key": "mec", "last_run_at": None},
    {"agency_key": "aids", "last_run_at": None},
]


class TestOpenCircuitExclusion:
    """Agência com o disjuntor aberto: a API a pula sem gravar scrape_run e o alerta do
    circuito já cobre; sai de "Agências Sem Execução" e das falhas consecutivas."""

    def test_query_reads_non_closed_circuits(self):
        normalized = " ".join(OPEN_CIRCUIT_AGENCIES_QUERY.split())
        assert normalized == (
            "SELECT agency_key, circuit_state, circuit_opened_at, next_due_at, "
            "last_error_category FROM scrape_agency_state WHERE circuit_state <> 'closed' "
            "ORDER BY agency_key"
        )

    def test_find_open_circuits_returns_rows(self):
        conn, _ = _make_conn([], open_circuits=["antaq", "mec"])
        assert find_open_circuits(conn) == [{"agency_key": "antaq"}, {"agency_key": "mec"}]

    def test_find_open_circuits_missing_table(self):
        conn, _ = _make_conn([], state_error=errors.UndefinedTable("no scrape_agency_state"))
        assert find_open_circuits(conn) == []
        conn.rollback.assert_called_once()

    def test_find_open_circuit_agencies(self):
        conn, _ = _make_conn([], open_circuits=["mec", "antaq"])
        assert find_open_circuit_agencies(conn) == {"mec", "antaq"}

    def test_missing_table_rolls_back_and_returns_empty(self):
        conn, _ = _make_conn([], state_error=errors.UndefinedTable("no scrape_agency_state"))
        assert find_open_circuit_agencies(conn) == set()
        conn.rollback.assert_called_once()

    def test_failures_exclude_open_circuits(self):
        conn, _ = _make_conn(FAILURE_ROWS, open_circuits=["mec"])
        assert [r["agency_key"] for r in find_consecutive_failures(conn)] == ["mds"]

    def test_silent_excludes_open_circuits(self):
        conn, _ = _make_conn(SILENT_ROWS, open_circuits=["mec"])
        assert [r["agency_key"] for r in find_silent_agencies(conn, ["mec", "aids"])] == ["aids"]

    def test_without_table_nothing_changes(self):
        error = errors.UndefinedTable("no scrape_agency_state")
        conn, _ = _make_conn(SILENT_ROWS, state_error=error)
        assert find_silent_agencies(conn, ["mec", "aids"]) == SILENT_ROWS
        conn, _ = _make_conn(FAILURE_ROWS, state_error=error)
        assert find_consecutive_failures(conn) == FAILURE_ROWS

    def test_exclusion_can_be_disabled(self):
        conn, cursor = _make_conn(FAILURE_ROWS, open_circuits=["mec"])
        assert find_consecutive_failures(conn, skip_open_circuits=False) == FAILURE_ROWS
        assert cursor.execute.call_count == 1

    def test_stale_is_not_filtered(self):
        conn, cursor = _make_conn([{"agency_key": "mec", "last_success_at": None}], ["mec"])
        assert len(find_stale_agencies(conn)) == 1
        assert cursor.execute.call_count == 1


class TestComputeCoverageReport:
    def test_total_articles_counts_new_articles(self, mock_cursor_with_rows):
        """Total de notícias novas (articles_new); antes da migration 037, articles_saved."""
        mock_conn, mock_cursor = mock_cursor_with_rows([{"total_articles": 0}])
        compute_coverage_report(mock_conn, hours=24)
        sql = " ".join(mock_cursor.execute.call_args_list[0][0][0].split())
        assert "COALESCE(SUM(COALESCE(articles_new, articles_saved)), 0) AS total_articles" in sql

    def test_returns_correct_ratio(self, mock_cursor_with_rows):
        mock_conn, _ = mock_cursor_with_rows(
            [
                {
                    "total_active": 155,
                    "agencies_scraped": 140,
                    "agencies_with_errors": 15,
                    "total_articles": 500,
                    "coverage_ratio": 0.90,
                },
            ]
        )
        result = compute_coverage_report(mock_conn, hours=24)
        assert result["total_active"] == 155
        assert result["agencies_scraped"] == 140
        assert result["coverage_ratio"] == 0.90
