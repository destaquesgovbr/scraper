"""Tests for health check functions — consecutive failures, stale, silent agencies, coverage."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from govbr_scraper.monitoring.health_checks import (
    SILENT_AGENCIES_QUERY,
    compute_coverage_report,
    find_consecutive_failures,
    find_silent_agencies,
    find_stale_agencies,
)


@pytest.fixture
def mock_cursor_with_rows():
    """Fixture factory for creating mock cursors with custom rows."""

    def _create(rows):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_conn, mock_cursor

    return _create


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

    def test_custom_window_hours(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_consecutive_failures(mock_conn, threshold=3, window_hours=6)
        params = mock_cursor.execute.call_args[0][1]
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
        sql = mock_cursor.execute.call_args[0][0]
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
        sql, params = mock_cursor.execute.call_args[0]
        assert sql == SILENT_AGENCIES_QUERY
        assert params == {"agencies": ["mec", "aids"], "silent_hours": 3}

    def test_default_window_is_2_hours(self, mock_cursor_with_rows):
        mock_conn, mock_cursor = mock_cursor_with_rows([])
        find_silent_agencies(mock_conn, ["mec"])
        assert mock_cursor.execute.call_args[0][1]["silent_hours"] == 2

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


class TestComputeCoverageReport:
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
