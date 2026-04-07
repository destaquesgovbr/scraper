"""Tests for health check functions — consecutive failures, stale agencies, coverage."""

from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.monitoring.health_checks import (
    find_consecutive_failures,
    find_stale_agencies,
    compute_coverage_report,
)


def _mock_cursor_with_rows(rows):
    """Create a mock cursor context manager returning given rows."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


class TestFindConsecutiveFailures:

    def test_3_errors_returns_agency(self):
        mock_conn, _ = _mock_cursor_with_rows([
            {"agency_key": "mec", "consecutive_failures": 3, "last_error": "network_error",
             "last_failure_at": datetime(2026, 4, 6, 14, 30, tzinfo=timezone.utc)},
        ])
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert len(result) == 1
        assert result[0]["agency_key"] == "mec"

    def test_2_errors_1_success_returns_empty(self):
        mock_conn, _ = _mock_cursor_with_rows([])
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert result == []

    def test_empty_table_returns_empty(self):
        mock_conn, _ = _mock_cursor_with_rows([])
        result = find_consecutive_failures(mock_conn, threshold=3)
        assert result == []


class TestFindStaleAgencies:

    def test_no_articles_in_24h_returns_agency(self):
        mock_conn, _ = _mock_cursor_with_rows([
            {"agency_key": "mds", "last_success_at": datetime(2026, 4, 4, 10, 0, tzinfo=timezone.utc)},
        ])
        result = find_stale_agencies(mock_conn, stale_hours=24)
        assert len(result) == 1
        assert result[0]["agency_key"] == "mds"

    def test_recent_success_returns_empty(self):
        mock_conn, _ = _mock_cursor_with_rows([])
        result = find_stale_agencies(mock_conn, stale_hours=24)
        assert result == []


class TestComputeCoverageReport:

    def test_returns_correct_ratio(self):
        mock_conn, _ = _mock_cursor_with_rows([
            {"total_active": 155, "agencies_scraped": 140, "agencies_with_errors": 15,
             "total_articles": 500, "coverage_ratio": 0.90},
        ])
        result = compute_coverage_report(mock_conn, hours=24)
        assert result["total_active"] == 155
        assert result["agencies_scraped"] == 140
        assert result["coverage_ratio"] == 0.90
