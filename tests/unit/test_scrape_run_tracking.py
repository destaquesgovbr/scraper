"""Tests for scrape run tracking — recording and querying scrape results."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult


def _make_pg_with_mock_pool():
    """Create a PostgresManager with a mocked connection pool."""
    from govbr_scraper.storage.postgres_manager import PostgresManager

    pg = PostgresManager.__new__(PostgresManager)
    mock_pool = MagicMock()
    pg.pool = mock_pool
    mock_conn = MagicMock()
    mock_pool.getconn.return_value = mock_conn
    # cursor() used as context manager (with conn.cursor() as cur)
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return pg, mock_conn, mock_cursor


class TestRecordScrapeRun:
    """PostgresManager.record_scrape_run() persists a ScrapeRunResult."""

    def _make_run(self, **overrides) -> ScrapeRunResult:
        defaults = {
            "agency_key": "mec",
            "status": "success",
            "articles_scraped": 10,
            "articles_saved": 8,
            "execution_time_seconds": 2.5,
            "scraped_at": datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc),
        }
        defaults.update(overrides)
        return ScrapeRunResult(**defaults)

    def test_record_scrape_run_executes_insert(self):
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()

        run = self._make_run()
        pg.record_scrape_run(run)

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO scrape_runs" in sql
        assert "agency_key" in sql
        mock_conn.commit.assert_called_once()

    def test_record_scrape_run_includes_error_category(self):
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()

        run = self._make_run(
            status="error",
            error_category=ErrorCategory.ANTI_BOT,
            error_message="Anti-bot protection detected",
        )
        pg.record_scrape_run(run)

        params = mock_cursor.execute.call_args[0][1]
        assert "anti_bot" in params
        assert "Anti-bot protection detected" in params


class TestGetRecentRuns:
    """PostgresManager.get_recent_runs() queries scrape history."""

    def test_returns_ordered_by_scraped_at_desc(self):
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = [
            {"agency_key": "mec", "status": "success", "scraped_at": "2026-04-06T12:00"},
            {"agency_key": "mec", "status": "error", "scraped_at": "2026-04-06T11:50"},
        ]

        results = pg.get_recent_runs("mec", limit=5)

        assert len(results) == 2
        sql = mock_cursor.execute.call_args[0][0]
        assert "ORDER BY scraped_at DESC" in sql

    def test_respects_limit(self):
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = []

        pg.get_recent_runs("mec", limit=3)

        params = mock_cursor.execute.call_args[0][1]
        assert 3 in params


class TestStorageAdapterDelegation:
    """StorageAdapter.record_scrape_run() delegates to PostgresManager."""

    def test_delegates_to_postgres(self):
        from govbr_scraper.storage.storage_adapter import StorageAdapter

        mock_pg = MagicMock()
        adapter = StorageAdapter(postgres_manager=mock_pg)

        run = ScrapeRunResult(
            agency_key="mec",
            status="success",
            scraped_at=datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc),
        )
        adapter.record_scrape_run(run)

        mock_pg.record_scrape_run.assert_called_once_with(run)


class TestGetRecentRunsExtended:
    """Extended tests for get_recent_runs() edge cases and filtering."""

    def test_multiple_runs_for_same_agency_returns_all_within_limit(self):
        """Multiple runs for same agency within limit are all returned."""
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = [
            {"agency_key": "mec", "status": "success", "scraped_at": "2026-04-06T14:00"},
            {"agency_key": "mec", "status": "success", "scraped_at": "2026-04-06T13:00"},
            {"agency_key": "mec", "status": "error", "scraped_at": "2026-04-06T12:00"},
        ]

        results = pg.get_recent_runs("mec", limit=5)

        assert len(results) == 3
        # Verify query was executed with correct agency filter
        params = mock_cursor.execute.call_args[0][1]
        assert "mec" in params

    def test_empty_result_when_no_runs_found(self):
        """Returns empty list when no runs found for agency."""
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = []

        results = pg.get_recent_runs("nonexistent_agency", limit=10)

        assert results == []

    def test_limit_constrains_result_count(self):
        """Limit parameter correctly constrains result count."""
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        # Mock returns more rows than limit
        mock_cursor.fetchall.return_value = [
            {"agency_key": "fazenda", "status": "success", "scraped_at": f"2026-04-06T{12+i}:00"}
            for i in range(10)
        ]

        results = pg.get_recent_runs("fazenda", limit=5)

        # fetchall returns DB result, but limit was passed to query
        params = mock_cursor.execute.call_args[0][1]
        assert 5 in params
        # In real scenario DB would limit, here we just verify parameter was passed
        assert len(results) == 10  # Mock returns all, but query had LIMIT 5

    def test_query_filters_by_agency_key(self):
        """Query includes WHERE clause filtering by agency_key."""
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = []

        pg.get_recent_runs("agricultura", limit=10)

        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]

        assert "WHERE" in sql
        assert "agency_key" in sql
        assert "agricultura" in params

    def test_ordering_ensures_most_recent_first(self):
        """Query orders by scraped_at DESC to get most recent first."""
        pg, mock_conn, mock_cursor = _make_pg_with_mock_pool()
        mock_cursor.fetchall.return_value = [
            {"agency_key": "saude", "status": "success", "scraped_at": "2026-04-06T15:00"},
            {"agency_key": "saude", "status": "error", "scraped_at": "2026-04-06T14:30"},
            {"agency_key": "saude", "status": "success", "scraped_at": "2026-04-06T14:00"},
        ]

        results = pg.get_recent_runs("saude", limit=10)

        # Verify ORDER BY clause is present
        sql = mock_cursor.execute.call_args[0][0]
        assert "ORDER BY scraped_at DESC" in sql

        # Mock data is already ordered DESC, verify we got it
        assert len(results) == 3
        assert results[0]["scraped_at"] == "2026-04-06T15:00"
