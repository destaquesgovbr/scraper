"""Shared fixtures for unit tests."""

from unittest.mock import MagicMock

import pytest

from govbr_scraper.scrapers.webscraper import WebScraper
from govbr_scraper.storage.postgres_manager import PostgresManager


@pytest.fixture
def scraper():
    """Base WebScraper instance for tests that don't need custom config."""
    return WebScraper(
        base_url="https://www.gov.br/test/pt-br/noticias",
        min_date="2026-01-01",
    )


@pytest.fixture
def mock_pool():
    """Mock psycopg2 connection pool returning (pool, conn, cursor)."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    pool = MagicMock()
    pool.getconn.return_value = mock_conn

    return pool, mock_conn, mock_cursor


@pytest.fixture
def pg_manager(mock_pool):
    """PostgresManager with mocked connection pool."""
    pool_obj, _, _ = mock_pool
    manager = PostgresManager.__new__(PostgresManager)
    manager._connection_string = "postgresql://test"
    manager.pool = pool_obj
    manager._agencies_by_key = {}
    manager._agencies_by_id = {}
    manager._themes_by_code = {}
    manager._themes_by_id = {}
    manager._cache_loaded = False
    return manager
