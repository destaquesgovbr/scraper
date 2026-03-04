"""Tests for the 'known URL fence' optimization in WebScraper.

When known_urls is provided, the scraper should:
- Skip fetching article pages for URLs already in the database
- Stop pagination after N consecutive known URLs (fence)
- Continue normally for unknown URLs
- Maintain backward compatibility when known_urls is empty
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.scrapers.webscraper import WebScraper


@pytest.fixture
def scraper():
    """Create a WebScraper with known_urls for testing."""
    known = {
        "https://gov.br/mec/noticia-antiga-1",
        "https://gov.br/mec/noticia-antiga-2",
        "https://gov.br/mec/noticia-antiga-3",
        "https://gov.br/mec/noticia-antiga-4",
    }
    s = WebScraper(
        min_date="2026-03-01",
        base_url="https://www.gov.br/mec/pt-br/assuntos/noticias",
        max_date="2026-03-03",
        known_urls=known,
    )
    return s


@pytest.fixture
def scraper_no_known():
    """Create a WebScraper without known_urls (backward compat)."""
    return WebScraper(
        min_date="2026-03-01",
        base_url="https://www.gov.br/mec/pt-br/assuntos/noticias",
        max_date="2026-03-03",
    )


def _make_item_mock(scraper, url, title="Some Title", news_date=None):
    """Helper: mock the extraction methods on scraper for a single item call."""
    if news_date is None:
        news_date = date(2026, 3, 2)
    item = MagicMock()
    scraper.extract_title_and_url = MagicMock(return_value=(title, url))
    scraper.extract_category = MagicMock(return_value="Educação")
    scraper.extract_date = MagicMock(return_value=news_date)
    scraper.extract_tags = MagicMock(return_value=[])
    brasilia_tz = timezone(timedelta(hours=-3))
    scraper.get_article_content = MagicMock(
        return_value=(
            "Article content",
            "https://img.gov.br/photo.jpg",
            datetime(2026, 3, 2, 14, 30, tzinfo=brasilia_tz),
            None,
            ["tag1"],
            None,
            None,
            "Educação",
        )
    )
    return item


class TestKnownUrlSkip:
    """Known URLs should be skipped without fetching the article page."""

    def test_known_article_is_skipped(self, scraper):
        """Article with known URL should be skipped (no fetch)."""
        item = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-1")

        result = scraper.extract_news_info(item)

        assert result is True  # Continue processing
        scraper.get_article_content.assert_not_called()
        assert len(scraper.news_data) == 0  # Not added to results

    def test_new_article_is_processed(self, scraper):
        """Article with unknown URL should be fetched and processed normally."""
        item = _make_item_mock(scraper, "https://gov.br/mec/noticia-nova")

        result = scraper.extract_news_info(item)

        assert result is True
        scraper.get_article_content.assert_called_once_with("https://gov.br/mec/noticia-nova")
        assert len(scraper.news_data) == 1
        assert scraper.news_data[0]["url"] == "https://gov.br/mec/noticia-nova"


class TestKnownUrlFenceStop:
    """Consecutive known URLs should trigger a stop after threshold."""

    def test_consecutive_known_urls_stop(self, scraper):
        """3 consecutive known URLs should trigger stop (return False)."""
        results = []
        for i in range(1, 4):
            item = _make_item_mock(scraper, f"https://gov.br/mec/noticia-antiga-{i}")
            results.append(scraper.extract_news_info(item))

        assert results == [True, True, False]  # skip, skip, STOP
        assert scraper.get_article_content.call_count == 0  # No fetches at all
        assert len(scraper.news_data) == 0

    def test_new_article_resets_consecutive_counter(self, scraper):
        """A new article between known ones should reset the consecutive counter."""
        # Known 1
        item1 = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-1")
        r1 = scraper.extract_news_info(item1)

        # New article — resets counter
        item2 = _make_item_mock(scraper, "https://gov.br/mec/noticia-nova")
        r2 = scraper.extract_news_info(item2)

        # Known 2 — counter restarts at 1
        item3 = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-2")
        r3 = scraper.extract_news_info(item3)

        # Known 3 — counter at 2, still below threshold
        item4 = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-3")
        r4 = scraper.extract_news_info(item4)

        assert [r1, r2, r3, r4] == [True, True, True, True]  # No stop
        assert scraper._consecutive_known == 2  # 2 consecutive, not 3

    def test_known_url_below_threshold_continues(self, scraper):
        """1-2 consecutive known URLs should not trigger stop."""
        item1 = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-1")
        r1 = scraper.extract_news_info(item1)

        item2 = _make_item_mock(scraper, "https://gov.br/mec/noticia-antiga-2")
        r2 = scraper.extract_news_info(item2)

        assert r1 is True
        assert r2 is True
        assert scraper._consecutive_known == 2


class TestBackwardCompatibility:
    """Without known_urls, behavior should be identical to current."""

    def test_empty_known_urls_processes_all(self, scraper_no_known):
        """Without known_urls, all articles are fetched normally."""
        s = scraper_no_known
        item = _make_item_mock(s, "https://gov.br/mec/qualquer-noticia")

        result = s.extract_news_info(item)

        assert result is True
        s.get_article_content.assert_called_once()
        assert len(s.news_data) == 1

    def test_date_stop_still_works_with_known_urls(self, scraper):
        """Date-based stop condition should still work alongside known URL fence."""
        old_date = date(2026, 2, 28)  # Before min_date (2026-03-01)
        item = _make_item_mock(
            scraper, "https://gov.br/mec/noticia-muito-velha", news_date=old_date
        )

        result = scraper.extract_news_info(item)

        assert result is False  # Stop due to date
        scraper.get_article_content.assert_not_called()


class TestStorageGetRecentUrls:
    """Tests for PostgresManager.get_recent_urls and StorageAdapter.get_recent_urls."""

    def test_get_recent_urls_returns_set(self):
        """get_recent_urls should return a set of URL strings."""
        from govbr_scraper.storage.postgres_manager import PostgresManager

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("https://gov.br/mec/noticia-1",),
            ("https://gov.br/mec/noticia-2",),
        ]
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pm = PostgresManager.__new__(PostgresManager)
        pm.pool = MagicMock()
        pm.pool.getconn.return_value = mock_conn

        result = pm.get_recent_urls("mec")

        assert result == {"https://gov.br/mec/noticia-1", "https://gov.br/mec/noticia-2"}
        assert isinstance(result, set)
        pm.pool.putconn.assert_called_once_with(mock_conn)

    def test_get_recent_urls_respects_limit(self):
        """get_recent_urls should pass limit to the SQL query."""
        from govbr_scraper.storage.postgres_manager import PostgresManager

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pm = PostgresManager.__new__(PostgresManager)
        pm.pool = MagicMock()
        pm.pool.getconn.return_value = mock_conn

        pm.get_recent_urls("mec", limit=50)

        mock_cursor.execute.assert_called_once()
        args = mock_cursor.execute.call_args
        assert args[0][1] == ("mec", 50)  # agency_key and limit params

    def test_get_recent_urls_empty_for_unknown_agency(self):
        """Unknown agency should return empty set (no error)."""
        from govbr_scraper.storage.postgres_manager import PostgresManager

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        pm = PostgresManager.__new__(PostgresManager)
        pm.pool = MagicMock()
        pm.pool.getconn.return_value = mock_conn

        result = pm.get_recent_urls("agencia_inexistente")

        assert result == set()

    def test_storage_adapter_delegates_to_postgres(self):
        """StorageAdapter.get_recent_urls should delegate to PostgresManager."""
        from govbr_scraper.storage.storage_adapter import StorageAdapter

        mock_pm = MagicMock()
        mock_pm.get_recent_urls.return_value = {"https://example.com/1"}

        adapter = StorageAdapter(postgres_manager=mock_pm)

        result = adapter.get_recent_urls("mec", limit=100)

        assert result == {"https://example.com/1"}
        mock_pm.get_recent_urls.assert_called_once_with("mec", 100)
