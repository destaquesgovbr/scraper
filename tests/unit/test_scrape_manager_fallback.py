"""
Tests for automatic fallback WebScraper → Plone6APIScraper.

Verifies that ScrapeManager automatically attempts Plone6APIScraper
when WebScraper fails with HTML_CHANGED error, and tracks fallback
metadata in structured logs.
"""

from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.scrape_manager import ScrapeManager
from govbr_scraper.scrapers.webscraper import ScrapingError, WebScraper


def _config(url, scraper_type="html"):
    return {"url": url, "scraper_type": scraper_type, "active": True}


def _make_manager():
    storage = MagicMock()
    storage.get_recent_urls.return_value = set()
    storage.insert.return_value = 0
    storage.record_scrape_run = MagicMock()
    return ScrapeManager(storage), storage


@pytest.fixture
def mock_scrapers():
    """Fixture that patches both WebScraper and Plone6APIScraper."""
    with (
        patch.object(WebScraper, "__init__", return_value=None) as mock_ws_init,
        patch.object(WebScraper, "scrape_news") as mock_ws_scrape,
        patch.object(Plone6APIScraper, "__init__", return_value=None) as mock_p6_init,
        patch.object(Plone6APIScraper, "scrape_news") as mock_p6_scrape,
    ):
        yield {
            "ws_init": mock_ws_init,
            "ws_scrape": mock_ws_scrape,
            "p6_init": mock_p6_init,
            "p6_scrape": mock_p6_scrape,
        }


@pytest.fixture
def mock_log_scrape_result():
    """Mock log_scrape_result to capture calls."""
    with patch("govbr_scraper.scrapers.scrape_manager.log_scrape_result") as mock_log:
        mock_log.return_value = MagicMock()
        yield mock_log


class TestFallbackWebScraperToPlone6:
    """Test automatic fallback from WebScraper to Plone6APIScraper on HTML_CHANGED."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_fallback_webscraper_to_plone6_success(
        self, mock_load, mock_scrapers, mock_log_scrape_result
    ):
        """WebScraper fails with HTML_CHANGED, Plone6 is instantiated lazily and succeeds."""
        mock_load.return_value = {"coaf": _config("https://www.gov.br/coaf/noticias", "html")}

        # WebScraper raises HTML_CHANGED error
        mock_scrapers["ws_scrape"].side_effect = ScrapingError(
            "No articles found on first page of coaf but response was 280000 bytes"
        )

        # Plone6 returns valid data
        mock_scrapers["p6_scrape"].return_value = [
            {"title": "Test Article", "url": "https://www.gov.br/coaf/test"}
        ]

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["coaf"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # WebScraper instantiated upfront, Plone6 instantiated lazily
        assert mock_scrapers["ws_init"].call_count == 1
        assert mock_scrapers["p6_init"].call_count == 1

        # Both should be called
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 1

        # Verify Plone6 was instantiated with known_urls=set() (empty, not inherited)
        p6_init_call_args = mock_scrapers["p6_init"].call_args
        # Signature: Plone6APIScraper(min_date, url, max_date=..., known_urls=...)
        assert p6_init_call_args[0][0] == "2026-01-01"  # min_date
        assert p6_init_call_args[0][1] == "https://www.gov.br/coaf/noticias"  # url
        assert p6_init_call_args[1]["max_date"] == "2026-01-31"
        assert p6_init_call_args[1]["known_urls"] == set()  # Empty, not inherited

        # Check structured log was called with fallback metadata
        mock_log_scrape_result.assert_called_once()
        call_kwargs = mock_log_scrape_result.call_args[1]
        assert call_kwargs["status"] == "success"
        assert call_kwargs["primary_scraper"] == "webscraper"
        assert call_kwargs["fallback_triggered"] is True
        assert call_kwargs["fallback_scraper"] == "plone6_api"
        assert call_kwargs["fallback_success"] is True
        assert call_kwargs["articles_scraped"] == 1

        # No errors reported
        assert result["errors"] == []

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_fallback_webscraper_to_plone6_both_fail(
        self, mock_load, mock_scrapers, mock_log_scrape_result
    ):
        """WebScraper fails with HTML_CHANGED, Plone6 also fails."""
        mock_load.return_value = {"coaf": _config("https://www.gov.br/coaf/noticias", "html")}

        # Both scrapers fail
        mock_scrapers["ws_scrape"].side_effect = ScrapingError(
            "No articles found on first page of coaf but response was 280000 bytes"
        )
        mock_scrapers["p6_scrape"].side_effect = ScrapingError("API request failed with status 500")

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["coaf"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # Both scrapers attempted
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 1

        # Error should be logged
        mock_log_scrape_result.assert_called_once()
        call_kwargs = mock_log_scrape_result.call_args[1]
        assert call_kwargs["status"] == "error"

        # Error reported
        assert len(result["errors"]) == 1

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_no_fallback_when_webscraper_succeeds(
        self, mock_load, mock_scrapers, mock_log_scrape_result
    ):
        """WebScraper succeeds, Plone6 is not instantiated or called (lazy instantiation)."""
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias", "html")}

        # WebScraper succeeds
        mock_scrapers["ws_scrape"].return_value = [
            {"title": "Test Article", "url": "https://www.gov.br/mec/test"}
        ]

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["mec"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # Only WebScraper instantiated (lazy instantiation: no fallback created until needed)
        assert mock_scrapers["ws_init"].call_count == 1
        assert mock_scrapers["p6_init"].call_count == 0

        # Only WebScraper called
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 0

        # Check structured log
        mock_log_scrape_result.assert_called_once()
        call_kwargs = mock_log_scrape_result.call_args[1]
        assert call_kwargs["status"] == "success"
        assert call_kwargs["primary_scraper"] == "webscraper"
        assert call_kwargs["fallback_triggered"] is False
        assert call_kwargs["fallback_scraper"] is None
        assert call_kwargs["fallback_success"] is None

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_no_fallback_for_plone6_primary(self, mock_load, mock_scrapers, mock_log_scrape_result):
        """Plone6 configured as primary, WebScraper is not called on failure."""
        mock_load.return_value = {
            "susep": _config("https://www.gov.br/susep/noticias", "plone6_api")
        }

        # Plone6 fails
        mock_scrapers["p6_scrape"].side_effect = ScrapingError("API unavailable")

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["susep"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # Only Plone6 instantiated (no fallback for plone6_api type)
        assert mock_scrapers["p6_init"].call_count == 1
        assert mock_scrapers["ws_init"].call_count == 0

        # Only Plone6 called
        assert mock_scrapers["p6_scrape"].call_count == 1
        assert mock_scrapers["ws_scrape"].call_count == 0

        # Error logged without fallback metadata
        mock_log_scrape_result.assert_called_once()
        call_kwargs = mock_log_scrape_result.call_args[1]
        assert call_kwargs["status"] == "error"

        # Error reported
        assert len(result["errors"]) == 1


class TestFallbackOnlyForHTMLChanged:
    """Fallback should only trigger for HTML_CHANGED errors, not other error types."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_no_fallback_for_network_error(self, mock_load, mock_scrapers, mock_log_scrape_result):
        """Network errors should not trigger fallback."""
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias", "html")}

        # WebScraper fails with network error
        mock_scrapers["ws_scrape"].side_effect = ScrapingError(
            "Connection timed out after 30 seconds"
        )

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["mec"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # WebScraper called, Plone6 NOT called
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 0

        # Error reported
        assert len(result["errors"]) == 1

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_no_fallback_for_anti_bot(self, mock_load, mock_scrapers, mock_log_scrape_result):
        """Anti-bot errors should not trigger fallback."""
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias", "html")}

        # WebScraper fails with anti-bot detection
        mock_scrapers["ws_scrape"].side_effect = ScrapingError(
            "Detected anti-bot JS challenge from Cloudflare"
        )

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["mec"], min_date="2026-01-01", max_date="2026-01-31", sequential=True
        )

        # WebScraper called, Plone6 NOT called
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 0

        # Error reported
        assert len(result["errors"]) == 1


class TestFallbackBulkMode:
    """Test fallback behavior in bulk mode (sequential=False)."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_fallback_works_in_bulk_mode(self, mock_load, mock_scrapers, mock_log_scrape_result):
        """Fallback should work in bulk mode too."""
        mock_load.return_value = {"coaf": _config("https://www.gov.br/coaf/noticias", "html")}

        # WebScraper fails with HTML_CHANGED
        mock_scrapers["ws_scrape"].side_effect = ScrapingError(
            "No articles found on first page of coaf but response was 280000 bytes"
        )

        # Plone6 succeeds
        mock_scrapers["p6_scrape"].return_value = [
            {"title": "Test", "url": "https://www.gov.br/coaf/test"}
        ]

        manager, storage = _make_manager()
        result = manager.run_scraper(
            agencies=["coaf"], min_date="2026-01-01", max_date="2026-01-31", sequential=False
        )

        # Both scrapers called
        assert mock_scrapers["ws_scrape"].call_count == 1
        assert mock_scrapers["p6_scrape"].call_count == 1

        # Success logged with fallback metadata
        mock_log_scrape_result.assert_called_once()
        call_kwargs = mock_log_scrape_result.call_args[1]
        assert call_kwargs["status"] == "success"
        assert call_kwargs["fallback_triggered"] is True
        assert call_kwargs["fallback_success"] is True
