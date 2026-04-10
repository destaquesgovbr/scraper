"""Tests for monitoring integration in ScrapeManager."""

from unittest.mock import MagicMock, patch, call

import pytest

from govbr_scraper.models.monitoring import ErrorCategory


class TestScrapeManagerMonitoring:
    """ScrapeManager records scrape runs with classification and timing."""

    def _make_manager(self, mock_storage=None):
        from govbr_scraper.scrapers.scrape_manager import ScrapeManager

        storage = mock_storage or MagicMock()
        storage.get_recent_urls.return_value = set()
        return ScrapeManager(storage), storage

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_records_success_run(self, mock_load, mock_ws_cls):
        mock_load.return_value = {"mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True}}
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.return_value = [
            {"agency": "mec", "title": "Test", "published_at": "2026-01-01", "url": "http://x"}
        ]
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        storage.insert.return_value = 1
        manager.run_scraper(agencies=["mec"], min_date="2026-01-01", max_date="2026-01-01", sequential=True)

        storage.record_scrape_run.assert_called_once()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.agency_key == "mec"
        assert run.status == "success"
        assert run.articles_scraped == 1
        assert run.error_category is None

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_records_error_with_classification(self, mock_load, mock_ws_cls):
        from govbr_scraper.scrapers.webscraper import ScrapingError

        mock_load.return_value = {"mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True}}
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.side_effect = ScrapingError(
            "Anti-bot protection detected for mec"
        )
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        manager.run_scraper(agencies=["mec"], min_date="2026-01-01", max_date="2026-01-01", sequential=True)

        storage.record_scrape_run.assert_called_once()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.status == "error"
        assert run.error_category == ErrorCategory.ANTI_BOT

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_continues_if_tracking_fails(self, mock_load, mock_ws_cls):
        mock_load.return_value = {
            "mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True},
            "mds": {"url": "https://www.gov.br/mds", "scraper_type": "html", "active": True},
        }
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.return_value = [
            {"agency": "mec", "title": "Test", "published_at": "2026-01-01", "url": "http://x"}
        ]
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        storage.insert.return_value = 1
        # First call to record_scrape_run raises, second should still be called
        storage.record_scrape_run.side_effect = [Exception("DB down"), None]

        result = manager.run_scraper(
            agencies=["mec", "mds"], min_date="2026-01-01", max_date="2026-01-01", sequential=True
        )

        # Both agencies should be processed despite tracking failure
        assert len(result["agencies_processed"]) == 2
        assert storage.record_scrape_run.call_count == 2

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_measures_execution_time(self, mock_load, mock_ws_cls):
        mock_load.return_value = {"mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True}}
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.return_value = []
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        manager.run_scraper(agencies=["mec"], min_date="2026-01-01", max_date="2026-01-01", sequential=True)

        storage.record_scrape_run.assert_called_once()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.execution_time_seconds is not None
        assert run.execution_time_seconds >= 0

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_records_empty_no_error(self, mock_load, mock_ws_cls):
        """0 articles without error = success with articles_scraped=0."""
        mock_load.return_value = {"mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True}}
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.return_value = []  # No articles
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        manager.run_scraper(agencies=["mec"], min_date="2026-01-01", max_date="2026-01-01", sequential=True)

        storage.record_scrape_run.assert_called_once()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.status == "success"
        assert run.articles_scraped == 0
        assert run.error_category is None
