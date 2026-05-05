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


    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_non_sequential_logs_articles_scraped_as_saved(self, mock_load, mock_ws_cls):
        """Non-sequential mode must not log articles_saved=0 for successful scrapes."""
        mock_load.return_value = {"mec": {"url": "https://www.gov.br/mec", "scraper_type": "html", "active": True}}
        mock_scraper = MagicMock()
        mock_scraper.scrape_news.return_value = [
            {"agency": "mec", "title": "Test", "published_at": "2026-01-01", "url": "http://x"},
            {"agency": "mec", "title": "Test2", "published_at": "2026-01-01", "url": "http://y"},
        ]
        mock_ws_cls.return_value = mock_scraper

        manager, storage = self._make_manager()
        storage.insert.return_value = 2
        manager.run_scraper(agencies=["mec"], min_date="2026-01-01", max_date="2026-01-01", sequential=False)

        storage.record_scrape_run.assert_called_once()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.articles_saved > 0, f"Expected articles_saved > 0, got {run.articles_saved}"


class TestScrapeManagerPreprocessing:
    """ScrapeManager data preprocessing and unique ID generation."""

    def _make_manager(self, mock_storage=None):
        from govbr_scraper.scrapers.scrape_manager import ScrapeManager

        storage = mock_storage or MagicMock()
        storage.get_recent_urls.return_value = set()
        return ScrapeManager(storage), storage

    def test_preprocess_data_generates_unique_id(self):
        """_preprocess_data should generate unique_id for each article."""
        manager, storage = self._make_manager()

        data = [
            {
                "agency": "mec",
                "published_at": "2026-01-15T14:30:00Z",
                "title": "Nova política educacional",
                "url": "https://www.gov.br/mec/noticia",
                "content": "Conteúdo da notícia",
            }
        ]

        result = manager._preprocess_data(data)

        assert "unique_id" in result
        assert len(result["unique_id"]) == 1
        assert isinstance(result["unique_id"][0], str)
        assert len(result["unique_id"][0]) > 0

    def test_preprocess_data_converts_to_columnar(self):
        """_preprocess_data should convert list of dicts to columnar OrderedDict."""
        from collections import OrderedDict

        manager, storage = self._make_manager()

        data = [
            {
                "agency": "mec",
                "published_at": "2026-01-15T14:30:00Z",
                "title": "Notícia 1",
                "url": "https://www.gov.br/mec/noticia1",
                "content": "Conteúdo 1",
            },
            {
                "agency": "mec",
                "published_at": "2026-01-16T10:00:00Z",
                "title": "Notícia 2",
                "url": "https://www.gov.br/mec/noticia2",
                "content": "Conteúdo 2",
            },
        ]

        result = manager._preprocess_data(data)

        assert isinstance(result, OrderedDict)
        assert "title" in result
        assert "url" in result
        assert "agency" in result
        assert len(result["title"]) == 2
        assert result["title"][0] == "Notícia 1"
        assert result["title"][1] == "Notícia 2"

    def test_generate_unique_id_delegates_to_module(self):
        """_generate_unique_id should delegate to unique_id module."""
        from govbr_scraper.scrapers import unique_id

        manager, storage = self._make_manager()

        # Test that it produces a valid unique ID
        result = manager._generate_unique_id(
            agency="mec",
            published_at_value="2026-01-15T14:30:00Z",
            title="Nova política educacional"
        )

        # Should be in format: slug_suffix (e.g., "nova-politica-educacional_abc123")
        assert isinstance(result, str)
        assert "_" in result  # Should have underscore separator
        parts = result.split("_")
        assert len(parts) == 2  # slug and suffix
        assert len(parts[1]) == 6  # suffix is 6 characters

    def test_preprocess_preserves_all_fields(self):
        """_preprocess_data should preserve all input fields."""
        manager, storage = self._make_manager()

        data = [
            {
                "agency": "mec",
                "published_at": "2026-01-15T14:30:00Z",
                "title": "Notícia",
                "url": "https://www.gov.br/mec/noticia",
                "content": "Conteúdo",
                "category": "Educação",
                "tags": ["educacao", "ensino"],
                "image": "https://img.com/photo.jpg",
                "editorial_lead": "Especial",
                "subtitle": "Subtítulo",
            }
        ]

        result = manager._preprocess_data(data)

        assert "category" in result
        assert "tags" in result
        assert "image" in result
        assert "editorial_lead" in result
        assert "subtitle" in result
        assert result["category"][0] == "Educação"
        assert result["tags"][0] == ["educacao", "ensino"]
