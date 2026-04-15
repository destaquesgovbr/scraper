"""
Tests for Plone6APIScraper integration in ScrapeManager (Strategy Pattern).

Verifies that ScrapeManager selects the correct scraper based on
the scraper_type field in the YAML config, and maintains backward
compatibility for agencies without the field.
"""
from unittest.mock import MagicMock, patch, call

import pytest

from govbr_scraper.scrapers.scrape_manager import ScrapeManager
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.webscraper import WebScraper


def _config(url, scraper_type="html"):
    return {"url": url, "scraper_type": scraper_type, "active": True}


def _make_manager():
    storage = MagicMock()
    storage.get_recent_urls.return_value = set()
    storage.insert.return_value = 0
    return ScrapeManager(storage), storage


@pytest.fixture
def mock_scrapers():
    """Fixture that patches both WebScraper and Plone6APIScraper."""
    with patch.object(WebScraper, "__init__", return_value=None) as mock_ws_init, \
         patch.object(WebScraper, "scrape_news", return_value=[]) as mock_ws_scrape, \
         patch.object(Plone6APIScraper, "__init__", return_value=None) as mock_p6_init, \
         patch.object(Plone6APIScraper, "scrape_news", return_value=[]) as mock_p6_scrape:
        yield {
            "ws_init": mock_ws_init,
            "ws_scrape": mock_ws_scrape,
            "p6_init": mock_p6_init,
            "p6_scrape": mock_p6_scrape,
        }


class TestScraperTypeSelection:
    """ScrapeManager must pick WebScraper or Plone6APIScraper from config."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_html_type_uses_webscraper(self, mock_load, mock_scrapers):
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias", "html")}
        manager, _ = _make_manager()

        manager.run_scraper(agencies=["mec"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        mock_scrapers["ws_init"].assert_called_once()
        mock_scrapers["p6_init"].assert_not_called()

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_plone6_api_type_uses_plone6_scraper(self, mock_load, mock_scrapers):
        mock_load.return_value = {
            "susep": _config("https://www.gov.br/susep/noticias", "plone6_api")
        }
        manager, _ = _make_manager()

        manager.run_scraper(agencies=["susep"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        mock_scrapers["p6_init"].assert_called_once()
        mock_scrapers["ws_init"].assert_not_called()

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_mixed_config_uses_both_scrapers(self, mock_load, mock_scrapers):
        """Both scrapers instantiated when config has mixed scraper types."""
        mock_load.return_value = {
            "mec":   _config("https://www.gov.br/mec/noticias", "html"),
            "susep": _config("https://www.gov.br/susep/noticias", "plone6_api"),
        }
        manager, _ = _make_manager()

        manager.run_scraper(agencies=["mec", "susep"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        # Both scrapers should be instantiated once each
        assert mock_scrapers["ws_init"].call_count == 1
        assert mock_scrapers["p6_init"].call_count == 1


class TestBackwardCompatibility:
    """Agencies without scraper_type must default to WebScraper."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_missing_scraper_type_defaults_to_webscraper(self, mock_load, mock_scrapers):
        mock_load.return_value = {
            "mec": {"url": "https://www.gov.br/mec/noticias", "active": True}
            # scraper_type ausente
        }
        manager, _ = _make_manager()

        manager.run_scraper(agencies=["mec"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        mock_scrapers["ws_init"].assert_called_once()
        mock_scrapers["p6_init"].assert_not_called()


class TestKnownUrlsPassthrough:
    """known_urls from storage must be forwarded to whichever scraper is used."""

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_known_urls_passed_to_webscraper(self, mock_load, mock_scrapers):
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias")}
        manager, storage = _make_manager()
        known = {"https://www.gov.br/mec/noticia-1"}
        storage.get_recent_urls.return_value = known

        manager.run_scraper(agencies=["mec"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        _, kwargs = mock_scrapers["ws_init"].call_args
        assert kwargs["known_urls"] == known

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_known_urls_passed_to_plone6_scraper(self, mock_load, mock_scrapers):
        mock_load.return_value = {
            "susep": _config("https://www.gov.br/susep/noticias", "plone6_api")
        }
        manager, storage = _make_manager()
        known = {"https://www.gov.br/susep/noticia-1"}
        storage.get_recent_urls.return_value = known

        manager.run_scraper(agencies=["susep"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        _, kwargs = mock_scrapers["p6_init"].call_args
        assert kwargs["known_urls"] == known

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_calls_get_recent_urls_with_agency_name(self, mock_load, mock_scrapers):
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias")}
        manager, storage = _make_manager()

        manager.run_scraper(agencies=["mec"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        storage.get_recent_urls.assert_called_once_with("mec")

    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_falls_back_to_empty_set_when_storage_fails(self, mock_load, mock_scrapers):
        mock_load.return_value = {"mec": _config("https://www.gov.br/mec/noticias")}
        manager, storage = _make_manager()
        storage.get_recent_urls.side_effect = Exception("DB down")

        manager.run_scraper(agencies=["mec"], min_date="2026-01-01",
                            max_date="2026-01-31", sequential=True)

        _, kwargs = mock_scrapers["ws_init"].call_args
        assert kwargs["known_urls"] == set()
