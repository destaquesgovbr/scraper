"""
Tests for ScrapeManager - agency active/inactive filtering.
"""
import pytest
from govbr_scraper.scrapers.scrape_manager import ScrapeManager


class TestScrapeManagerExtractUrl:
    """Tests for _extract_url method."""

    def test_extract_url_from_dict(self):
        """Dict format should return the 'url' field."""
        manager = ScrapeManager(storage=None)
        agency_data = {"url": "https://example.com/news", "active": True}
        assert manager._extract_url(agency_data) == "https://example.com/news"

    def test_extract_url_from_dict_with_extra_fields(self):
        """Dict with extra fields should still return 'url'."""
        manager = ScrapeManager(storage=None)
        agency_data = {
            "url": "https://example.com/news",
            "active": False,
            "disabled_reason": "URL problematica",
            "disabled_date": "2025-01-15",
        }
        assert manager._extract_url(agency_data) == "https://example.com/news"


class TestScrapeManagerIsAgencyInactive:
    """Tests for _is_agency_inactive method."""

    def test_dict_with_active_true(self):
        """Dict with active=True should return False (not inactive)."""
        manager = ScrapeManager(storage=None)
        agency_data = {"url": "https://example.com", "active": True}
        assert manager._is_agency_inactive("test", agency_data) is False

    def test_dict_with_active_false(self):
        """Dict with active=False should return True (is inactive)."""
        manager = ScrapeManager(storage=None)
        agency_data = {"url": "https://example.com", "active": False}
        assert manager._is_agency_inactive("test", agency_data) is True

    def test_dict_without_active_field(self):
        """Dict without 'active' field should default to active (return False)."""
        manager = ScrapeManager(storage=None)
        agency_data = {"url": "https://example.com"}
        assert manager._is_agency_inactive("test", agency_data) is False

    def test_dict_with_disabled_reason(self):
        """Dict with disabled_reason should still check 'active' field."""
        manager = ScrapeManager(storage=None)
        agency_data = {
            "url": "https://example.com",
            "active": False,
            "disabled_reason": "Site fora do ar",
        }
        assert manager._is_agency_inactive("test", agency_data) is True


class TestScrapeManagerLoadUrlsFromYaml:
    """Tests for _load_urls_from_yaml method."""

    def test_load_urls_returns_list(self):
        """_load_urls_from_yaml should return a list of URLs."""
        manager = ScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("site_urls.yaml")
        assert isinstance(urls, list)
        assert len(urls) > 0

    def test_load_urls_filters_inactive(self):
        """Inactive agencies should not be in the returned list."""
        manager = ScrapeManager(storage=None)
        # site_urls.yaml has some inactive agencies (cisc, ibde, etc.)
        urls = manager._load_urls_from_yaml("site_urls.yaml")
        # None of the URLs should contain patterns from inactive agencies
        for url in urls:
            # cisc uses the generic gov.br/pt-br/noticias URL
            assert url != "https://www.gov.br/pt-br/noticias"

    def test_load_specific_active_agency(self):
        """Loading a specific active agency should work."""
        manager = ScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("site_urls.yaml", agency="mec")
        assert len(urls) == 1
        assert "mec" in urls[0]

    def test_load_specific_inactive_agency_raises(self):
        """Loading a specific inactive agency should raise ValueError."""
        manager = ScrapeManager(storage=None)
        with pytest.raises(ValueError, match="inactive"):
            manager._load_urls_from_yaml("site_urls.yaml", agency="cisc")

    def test_load_nonexistent_agency_raises(self):
        """Loading a nonexistent agency should raise ValueError."""
        manager = ScrapeManager(storage=None)
        with pytest.raises(ValueError, match="not found"):
            manager._load_urls_from_yaml("site_urls.yaml", agency="nonexistent_agency_xyz")
