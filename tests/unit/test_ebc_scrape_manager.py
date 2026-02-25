"""
Tests for EBCScrapeManager - agency active/inactive filtering.
"""
import pytest
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager


class TestEBCScrapeManagerExtractUrl:
    """Tests for _extract_url method."""

    def test_extract_url_from_dict(self):
        """Dict format should return the 'url' field."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {"url": "https://agenciabrasil.ebc.com.br/ultimas", "active": True}
        assert manager._extract_url(agency_data) == "https://agenciabrasil.ebc.com.br/ultimas"

    def test_extract_url_from_dict_with_extra_fields(self):
        """Dict with extra fields should still return 'url'."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {
            "url": "https://memoria.ebc.com.br/noticias",
            "active": False,
            "disabled_reason": "Site fora do ar (502 Bad Gateway)",
            "disabled_date": "2026-02-12",
        }
        assert manager._extract_url(agency_data) == "https://memoria.ebc.com.br/noticias"


class TestEBCScrapeManagerIsAgencyInactive:
    """Tests for _is_agency_inactive method."""

    def test_dict_with_active_true(self):
        """Dict with active=True should return False (not inactive)."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {"url": "https://example.com", "active": True}
        assert manager._is_agency_inactive("test", agency_data) is False

    def test_dict_with_active_false(self):
        """Dict with active=False should return True (is inactive)."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {"url": "https://example.com", "active": False}
        assert manager._is_agency_inactive("test", agency_data) is True

    def test_dict_without_active_field(self):
        """Dict without 'active' field should default to active (return False)."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {"url": "https://example.com"}
        assert manager._is_agency_inactive("test", agency_data) is False

    def test_dict_with_disabled_reason(self):
        """Dict with disabled_reason should still check 'active' field."""
        manager = EBCScrapeManager(storage=None)
        agency_data = {
            "url": "https://example.com",
            "active": False,
            "disabled_reason": "Site fora do ar",
        }
        assert manager._is_agency_inactive("test", agency_data) is True


class TestEBCScrapeManagerLoadUrlsFromYaml:
    """Tests for _load_urls_from_yaml method."""

    def test_load_urls_returns_list(self):
        """_load_urls_from_yaml should return a list of URLs."""
        manager = EBCScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("ebc_urls.yaml")
        assert isinstance(urls, list)
        assert len(urls) > 0

    def test_load_urls_filters_inactive(self):
        """Inactive agencies should not be in the returned list."""
        manager = EBCScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("ebc_urls.yaml")
        # memoria-ebc is inactive, so its URL should not be present
        for url in urls:
            assert "memoria.ebc.com.br" not in url

    def test_load_urls_includes_active_agencies(self):
        """Active agencies should be in the returned list."""
        manager = EBCScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("ebc_urls.yaml")
        urls_str = " ".join(urls)
        # agencia-brasil and tvbrasil are active
        assert "agenciabrasil.ebc.com.br" in urls_str or "tvbrasil.ebc.com.br" in urls_str

    def test_load_specific_active_agency(self):
        """Loading a specific active agency should work."""
        manager = EBCScrapeManager(storage=None)
        urls = manager._load_urls_from_yaml("ebc_urls.yaml", agency="agencia-brasil")
        assert len(urls) == 1
        assert "agenciabrasil.ebc.com.br" in urls[0]

    def test_load_specific_inactive_agency_raises(self):
        """Loading a specific inactive agency should raise ValueError."""
        manager = EBCScrapeManager(storage=None)
        with pytest.raises(ValueError, match="inactive"):
            manager._load_urls_from_yaml("ebc_urls.yaml", agency="memoria-ebc")

    def test_load_nonexistent_agency_raises(self):
        """Loading a nonexistent agency should raise ValueError."""
        manager = EBCScrapeManager(storage=None)
        with pytest.raises(ValueError, match="not found"):
            manager._load_urls_from_yaml("ebc_urls.yaml", agency="nonexistent_agency_xyz")


class TestEBCScrapeManagerConvertFormat:
    """Tests for EBC to govbr format conversion."""

    def test_convert_preserves_editorial_lead(self):
        """Editorial lead should be preserved during conversion."""
        manager = EBCScrapeManager(storage=None)
        ebc_data = [
            {
                "title": "Test Title",
                "url": "https://example.com/news/123",
                "content": "Test content here",
                "editorial_lead": "Caminhos da Reportagem",
                "agency": "tvbrasil",
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]["editorial_lead"] == "Caminhos da Reportagem"

    def test_convert_skips_items_with_errors(self):
        """Items with error field should be skipped."""
        manager = EBCScrapeManager(storage=None)
        ebc_data = [
            {"title": "Good", "url": "https://example.com/1", "content": "Content"},
            {"title": "Bad", "url": "https://example.com/2", "content": "Content", "error": "Failed"},
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]["title"] == "Good"

    def test_convert_skips_incomplete_items(self):
        """Items without title, url, or content should be skipped."""
        manager = EBCScrapeManager(storage=None)
        ebc_data = [
            {"title": "", "url": "https://example.com/1", "content": "Content"},
            {"title": "Title", "url": "", "content": "Content"},
            {"title": "Title", "url": "https://example.com/3", "content": ""},
            {"title": "Complete", "url": "https://example.com/4", "content": "Full content"},
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]["title"] == "Complete"
