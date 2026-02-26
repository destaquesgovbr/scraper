"""
Tests for EBCScrapeManager.

Note: Tests for YAML config loading functions (extract_url, is_agency_inactive,
load_urls_from_yaml) have been moved to test_yaml_config.py since these functions
are now in the shared yaml_config module.
"""
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager


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
