"""
Tests for yaml_config module - shared utilities for loading agency YAML configuration.
"""
import os
import pytest
from govbr_scraper.scrapers.yaml_config import (
    load_urls_from_yaml,
    extract_url,
    is_agency_inactive,
)


def get_config_dir():
    """Get the config directory path for tests."""
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
        "src",
        "govbr_scraper",
        "scrapers",
        "config",
    )


class TestExtractUrl:
    """Tests for extract_url function."""

    def test_extract_url_from_dict(self):
        """Dict format should return the 'url' field."""
        agency_data = {"url": "https://example.com/news", "active": True}
        assert extract_url(agency_data) == "https://example.com/news"

    def test_extract_url_from_dict_with_extra_fields(self):
        """Dict with extra fields should still return 'url'."""
        agency_data = {
            "url": "https://example.com/news",
            "active": False,
            "disabled_reason": "URL problematica",
            "disabled_date": "2025-01-15",
        }
        assert extract_url(agency_data) == "https://example.com/news"


class TestIsAgencyInactive:
    """Tests for is_agency_inactive function."""

    def test_dict_with_active_true(self):
        """Dict with active=True should return False (not inactive)."""
        agency_data = {"url": "https://example.com", "active": True}
        assert is_agency_inactive("test", agency_data) is False

    def test_dict_with_active_false(self):
        """Dict with active=False should return True (is inactive)."""
        agency_data = {"url": "https://example.com", "active": False}
        assert is_agency_inactive("test", agency_data) is True

    def test_dict_without_active_field(self):
        """Dict without 'active' field should default to active (return False)."""
        agency_data = {"url": "https://example.com"}
        assert is_agency_inactive("test", agency_data) is False

    def test_dict_with_disabled_reason(self):
        """Dict with disabled_reason should still check 'active' field."""
        agency_data = {
            "url": "https://example.com",
            "active": False,
            "disabled_reason": "Site fora do ar",
        }
        assert is_agency_inactive("test", agency_data) is True


class TestLoadUrlsFromYamlGovBr:
    """Tests for load_urls_from_yaml function with gov.br config."""

    def test_load_urls_returns_dict(self):
        """load_urls_from_yaml should return a dict mapping agency names to URLs."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        assert isinstance(agency_urls, dict)
        assert len(agency_urls) > 0

    def test_load_urls_filters_inactive(self):
        """Inactive agencies should not be in the returned dict."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        # cisc uses the generic gov.br/pt-br/noticias URL and is inactive
        for agency_name, url in agency_urls.items():
            assert url != "https://www.gov.br/pt-br/noticias"

    def test_load_specific_active_agency(self):
        """Loading a specific active agency should work."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="mec")
        assert len(agency_urls) == 1
        assert "mec" in agency_urls
        assert "mec" in agency_urls["mec"]

    def test_load_specific_inactive_agency_raises(self):
        """Loading a specific inactive agency should raise ValueError."""
        config_dir = get_config_dir()
        with pytest.raises(ValueError, match="inactive"):
            load_urls_from_yaml(config_dir, "site_urls.yaml", agency="cisc")

    def test_load_nonexistent_agency_raises(self):
        """Loading a nonexistent agency should raise ValueError."""
        config_dir = get_config_dir()
        with pytest.raises(ValueError, match="not found"):
            load_urls_from_yaml(config_dir, "site_urls.yaml", agency="nonexistent_agency_xyz")


class TestLoadUrlsFromYamlEBC:
    """Tests for load_urls_from_yaml function with EBC config."""

    def test_load_urls_returns_dict(self):
        """load_urls_from_yaml should return a dict mapping agency names to URLs."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")
        assert isinstance(agency_urls, dict)
        assert len(agency_urls) > 0

    def test_load_urls_filters_inactive(self):
        """Inactive agencies should not be in the returned dict."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")
        # memoria-ebc is inactive, so its URL should not be present
        for agency_name, url in agency_urls.items():
            assert "memoria.ebc.com.br" not in url

    def test_load_urls_includes_active_agencies(self):
        """Active agencies should be in the returned dict."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")
        urls_str = " ".join(agency_urls.values())
        # agencia_brasil and tvbrasil are active
        assert "agenciabrasil.ebc.com.br" in urls_str or "tvbrasil.ebc.com.br" in urls_str

    def test_load_specific_active_agency(self):
        """Loading a specific active agency should work."""
        config_dir = get_config_dir()
        agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml", agency="agencia_brasil")
        assert len(agency_urls) == 1
        assert "agencia_brasil" in agency_urls
        assert "agenciabrasil.ebc.com.br" in agency_urls["agencia_brasil"]

    def test_load_specific_inactive_agency_raises(self):
        """Loading a specific inactive agency should raise ValueError."""
        config_dir = get_config_dir()
        with pytest.raises(ValueError, match="inactive"):
            load_urls_from_yaml(config_dir, "ebc_urls.yaml", agency="memoria-ebc")

    def test_load_nonexistent_agency_raises(self):
        """Loading a nonexistent agency should raise ValueError."""
        config_dir = get_config_dir()
        with pytest.raises(ValueError, match="not found"):
            load_urls_from_yaml(config_dir, "ebc_urls.yaml", agency="nonexistent_agency_xyz")
