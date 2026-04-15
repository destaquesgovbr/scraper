"""
Tests for yaml_config module - shared utilities for loading agency YAML configuration.
"""
import os
import pytest
from govbr_scraper.scrapers.yaml_config import (
    get_config_dir,
    load_urls_from_yaml,
    extract_url,
    is_agency_inactive,
)

# Path to the scrapers module, used to resolve config dir in tests
_SCRAPERS_MODULE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "src",
    "govbr_scraper",
    "scrapers",
    "scrape_manager.py",
)


class TestGetConfigDir:
    """Tests for get_config_dir function."""

    def test_returns_config_subdir(self):
        """get_config_dir should return a path ending in 'config'."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        assert config_dir.endswith("config")

    def test_config_dir_exists(self):
        """get_config_dir should return an existing directory."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        assert os.path.isdir(config_dir)


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


class TestLoadUrlsFromYaml:
    """Tests for load_urls_from_yaml function with both gov.br and EBC configs."""

    @pytest.mark.parametrize("yaml_file,expected_agency,expected_url_pattern", [
        ("site_urls.yaml", "mec", "gov.br/mec"),
        ("ebc_urls.yaml", "agencia_brasil", "agenciabrasil.ebc.com.br"),
    ])
    def test_load_urls_returns_dict(self, yaml_file, expected_agency, expected_url_pattern):
        """load_urls_from_yaml should return a dict mapping agency names to URLs."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, yaml_file)
        assert isinstance(agency_urls, dict)
        assert len(agency_urls) > 0

    @pytest.mark.parametrize("yaml_file,inactive_url_pattern", [
        ("site_urls.yaml", "https://www.gov.br/pt-br/noticias"),  # cisc is inactive
        ("ebc_urls.yaml", "memoria.ebc.com.br"),  # memoria-ebc is inactive
    ])
    def test_load_urls_filters_inactive(self, yaml_file, inactive_url_pattern):
        """Inactive agencies should not be in the returned dict."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, yaml_file)
        for agency_name, config in agency_urls.items():
            assert inactive_url_pattern not in config["url"]

    def test_load_urls_includes_active_ebc_agencies(self):
        """Active EBC agencies should be in the returned dict."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")
        urls_str = " ".join(config["url"] for config in agency_urls.values())
        # agencia_brasil and tvbrasil are active
        assert "agenciabrasil.ebc.com.br" in urls_str or "tvbrasil.ebc.com.br" in urls_str

    @pytest.mark.parametrize("yaml_file,agency,expected_url_pattern", [
        ("site_urls.yaml", "mec", "mec"),
        ("ebc_urls.yaml", "agencia_brasil", "agenciabrasil.ebc.com.br"),
    ])
    def test_load_specific_active_agency(self, yaml_file, agency, expected_url_pattern):
        """Loading a specific active agency should work."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, yaml_file, agency=agency)
        assert len(agency_urls) == 1
        assert agency in agency_urls
        assert expected_url_pattern in agency_urls[agency]["url"]

    @pytest.mark.parametrize("yaml_file,inactive_agency", [
        ("site_urls.yaml", "cisc"),
        ("ebc_urls.yaml", "memoria-ebc"),
    ])
    def test_load_specific_inactive_agency_raises(self, yaml_file, inactive_agency):
        """Loading a specific inactive agency should raise ValueError."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        with pytest.raises(ValueError, match="inactive"):
            load_urls_from_yaml(config_dir, yaml_file, agency=inactive_agency)

    @pytest.mark.parametrize("yaml_file", ["site_urls.yaml", "ebc_urls.yaml"])
    def test_load_nonexistent_agency_raises(self, yaml_file):
        """Loading a nonexistent agency should raise ValueError."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        with pytest.raises(ValueError, match="not found"):
            load_urls_from_yaml(config_dir, yaml_file, agency="nonexistent_agency_xyz")


class TestLoadUrlsReturnsConfigDict:
    """Tests for the dict structure returned by load_urls_from_yaml."""

    def test_values_are_dicts_not_strings(self):
        """Each value in the returned dict must be a config dict, not a URL string."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        for agency_name, config in agency_urls.items():
            assert isinstance(config, dict), f"{agency_name}: expected dict, got {type(config)}"

    def test_config_dict_has_url_field(self):
        """Each config dict must contain a 'url' key."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="mec")
        assert "url" in agency_urls["mec"]
        assert "mec" in agency_urls["mec"]["url"]

    def test_config_dict_has_scraper_type_field(self):
        """Each config dict must contain a 'scraper_type' key."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        for agency_name, config in agency_urls.items():
            assert "scraper_type" in config, f"{agency_name} missing scraper_type"

    def test_plone6_agencies_have_correct_scraper_type(self):
        """Plone 6 agencies must be configured with scraper_type=plone6_api."""
        plone6_agencies = ["susep", "patrimonio", "propriedade-intelectual", "pncp"]
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        for agency in plone6_agencies:
            result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency=agency)
            assert result[agency]["scraper_type"] == "plone6_api", \
                f"{agency} should have scraper_type=plone6_api"

    def test_regular_agencies_default_to_html_scraper_type(self):
        """Agencies without explicit scraper_type must default to 'html'."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="mec")
        assert result["mec"]["scraper_type"] == "html"

    def test_scraper_type_values_are_valid(self):
        """All scraper_type values must be within the known set."""
        valid_types = {"html", "plone6_api"}
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        for agency_name, config in agency_urls.items():
            assert config["scraper_type"] in valid_types, \
                f"{agency_name}: unknown scraper_type '{config['scraper_type']}'"

    def test_config_dict_has_active_field(self):
        """Each config dict must contain an 'active' key set to True (inactive filtered out)."""
        config_dir = get_config_dir(_SCRAPERS_MODULE)
        agency_urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
        for agency_name, config in agency_urls.items():
            assert "active" in config, f"{agency_name} missing active field"
            assert config["active"] is True, f"{agency_name}: active should be True (inactive are filtered)"
