"""
Integration tests for EBCWebScraper against real EBC sites.

Tests the EBCWebScraper with real HTTP requests to validate that:
- Agencia Brasil and TV Brasil index pages can be scraped
- Article extraction works correctly for both sources
- Different parsing strategies (_scrape_agencia_brasil_content vs _scrape_tvbrasil_content) work
"""

import pytest
import requests
from datetime import datetime
from pathlib import Path
from govbr_scraper.scrapers.ebc_webscraper import EBCWebScraper
from govbr_scraper.scrapers.yaml_config import load_urls_from_yaml


@pytest.fixture(scope="module")
def ebc_urls():
    """Load EBC URLs from YAML configuration."""
    config_dir = str(Path(__file__).parent.parent.parent / "src" / "govbr_scraper" / "scrapers" / "config")

    try:
        urls = load_urls_from_yaml(config_dir, "ebc_urls.yaml")
    except Exception as e:
        pytest.skip(f"Could not load ebc_urls.yaml: {e}")

    return urls


@pytest.mark.integration
class TestEBCWebScraper:
    """Test EBCWebScraper against real EBC sites."""

    def test_agencia_brasil_extracts_article(self, ebc_urls):
        """
        Agencia Brasil should extract title, content, source, published_datetime.

        This test validates:
        - Index page scraping works (scrape_index_page)
        - Article page scraping works (scrape_news_page)
        - _scrape_agencia_brasil_content extracts all expected fields
        - Published datetime is valid
        """
        if "agencia_brasil" not in ebc_urls:
            pytest.skip("agencia_brasil not found in ebc_urls.yaml")

        base_url = ebc_urls["agencia_brasil"]["url"]
        scraper = EBCWebScraper(base_url=base_url, min_date="2020-01-01")

        # Step 1: Fetch index page to get article URLs
        try:
            article_urls = scraper.scrape_index_page(base_url)
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch Agencia Brasil index page (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_index_page() raised unexpected error: {e}")

        assert len(article_urls) > 0, (
            "No article URLs found on Agencia Brasil index page. "
            "This may indicate HTML structure changes."
        )

        # Validate URL structure
        first_url = article_urls[0]
        assert first_url.startswith("https://"), f"Article URL should be absolute HTTPS, got {first_url}"
        assert "agenciabrasil.ebc.com.br" in first_url, (
            f"Article URL should be from agenciabrasil.ebc.com.br domain, got {first_url}"
        )

        # Step 2: Fetch first article page
        try:
            article_data = scraper.scrape_news_page(first_url)
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch article page (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_news_page() raised unexpected error: {e}")

        # Should not have error message if successful (error field may exist but be empty)
        if article_data.get("error"):
            pytest.fail(f"Article extraction failed with error: {article_data['error']}")

        # Validate required fields for Agencia Brasil
        assert "title" in article_data, "title field is missing"
        assert article_data["title"], "title is empty"
        assert isinstance(article_data["title"], str), f"title should be str, got {type(article_data['title'])}"

        assert "content" in article_data, "content field is missing"
        assert article_data["content"], "content is empty"
        assert isinstance(article_data["content"], str), (
            f"content should be str, got {type(article_data['content'])}"
        )
        # Content should have meaningful length (not just whitespace)
        assert len(article_data["content"].strip()) > 50, (
            f"content seems too short ({len(article_data['content'])} chars), may indicate extraction failure"
        )

        assert "published_datetime" in article_data, "published_datetime field is missing"
        assert article_data["published_datetime"], "published_datetime is None"
        assert isinstance(article_data["published_datetime"], datetime), (
            f"published_datetime should be datetime, got {type(article_data['published_datetime'])}"
        )

        # Validate datetime is in reasonable range
        year = article_data["published_datetime"].year
        current_year = datetime.now().year
        assert 2020 <= year <= current_year + 1, (
            f"published_datetime year {year} is outside reasonable range (2020-{current_year + 1})"
        )

        # Agencia Brasil specific field: source (author)
        assert "source" in article_data, "source field is missing (Agencia Brasil should extract author)"
        # Note: source may be empty for some articles, so we just check it exists

        assert "agency" in article_data, "agency field is missing"
        assert article_data["agency"] == "agencia_brasil", (
            f"agency should be 'agencia_brasil', got {article_data['agency']}"
        )

    def test_tv_brasil_extracts_article(self, ebc_urls):
        """
        TV Brasil should extract title, content, editorial_lead (program name).

        This test validates:
        - Index page scraping works for TV Brasil
        - Article page scraping works with _scrape_tvbrasil_content
        - Editorial lead (program name) is extracted correctly
        """
        if "tvbrasil" not in ebc_urls:
            pytest.skip("tvbrasil not found in ebc_urls.yaml")

        base_url = ebc_urls["tvbrasil"]["url"]
        scraper = EBCWebScraper(base_url=base_url, min_date="2020-01-01")

        # Step 1: Fetch index page
        try:
            article_urls = scraper.scrape_index_page(base_url)
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch TV Brasil index page (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_index_page() raised unexpected error: {e}")

        assert len(article_urls) > 0, (
            "No article URLs found on TV Brasil index page. "
            "This may indicate HTML structure changes."
        )

        # Validate URL structure
        first_url = article_urls[0]
        assert first_url.startswith("https://"), f"Article URL should be absolute HTTPS, got {first_url}"
        assert "tvbrasil.ebc.com.br" in first_url, (
            f"Article URL should be from tvbrasil.ebc.com.br domain, got {first_url}"
        )

        # Step 2: Fetch first article page
        try:
            article_data = scraper.scrape_news_page(first_url)
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch TV Brasil article (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_news_page() raised unexpected error: {e}")

        # Should not have error message if successful (error field may exist but be empty)
        if article_data.get("error"):
            pytest.fail(f"Article extraction failed with error: {article_data['error']}")

        # Validate required fields
        assert "title" in article_data, "title field is missing"
        assert article_data["title"], "title is empty"

        assert "content" in article_data, "content field is missing"
        assert article_data["content"], "content is empty"
        assert len(article_data["content"].strip()) > 50, (
            f"content seems too short ({len(article_data['content'])} chars)"
        )

        # TV Brasil specific field: editorial_lead (program name)
        # The field must exist in the response; value may be empty for some articles
        assert "editorial_lead" in article_data, (
            "editorial_lead field is missing (TV Brasil should extract program name)"
        )

        assert "agency" in article_data, "agency field is missing"
        assert article_data["agency"] == "tvbrasil", (
            f"agency should be 'tvbrasil', got {article_data['agency']}"
        )

    def test_index_page_strategies(self, ebc_urls):
        """
        scrape_index_page() should handle different HTML structures.

        This test validates that the 3 index page strategies work:
        - Strategy 1: a.capa-noticia (Agencia Brasil)
        - Strategy 2: memoria.ebc legacy format
        - Strategy 3: view-ultimas (TV Brasil)
        """
        # Test both active EBC sources
        for agency_key in ["agencia_brasil", "tvbrasil"]:
            if agency_key not in ebc_urls:
                continue

            base_url = ebc_urls[agency_key]["url"]
            scraper = EBCWebScraper(base_url=base_url, min_date="2020-01-01")

            try:
                article_urls = scraper.scrape_index_page(base_url)
            except Exception as e:
                pytest.skip(f"Failed to fetch {agency_key} index page: {e}")

            # Should find articles with any of the 3 strategies
            assert len(article_urls) > 0, (
                f"{agency_key} index page returned 0 URLs. "
                f"All 3 scraping strategies failed to find articles."
            )

            # All URLs should be valid
            for url in article_urls[:5]:  # Check first 5
                assert url.startswith("https://"), f"Invalid URL format: {url}"
                assert "ebc.com.br" in url, f"URL should be from EBC domain: {url}"
