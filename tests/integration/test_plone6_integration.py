"""
Integration tests for Plone6APIScraper against real Plone 6 REST API.

Tests the Plone6APIScraper with real HTTP requests to validate that:
- The ++api++/@search endpoint is accessible and returns valid JSON
- Article extraction from JSON works correctly
- Date filtering is applied properly
"""

import pytest
import requests
from datetime import datetime, timedelta
from pathlib import Path
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.yaml_config import load_urls_from_yaml

# Choose 1 stable Plone6 agency for testing
# esporte is a good choice as it's a major government agency with consistent availability
PLONE6_AGENCY = "esporte"


@pytest.fixture(scope="module")
def plone6_agency_url():
    """Load Plone6 agency URL from YAML configuration."""
    config_dir = str(Path(__file__).parent.parent.parent / "src" / "govbr_scraper" / "scrapers" / "config")

    try:
        urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
    except Exception as e:
        pytest.skip(f"Could not load site_urls.yaml: {e}")

    if PLONE6_AGENCY not in urls:
        pytest.skip(f"{PLONE6_AGENCY} not found in site_urls.yaml")

    agency_data = urls[PLONE6_AGENCY]

    # Verify it's actually a Plone6 API agency
    if agency_data.get("scraper_type") != "plone6_api":
        pytest.skip(
            f"{PLONE6_AGENCY} is not a plone6_api agency "
            f"(type: {agency_data.get('scraper_type')})"
        )

    return agency_data["url"]


@pytest.mark.integration
class TestPlone6APIScraper:
    """Test Plone6APIScraper against real Plone 6 REST API."""

    def test_scrape_news_returns_articles(self, plone6_agency_url):
        """
        Plone6APIScraper.scrape_news() should return articles from real API.

        This test validates:
        - API is accessible and returns data
        - Articles are extracted correctly from JSON
        - Required fields (title, url, content, published_at) are present
        - Date filtering works (min_date constraint)
        """
        # Use recent date range to limit results and ensure data exists
        min_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        scraper = Plone6APIScraper(
            base_url=plone6_agency_url,
            min_date=min_date,
        )

        try:
            articles = scraper.scrape_news()
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch from API (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_news() raised unexpected error: {e}")

        # Should find at least some articles in the last 30 days
        assert len(articles) > 0, (
            f"No articles found for {PLONE6_AGENCY} in last 30 days. "
            f"This may indicate API changes or the agency hasn't published recently."
        )

        # Validate first article structure
        first = articles[0]

        # Required fields
        assert "title" in first, "title field is missing"
        assert first["title"], "title is empty"
        assert isinstance(first["title"], str), f"title should be str, got {type(first['title'])}"

        assert "url" in first, "url field is missing"
        assert first["url"], "url is empty"
        assert first["url"].startswith("https://"), f"url should be absolute HTTPS, got {first['url']}"

        assert "content" in first, "content field is missing"
        assert first["content"], "content is empty"
        assert isinstance(first["content"], str), f"content should be str, got {type(first['content'])}"

        assert "published_at" in first, "published_at field is missing"
        assert first["published_at"], "published_at is missing"
        assert isinstance(first["published_at"], datetime), (
            f"published_at should be datetime, got {type(first['published_at'])}"
        )

        # Validate date is within expected range
        # Note: published_at from Plone6 API is timezone-aware, so compare dates only
        min_date_dt = datetime.strptime(min_date, "%Y-%m-%d").date()
        assert first["published_at"].date() >= min_date_dt, (
            f"published_at {first['published_at'].date()} is before min_date {min_date_dt}"
        )

        # Optional but expected fields
        assert "agency" in first, "agency field is missing"

    def test_build_api_url_returns_valid_json(self, plone6_agency_url):
        """
        _build_api_url() should generate valid Plone 6 REST API URL.

        This test validates:
        - URL transformation (base URL -> ++api++/@search) works correctly
        - Generated URL returns valid JSON
        - JSON has expected Plone API structure (items array)
        """
        scraper = Plone6APIScraper(
            base_url=plone6_agency_url,
            min_date="2020-01-01",
        )

        # Build API URL for first page
        api_url = scraper._build_api_url(b_start=0, b_size=10)

        # Validate URL structure
        assert "++api++" in api_url, "API URL should contain ++api++ segment"
        assert "@search" in api_url, "API URL should contain @search endpoint"

        # Test actual HTTP request
        try:
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            pytest.skip(f"API request failed (network issue): {e}")

        # Should be valid JSON
        try:
            data = response.json()
        except ValueError as e:
            pytest.fail(f"API response is not valid JSON: {e}")

        # Validate Plone API response structure
        assert isinstance(data, dict), f"API response should be dict, got {type(data)}"
        assert "items" in data, "API response should contain 'items' key (Plone REST API format)"
        assert isinstance(data["items"], list), f"'items' should be a list, got {type(data['items'])}"

        # Should have pagination metadata
        assert "items_total" in data, "API response should contain 'items_total' (Plone pagination)"

    def test_date_filtering_works(self, plone6_agency_url):
        """
        Date filtering (min_date) should be applied correctly.

        This test validates that articles older than min_date are not returned.
        """
        # Use a recent cutoff date
        min_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        min_date_dt = datetime.strptime(min_date, "%Y-%m-%d")

        scraper = Plone6APIScraper(
            base_url=plone6_agency_url,
            min_date=min_date,
        )

        try:
            articles = scraper.scrape_news()
        except Exception as e:
            pytest.skip(f"Failed to fetch from API: {e}")

        if not articles:
            pytest.skip(f"No articles found for {PLONE6_AGENCY} in last 7 days")

        # All articles should be newer than min_date
        for i, article in enumerate(articles):
            published_at = article.get("published_at")
            assert published_at is not None, f"Article {i} has no published_at"
            assert isinstance(published_at, datetime), (
                f"Article {i} published_at should be datetime, got {type(published_at)}"
            )

            # Allow some timezone/boundary flexibility
            assert published_at.date() >= min_date_dt.date(), (
                f"Article {i} published_at {published_at} is before min_date {min_date_dt}"
            )
