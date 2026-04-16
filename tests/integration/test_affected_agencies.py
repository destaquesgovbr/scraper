"""
Integration tests for Issue #19 - Parser HTML desatualizado.

Validates that the scraper correctly extracts news from the 12 affected agencies
by making real HTTP requests to the gov.br pages.

These tests:
1. Fetch real pages from affected agencies
2. Validate the scraper finds articles (core fix for Issue #19)
3. Validate extraction of title, url, date using production WebScraper code
"""

import pytest
import requests
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from govbr_scraper.scrapers.webscraper import WebScraper
from govbr_scraper.scrapers.yaml_config import load_urls_from_yaml


# =============================================================================
# Configuration
# =============================================================================

_CONFIG_DIR = str(Path(__file__).parent.parent.parent / "src" / "govbr_scraper" / "scrapers" / "config")
_ALL_URLS = load_urls_from_yaml(_CONFIG_DIR, "site_urls.yaml")

# Opt-in: only agencies explicitly listed here are used in integration tests.
# Adding an agency to site_urls.yaml does NOT automatically add it here.
_WORKING_AGENCY_KEY = "mec"

# Agencies that work with BeautifulSoup (traditional HTML templates, scraper_type: html)
_SCRAPABLE_AGENCY_KEYS = ["palmares", "sudam", "cemaden", "semanaenef", "sri", "ctir"]

# Working agency for baseline comparison
WORKING_AGENCY = {
    "key": _WORKING_AGENCY_KEY,
    "url": _ALL_URLS[_WORKING_AGENCY_KEY]["url"],
}

# Agencies that work with BeautifulSoup (traditional HTML templates)
SCRAPABLE_AGENCIES = {key: _ALL_URLS[key]["url"] for key in _SCRAPABLE_AGENCY_KEYS}

# Agencies with scraper_type: plone6_api (ctav, esg, esporte, hfa, memp,
# patrimonio, pncp, povosindigenas, propriedade-intelectual, reconstrucaors, susep)
# are covered by test_plone6_integration.py, not tested here.

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}

REQUEST_TIMEOUT = 30


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def working_agency_data():
    """Fetch and parse the working agency page."""
    try:
        response = requests.get(
            WORKING_AGENCY["url"], headers=HEADERS, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        return {
            "html": response.content,
            "url": WORKING_AGENCY["url"],
            "key": WORKING_AGENCY["key"],
        }
    except requests.RequestException as e:
        pytest.skip(f"Could not fetch working agency: {e}")


@pytest.fixture(scope="module")
def affected_agencies_data():
    """Fetch and parse pages from scrapable agencies used in tests."""
    data = {}
    for agency_key, url in SCRAPABLE_AGENCIES.items():
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data[agency_key] = {"html": response.content, "url": url}
        except requests.RequestException as e:
            data[agency_key] = {"error": str(e), "url": url}
    return data


def create_scraper(base_url: str) -> WebScraper:
    """Create a WebScraper instance for testing."""
    return WebScraper(base_url=base_url, min_date="2020-01-01")


# =============================================================================
# Tests - Scraper Finds Articles (Core Fix for Issue #19)
# =============================================================================


@pytest.mark.integration
class TestScraperFindsArticles:
    """
    Core test: Verify that the scraper finds articles on scrapable agencies
    using the production WebScraper.scrape_page() code.

    Before the fix, these agencies returned 0 articles with large response sizes,
    causing ScrapingError. After the fix, articles should be found.

    Note: SPA agencies (Volto/React) are excluded as they require JavaScript.
    """

    @pytest.mark.parametrize("agency_key", SCRAPABLE_AGENCIES.keys())
    def test_finds_articles_on_scrapable_agency(self, affected_agencies_data, agency_key):
        """The scraper must find at least 1 article on each scrapable agency."""
        page_data = affected_agencies_data.get(agency_key)

        if "error" in page_data:
            pytest.skip(f"Could not fetch {agency_key}: {page_data['error']}")

        scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])

        # Mock fetch_page to inject pre-fetched HTML
        mock_response = MagicMock()
        mock_response.content = page_data["html"]
        mock_response.url = page_data["url"]

        with patch.object(scraper, 'fetch_page', return_value=mock_response):
            # Use production scrape_page() which includes proper Fallback 3 filtering
            should_continue, items_count = scraper.scrape_page(page_data["url"])

        response_size = len(page_data["html"])
        assert items_count > 0, (
            f"ISSUE #19 NOT FIXED: No articles found for {agency_key}. "
            f"Response size: {response_size} bytes. "
            f"This would cause ScrapingError in production."
        )

    def test_working_agency_still_works(self, working_agency_data):
        """Baseline: working agency (mec) should continue to find articles."""
        scraper = create_scraper(WORKING_AGENCY["url"])

        mock_response = MagicMock()
        mock_response.content = working_agency_data["html"]
        mock_response.url = working_agency_data["url"]

        with patch.object(scraper, 'fetch_page', return_value=mock_response):
            should_continue, items_count = scraper.scrape_page(working_agency_data["url"])

        assert items_count > 0, "Working agency should find articles"


# =============================================================================
# Tests - Scraper Extracts Required Fields
# =============================================================================


@pytest.mark.integration
class TestScraperExtractsFields:
    """
    Verify that the scraper extracts the same fields from scrapable agencies
    that it extracts from working agencies: title, url, date.

    Uses production WebScraper code to ensure real extraction logic is tested.

    Note: SPA agencies are excluded as they require JavaScript.
    """

    @pytest.mark.parametrize("agency_key", SCRAPABLE_AGENCIES.keys())
    def test_extracts_title_and_url(self, affected_agencies_data, agency_key):
        """Scrapable agencies must extract valid title and URL from at least some items."""
        page_data = affected_agencies_data.get(agency_key)

        if "error" in page_data:
            pytest.skip(f"Could not fetch {agency_key}: {page_data['error']}")

        scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])

        # Mock fetch_page and extract_news_info to get parsed items
        mock_response = MagicMock()
        mock_response.content = page_data["html"]
        mock_response.url = page_data["url"]

        # Track extracted items by mocking extract_news_info
        extracted_items = []

        def mock_extract_news_info(item):
            # Store the item for testing
            extracted_items.append(item)
            # Return True to continue processing (don't stop early)
            # In production, this would fetch article content and check dates
            return True

        with patch.object(scraper, 'fetch_page', return_value=mock_response):
            with patch.object(scraper, 'extract_news_info', side_effect=mock_extract_news_info):
                scraper.scrape_page(page_data["url"])

        if not extracted_items:
            pytest.skip(f"No articles found for {agency_key}")

        # Check first 5 items for valid titles (some may be images without text)
        valid_titles = 0
        for item in extracted_items[:5]:
            title, url = scraper.extract_title_and_url(item)
            if title != "No Title" and len(title.strip()) > 0:
                valid_titles += 1

        assert valid_titles > 0, f"{agency_key}: No valid titles found in first 5 items"

    @pytest.mark.parametrize("agency_key", SCRAPABLE_AGENCIES.keys())
    def test_extracts_date(self, affected_agencies_data, agency_key):
        """Scrapable agencies must extract valid date from at least some articles."""
        page_data = affected_agencies_data.get(agency_key)

        if "error" in page_data:
            pytest.skip(f"Could not fetch {agency_key}: {page_data['error']}")

        scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])

        # Mock fetch_page and extract_news_info to get parsed items
        mock_response = MagicMock()
        mock_response.content = page_data["html"]
        mock_response.url = page_data["url"]

        extracted_items = []

        def mock_extract_news_info(item):
            extracted_items.append(item)
            return True

        with patch.object(scraper, 'fetch_page', return_value=mock_response):
            with patch.object(scraper, 'extract_news_info', side_effect=mock_extract_news_info):
                scraper.scrape_page(page_data["url"])

        if not extracted_items:
            pytest.skip(f"No articles found for {agency_key}")

        # Check first 5 articles for dates
        dates_extracted = 0
        for item in extracted_items[:5]:
            extracted_date = scraper.extract_date(item)
            if extracted_date is not None:
                dates_extracted += 1
                assert isinstance(extracted_date, date)
                assert 2015 <= extracted_date.year <= datetime.now().year + 1

        assert dates_extracted > 0, (
            f"{agency_key}: Could not extract date from any of the first 5 articles"
        )
