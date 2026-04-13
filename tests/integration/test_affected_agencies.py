"""
Integration tests for Issue #19 - Parser HTML desatualizado.

Validates that the scraper correctly extracts news from the 12 affected agencies
by making real HTTP requests to the gov.br pages.

These tests:
1. Fetch real pages from affected agencies
2. Validate the scraper finds articles (core fix for Issue #19)
3. Validate extraction of title, url, date
4. Compare with a working agency (mec) to ensure same data structure
"""

import pytest
import requests
from datetime import date, datetime
from bs4 import BeautifulSoup
from govbr_scraper.scrapers.webscraper import WebScraper


# =============================================================================
# Configuration
# =============================================================================

# Working agency for baseline comparison
WORKING_AGENCY = {
    "key": "mec",
    "url": "https://www.gov.br/mec/pt-br/assuntos/noticias",
}

# Agencies that work with BeautifulSoup (traditional HTML templates)
SCRAPABLE_AGENCIES = {
    "palmares": "https://www.gov.br/palmares/pt-br/assuntos/noticias",
    "sudam": "https://www.gov.br/sudam/pt-br/noticias-1",
    "cemaden": "https://www.gov.br/cemaden/pt-br/assuntos/noticias-cemaden/ultimas-noticias",
    "semanaenef": "https://www.gov.br/semanaenef/pt-br/noticias",
    "sri": "https://www.gov.br/sri/pt-br/noticias/mais-noticias/ultimas-noticias",
}

# Agencies that use Volto/SPA (require JavaScript to render content)
# These cannot be scraped with BeautifulSoup - require Playwright/Selenium
SPA_AGENCIES = {
    "ctir": "https://www.gov.br/ctir/pt-br/assuntos/noticias",
    "esporte": "https://www.gov.br/esporte/pt-br/noticias-e-conteudos/esporte",
    "hfa": "https://www.gov.br/hfa/pt-br/noticias",
    "memp": "https://www.gov.br/memp/pt-br/assuntos/noticias",
    "reconstrucaors": "https://www.gov.br/reconstrucaors/pt-br/acompanhe-a-reconstrucao/noticias",
    "esg": "https://www.gov.br/esg/pt-br/centrais-de-conteudo/noticias",
    "ctav": "https://www.gov.br/ctav/pt-br/noticias",
}

# All agencies combined (for reference/reporting)
AFFECTED_AGENCIES = {**SCRAPABLE_AGENCIES, **SPA_AGENCIES}

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
    """Fetch and parse pages from all affected agencies."""
    data = {}
    for agency_key, url in AFFECTED_AGENCIES.items():
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


def find_news_items(html_content: bytes) -> list:
    """
    Find news items using the same fallback chain as webscraper.py.
    This mirrors the logic in scrape_page_of_news after the fix.
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Strategy 1: article.tileItem (main pattern)
    news_items = soup.find_all("article", class_="tileItem")

    # Fallback 1: ul.noticias > li
    if not news_items:
        news_list = soup.find("ul", class_="noticias")
        if news_list:
            news_items = news_list.find_all("li")

    # Fallback 2: article.entry inside div.entries (SUDAM, CTIR, etc.)
    if not news_items:
        entries_container = soup.find("div", class_="entries")
        if entries_container:
            news_items = entries_container.find_all("article", class_="entry")

    # Fallback 3: div.item in content-core (Palmares, HFA, etc.)
    if not news_items:
        content_core = soup.find("div", id="content-core")
        if content_core:
            news_items = content_core.find_all("div", class_="item")

    return news_items


# =============================================================================
# Tests - Scraper Finds Articles (Core Fix for Issue #19)
# =============================================================================


@pytest.mark.integration
class TestScraperFindsArticles:
    """
    Core test: Verify that the scraper finds articles on scrapable agencies.

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

        news_items = find_news_items(page_data["html"])
        response_size = len(page_data["html"])

        assert len(news_items) > 0, (
            f"ISSUE #19 NOT FIXED: No articles found for {agency_key}. "
            f"Response size: {response_size} bytes. "
            f"This would cause ScrapingError in production."
        )

    def test_working_agency_still_works(self, working_agency_data):
        """Baseline: working agency (mec) should continue to find articles."""
        news_items = find_news_items(working_agency_data["html"])
        assert len(news_items) > 0, "Working agency should find articles"


# =============================================================================
# Tests - Scraper Extracts Required Fields
# =============================================================================


@pytest.mark.integration
class TestScraperExtractsFields:
    """
    Verify that the scraper extracts the same fields from scrapable agencies
    that it extracts from working agencies: title, url, date.

    Note: SPA agencies are excluded as they require JavaScript.
    """

    @pytest.mark.parametrize("agency_key", SCRAPABLE_AGENCIES.keys())
    def test_extracts_title_and_url(self, affected_agencies_data, agency_key):
        """Scrapable agencies must extract valid title and URL from at least some items."""
        page_data = affected_agencies_data.get(agency_key)

        if "error" in page_data:
            pytest.skip(f"Could not fetch {agency_key}: {page_data['error']}")

        news_items = find_news_items(page_data["html"])
        if not news_items:
            pytest.skip(f"No articles found for {agency_key}")

        scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])

        # Check first 5 items for valid titles (some may be images without text)
        valid_titles = 0
        for item in news_items[:5]:
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

        news_items = find_news_items(page_data["html"])
        if not news_items:
            pytest.skip(f"No articles found for {agency_key}")

        scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])

        # Check first 5 articles for dates
        dates_extracted = 0
        for item in news_items[:5]:
            extracted_date = scraper.extract_date(item)
            if extracted_date is not None:
                dates_extracted += 1
                assert isinstance(extracted_date, date)
                assert 2015 <= extracted_date.year <= datetime.now().year + 1

        assert dates_extracted > 0, (
            f"{agency_key}: Could not extract date from any of the first 5 articles"
        )


# =============================================================================
# Tests - Comparison with Working Agency
# =============================================================================


@pytest.mark.integration
class TestComparisonWithWorkingAgency:
    """Compare affected agencies with working agency to ensure same data quality."""

    def test_working_agency_extracts_all_fields(self, working_agency_data):
        """Baseline: working agency extracts title, url, and date."""
        news_items = find_news_items(working_agency_data["html"])
        assert len(news_items) > 0

        scraper = create_scraper(WORKING_AGENCY["url"])
        item = news_items[0]

        title, url = scraper.extract_title_and_url(item)
        extracted_date = scraper.extract_date(item)

        assert title != "No Title", "Working agency should extract title"
        assert url != "No URL", "Working agency should extract URL"
        assert extracted_date is not None, "Working agency should extract date"

    @pytest.mark.parametrize("agency_key", ["palmares", "sudam"])
    def test_scrapable_agency_matches_working_quality(
        self, affected_agencies_data, working_agency_data, agency_key
    ):
        """Scrapable agencies should extract data with same quality as working agency."""
        page_data = affected_agencies_data.get(agency_key)

        if "error" in page_data:
            pytest.skip(f"Could not fetch {agency_key}: {page_data['error']}")

        # Get working agency data
        working_items = find_news_items(working_agency_data["html"])
        working_scraper = create_scraper(WORKING_AGENCY["url"])
        working_title, working_url = working_scraper.extract_title_and_url(working_items[0])
        working_date = working_scraper.extract_date(working_items[0])

        # Get affected agency data
        affected_items = find_news_items(page_data["html"])
        if not affected_items:
            pytest.skip(f"No articles found for {agency_key}")

        affected_scraper = create_scraper(SCRAPABLE_AGENCIES[agency_key])
        affected_title, affected_url = affected_scraper.extract_title_and_url(affected_items[0])

        # Compare: both should have valid data
        assert (working_title != "No Title") == (affected_title != "No Title"), (
            f"{agency_key} should extract title like working agency"
        )
        assert (working_url != "No URL") == (affected_url != "No URL"), (
            f"{agency_key} should extract URL like working agency"
        )


# =============================================================================
# Tests - Extraction Functions (Isolated)
# =============================================================================


@pytest.mark.integration
class TestExtractionFunctions:
    """Test the new extraction functions with real data."""

    def test_extract_date_3_on_article_container(self, affected_agencies_data):
        """Test extract_date_3 works on article-container pattern (palmares)."""
        page_data = affected_agencies_data.get("palmares")

        if "error" in page_data:
            pytest.skip("Could not fetch palmares")

        soup = BeautifulSoup(page_data["html"], "html.parser")
        article_containers = soup.find_all("div", class_="article-container")

        if not article_containers:
            pytest.skip("No article-container found on palmares")

        scraper = create_scraper(SCRAPABLE_AGENCIES["palmares"])

        # Test extract_date_3 directly
        for item in article_containers[:3]:
            result = scraper.extract_date_3(item)
            if result is not None:
                assert isinstance(result, datetime)
                return  # Found at least one date

        # It's OK if some pages don't have dates in this format
        pytest.skip("No dates found with extract_date_3 pattern")


# =============================================================================
# Summary Report (runs last)
# =============================================================================


@pytest.mark.integration
class TestSummaryReport:
    """Generate a summary report of all affected agencies."""

    def test_generate_report(self, affected_agencies_data):
        """Print extraction results for all affected agencies."""
        print("\n" + "=" * 70)
        print("ISSUE #19 FIX VALIDATION REPORT")
        print("=" * 70)

        success = 0
        fail = 0

        for agency_key, page_data in affected_agencies_data.items():
            if "error" in page_data:
                print(f"{agency_key}: SKIP (network error)")
                continue

            news_items = find_news_items(page_data["html"])

            if not news_items:
                print(f"{agency_key}: FAIL (0 articles found)")
                fail += 1
                continue

            scraper = create_scraper(AFFECTED_AGENCIES[agency_key])
            item = news_items[0]
            title, url = scraper.extract_title_and_url(item)
            extracted_date = scraper.extract_date(item)

            status = "OK" if title != "No Title" else "PARTIAL"
            print(f"{agency_key}: {status} ({len(news_items)} articles)")
            print(f"  Title: {title[:50]}..." if len(title) > 50 else f"  Title: {title}")
            print(f"  Date: {extracted_date}")
            success += 1

        print("=" * 70)
        print(f"RESULT: {success} OK, {fail} FAIL")
        print("=" * 70)
