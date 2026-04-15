"""
Unit tests for WebScraper.scrape_page() and anti-bot detection.

Tests cover:
1. _detect_anti_bot() - Detection of Cloudflare/JS challenges
2. scrape_page() - Page scraping with fallbacks, error handling, and filtering
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests
from bs4 import BeautifulSoup

from govbr_scraper.scrapers.webscraper import WebScraper, ScrapingError


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def scraper():
    """Create a WebScraper instance for testing."""
    return WebScraper(
        base_url="https://www.gov.br/mec/pt-br/noticias",
        min_date="2026-03-01",
        max_date="2026-03-10",
    )


@pytest.fixture
def scraper_no_max_date():
    """Create a WebScraper without max_date."""
    return WebScraper(
        base_url="https://www.gov.br/mec/pt-br/noticias",
        min_date="2026-03-01",
    )


# =============================================================================
# Tests for _detect_anti_bot()
# =============================================================================


class TestDetectAntiBot:
    """Tests for anti-bot protection detection."""

    def test_detect_anti_bot_returns_true_for_cloudflare_challenge(self, scraper):
        """Response with Cloudflare challenge indicators should be detected."""
        mock_response = MagicMock()
        mock_response.text = """
        <html>
        <head><title>Just a moment...</title></head>
        <body>
            <div class="cf-browser-verification">
                <div id="challenge-platform">Checking your browser...</div>
            </div>
        </body>
        </html>
        """

        result = scraper._detect_anti_bot(mock_response)

        assert result is True

    def test_detect_anti_bot_returns_true_for_jschl_challenge(self, scraper):
        """Response with jschl_vc (JS challenge) should be detected."""
        mock_response = MagicMock()
        mock_response.text = '<form id="challenge-form" action="/cdn-cgi/l/chk_jschl"><input name="jschl_vc" />'

        result = scraper._detect_anti_bot(mock_response)

        assert result is True

    def test_detect_anti_bot_returns_true_for_ray_id(self, scraper):
        """Response with ray_id (Cloudflare identifier) should be detected."""
        mock_response = MagicMock()
        mock_response.text = '<div class="ray_id">Ray ID: 123abc456def</div>'

        result = scraper._detect_anti_bot(mock_response)

        assert result is True

    def test_detect_anti_bot_returns_false_for_normal_page(self, scraper):
        """Normal government page should not be flagged as anti-bot."""
        mock_response = MagicMock()
        mock_response.text = """
        <html>
        <head><title>Ministério da Educação</title></head>
        <body>
            <article class="tileItem">
                <h2>Notícia do MEC</h2>
                <p>Conteúdo da notícia...</p>
            </article>
        </body>
        </html>
        """

        result = scraper._detect_anti_bot(mock_response)

        assert result is False


# =============================================================================
# Tests for scrape_page() - Anti-bot detection
# =============================================================================


class TestScrapePageAntiBot:
    """Tests for scrape_page() anti-bot handling."""

    def test_scrape_page_raises_on_anti_bot_detection(self, scraper):
        """scrape_page() should raise ScrapingError when anti-bot is detected."""
        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = '<div class="cf-browser-verification">Checking your browser</div>'
            mock_response.content = b'<html>...</html>'
            mock_fetch.return_value = mock_response

            with pytest.raises(ScrapingError, match="Anti-bot protection detected"):
                scraper.scrape_page("https://www.gov.br/mec/pt-br/noticias?b_start:int=0")


# =============================================================================
# Tests for scrape_page() - Empty page handling
# =============================================================================


class TestScrapePageEmpty:
    """Tests for scrape_page() empty page handling."""

    def test_scrape_page_raises_on_empty_first_page_large_response(self, scraper):
        """Large response with no articles on first page should raise ScrapingError."""
        with patch.object(scraper, 'fetch_page') as mock_fetch:
            # Large HTML response (> 5000 bytes) but no articles
            large_html = '<html><body>' + '<p>Padding content</p>' * 500 + '</body></html>'
            mock_response = MagicMock()
            mock_response.text = large_html
            mock_response.content = large_html.encode('utf-8')
            mock_fetch.return_value = mock_response

            with pytest.raises(ScrapingError, match="No articles found on first page"):
                scraper.scrape_page("https://www.gov.br/mec/pt-br/noticias?b_start:int=0")

    def test_scrape_page_returns_false_on_empty_page(self, scraper):
        """Empty page (small response, no articles) should return (False, 0)."""
        with patch.object(scraper, 'fetch_page') as mock_fetch:
            # Small HTML response with no articles
            small_html = '<html><body><p>Nenhuma notícia encontrada</p></body></html>'
            mock_response = MagicMock()
            mock_response.text = small_html
            mock_response.content = small_html.encode('utf-8')
            mock_fetch.return_value = mock_response

            should_continue, items_per_page = scraper.scrape_page(
                "https://www.gov.br/mec/pt-br/noticias?b_start:int=10"
            )

            assert should_continue is False
            assert items_per_page == 0

    def test_scrape_page_does_not_raise_on_empty_subsequent_page(self, scraper):
        """Empty subsequent page (not first) should not raise, just return (False, 0)."""
        with patch.object(scraper, 'fetch_page') as mock_fetch:
            # Large response but no articles on page 2
            large_html = '<html><body>' + '<div>Content</div>' * 300 + '</body></html>'
            mock_response = MagicMock()
            mock_response.text = large_html
            mock_response.content = large_html.encode('utf-8')
            mock_fetch.return_value = mock_response

            should_continue, items_per_page = scraper.scrape_page(
                "https://www.gov.br/mec/pt-br/noticias?b_start:int=20"
            )

            assert should_continue is False
            assert items_per_page == 0


# =============================================================================
# Tests for scrape_page() - Max date filtering
# =============================================================================


class TestScrapePageMaxDate:
    """Tests for scrape_page() max_date filtering."""

    def test_scrape_page_skips_page_when_all_items_newer_than_max_date(self, scraper):
        """Page where all items are newer than max_date should be skipped."""
        html_content = """
        <html><body>
            <article class="tileItem">
                <a href="/noticia1">Notícia 1</a>
                <span class="documentByLine">15/03/2026</span>
            </article>
            <article class="tileItem">
                <a href="/noticia2">Notícia 2</a>
                <span class="documentByLine">12/03/2026</span>
            </article>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            # Last item is 12/03/2026, which is > max_date (10/03/2026)
            with patch.object(scraper, 'extract_date') as mock_extract_date:
                mock_extract_date.return_value = date(2026, 3, 12)

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias?b_start:int=0"
                )

                assert should_continue is True  # Continue to next page
                assert items_per_page == 2  # Found 2 items (but skipped them)

    def test_scrape_page_processes_page_when_items_within_date_range(self, scraper):
        """Page with items within date range should be processed."""
        html_content = """
        <html><body>
            <article class="tileItem">
                <a href="/noticia1">Notícia 1</a>
                <span class="documentByLine">05/03/2026</span>
            </article>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_date') as mock_extract_date:
                mock_extract_date.return_value = date(2026, 3, 5)

                with patch.object(scraper, 'extract_news_info') as mock_extract_info:
                    mock_extract_info.return_value = True  # Continue processing

                    should_continue, items_per_page = scraper.scrape_page(
                        "https://www.gov.br/mec/pt-br/noticias?b_start:int=0"
                    )

                    assert should_continue is True
                    assert items_per_page == 1
                    mock_extract_info.assert_called_once()


# =============================================================================
# Tests for scrape_page() - HTML fallbacks
# =============================================================================


class TestScrapePageFallbacks:
    """Tests for scrape_page() HTML structure fallbacks."""

    def test_scrape_page_finds_articles_with_tileitem_class(self, scraper):
        """Standard Plone structure with article.tileItem should be found."""
        html_content = """
        <html><body>
            <article class="tileItem">
                <a href="/noticia1">Notícia 1</a>
            </article>
            <article class="tileItem">
                <a href="/noticia2">Notícia 2</a>
            </article>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = False  # Stop after first item

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                assert items_per_page == 2

    def test_scrape_page_fallback_to_ul_noticias(self, scraper):
        """Fallback 1: ul.noticias > li should be found."""
        html_content = """
        <html><body>
            <ul class="noticias">
                <li><a href="/noticia1">Notícia 1</a></li>
                <li><a href="/noticia2">Notícia 2</a></li>
            </ul>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = False

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                assert items_per_page == 2

    def test_scrape_page_fallback_to_entries_container(self, scraper):
        """Fallback 2: div.entries > article.entry should be found."""
        html_content = """
        <html><body>
            <div class="entries">
                <article class="entry">
                    <a href="/noticia1">Notícia 1</a>
                </article>
                <article class="entry">
                    <a href="/noticia2">Notícia 2</a>
                </article>
            </div>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = False

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                assert items_per_page == 2

    def test_scrape_page_fallback_to_div_item_with_validation(self, scraper):
        """Fallback 3: div#content-core > div.item (only those with <a> tags)."""
        html_content = """
        <html><body>
            <div id="content-core">
                <div class="item">
                    <a href="/noticia1">Notícia 1</a>
                </div>
                <div class="item">
                    <p>Não é uma notícia</p>
                </div>
                <div class="item">
                    <a href="/noticia2">Notícia 2</a>
                </div>
            </div>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = False

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                # Only 2 items should be found (those with <a> tags)
                assert items_per_page == 2

    def test_scrape_page_chains_through_fallbacks_when_tileitem_absent(self, scraper):
        """When tileItem not found, should automatically try ul.noticias."""
        html_content = """
        <html><body>
            <!-- No tileItem, but has ul.noticias -->
            <ul class="noticias">
                <li><a href="/noticia1">Notícia via fallback</a></li>
                <li><a href="/noticia2">Notícia via fallback</a></li>
            </ul>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = False

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                # Should find items via fallback strategy
                assert items_per_page == 2

    def test_scrape_page_max_date_filtering_applies_across_all_strategies(self, scraper):
        """max_date filtering should work regardless of which fallback strategy succeeds."""
        # Date is 2026-03-15, which is AFTER max_date (2026-03-10)
        html_content = """
        <html><body>
            <ul class="noticias">
                <li>
                    <a href="/noticia1">Notícia recente</a>
                    <span class="data">15/03/2026</span>
                </li>
            </ul>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_content
            mock_response.content = html_content.encode('utf-8')
            mock_fetch.return_value = mock_response

            # extract_news_info will be called but should return True (stop scraping)
            # because date > max_date
            with patch.object(scraper, 'extract_news_info') as mock_extract:
                mock_extract.return_value = True  # Date too recent, continue to next page

                should_continue, items_per_page = scraper.scrape_page(
                    "https://www.gov.br/mec/pt-br/noticias"
                )

                # Should continue because extract_news_info returned True
                assert should_continue is True
                assert items_per_page == 1

    def test_scrape_page_with_mixed_anti_bot_and_valid_content(self, scraper):
        """Anti-bot detection should take precedence over content extraction."""
        html_with_both = """
        <html>
        <head><title>Just a moment...</title></head>
        <body>
            <div class="cf-browser-verification">Checking your browser...</div>
            <!-- Even though there's valid content below, anti-bot should win -->
            <article class="tileItem">
                <a href="/noticia1">Notícia</a>
            </article>
        </body>
        </html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_with_both
            mock_response.content = html_with_both.encode('utf-8')
            mock_fetch.return_value = mock_response

            with pytest.raises(ScrapingError, match="Anti-bot protection detected"):
                scraper.scrape_page("https://www.gov.br/mec/pt-br/noticias")

    def test_scrape_page_empty_page_returns_zero_items(self, scraper):
        """Page with no articles in any fallback strategy should return 0 items."""
        html_empty = """
        <html><body>
            <h1>Notícias</h1>
            <p>Não há notícias disponíveis no momento.</p>
        </body></html>
        """

        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.text = html_empty
            mock_response.content = html_empty.encode('utf-8')
            mock_fetch.return_value = mock_response

            should_continue, items_per_page = scraper.scrape_page(
                "https://www.gov.br/mec/pt-br/noticias"
            )

            # No items found via any strategy
            assert items_per_page == 0
            assert should_continue is False  # Stop when no items found


# =============================================================================
# Tests for scrape_page() - Request failure handling
# =============================================================================


class TestScrapePageRequestFailure:
    """Tests for scrape_page() request failure handling."""

    def test_scrape_page_raises_on_request_exception(self, scraper):
        """Request exception should be wrapped in ScrapingError."""
        with patch.object(scraper, 'fetch_page') as mock_fetch:
            mock_fetch.side_effect = requests.exceptions.ConnectionError("Connection refused")

            with pytest.raises(ScrapingError, match="Failed to fetch page after retries"):
                scraper.scrape_page("https://www.gov.br/mec/pt-br/noticias")
