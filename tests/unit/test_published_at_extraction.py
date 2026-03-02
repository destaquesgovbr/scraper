"""
Unit tests for published_at extraction and fallback logic.

Tests cover:
1. Text parsing with gov.br HTML structure (separate spans for label and value)
2. Text parsing with inline text (fallback for non-standard pages)
3. Fallback to listing page date when article datetime extraction fails
4. _parse_datetime_from_text helper for both date formats
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from bs4 import BeautifulSoup
from govbr_scraper.scrapers.webscraper import WebScraper

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def scraper():
    """Create a WebScraper instance for testing."""
    return WebScraper(
        base_url="https://www.gov.br/test/pt-br/noticias",
        min_date="2026-01-01",
    )


@pytest.fixture
def govbr_html_separate_spans():
    """Gov.br HTML with date in separate <span class="value"> inside <span class="documentPublished">."""
    return """
    <html>
    <head><title>Test Article</title></head>
    <body>
    <div id="content">
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">10/02/2026 17h05</span>
        </span>
        <span class="documentModified">
            <span>Atualizado em</span>
            <span class="value">11/02/2026 09h30</span>
        </span>
        <p>Article content here.</p>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def govbr_html_inline_text():
    """HTML with date in inline text "Publicado em DD/MM/YYYY HHhMM"."""
    return """
    <html>
    <head><title>Test Article</title></head>
    <body>
    <div id="content">
        <span>Publicado em 05/02/2026 13h47</span>
        <span>Atualizado em 06/02/2026 19h01</span>
        <p>Article content here.</p>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def govbr_html_no_date():
    """HTML without any date information (e.g., login page after redirect)."""
    return """
    <html>
    <head><title>Login Required</title></head>
    <body>
    <div id="content">
        <form action="/login">
            <input type="text" name="username" />
        </form>
    </div>
    </body>
    </html>
    """


@pytest.fixture
def govbr_html_malformed_jsonld():
    """HTML with malformed JSON-LD (unescaped quotes in headline) but valid text dates."""
    return """
    <html>
    <head>
        <title>Test Article</title>
        <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "NewsArticle",
                "headline": ""Em 2026, serão 40 leilões em 4 anos", afirmou ministro",
                "datePublished": "2026-02-10T17:05:07-03:00"
            }
        </script>
    </head>
    <body>
    <div id="content">
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">10/02/2026 17h05</span>
        </span>
        <p>Article content here.</p>
    </div>
    </body>
    </html>
    """


# =============================================================================
# Tests for _parse_datetime_from_text
# =============================================================================


class TestParseDatetimeFromText:
    """Tests for the static _parse_datetime_from_text helper."""

    def test_pattern_govbr_format(self):
        """Test parsing DD/MM/YYYY HHhMM format."""
        brasilia_tz = timezone(timedelta(hours=-3))
        result = WebScraper._parse_datetime_from_text("10/02/2026 17h05", brasilia_tz)
        assert result == datetime(2026, 2, 10, 17, 5, tzinfo=brasilia_tz)

    def test_pattern_ebc_format(self):
        """Test parsing DD/MM/YYYY - HH:MM format."""
        brasilia_tz = timezone(timedelta(hours=-3))
        result = WebScraper._parse_datetime_from_text("17/11/2025 - 18:58", brasilia_tz)
        assert result == datetime(2025, 11, 17, 18, 58, tzinfo=brasilia_tz)

    def test_no_match(self):
        """Test that non-date text returns None."""
        brasilia_tz = timezone(timedelta(hours=-3))
        result = WebScraper._parse_datetime_from_text("Publicado em", brasilia_tz)
        assert result is None

    def test_empty_string(self):
        """Test that empty string returns None."""
        brasilia_tz = timezone(timedelta(hours=-3))
        result = WebScraper._parse_datetime_from_text("", brasilia_tz)
        assert result is None


# =============================================================================
# Tests for _extract_datetime_from_text
# =============================================================================


class TestExtractDatetimeFromText:
    """Tests for _extract_datetime_from_text with different HTML structures."""

    def test_govbr_separate_spans(self, scraper, govbr_html_separate_spans):
        """Test extraction from gov.br HTML with separate spans for label and value."""
        soup = BeautifulSoup(govbr_html_separate_spans, "html.parser")
        brasilia_tz = timezone(timedelta(hours=-3))

        published_dt, updated_dt = scraper._extract_datetime_from_text(soup)

        assert published_dt == datetime(2026, 2, 10, 17, 5, tzinfo=brasilia_tz)
        assert updated_dt == datetime(2026, 2, 11, 9, 30, tzinfo=brasilia_tz)

    def test_inline_text(self, scraper, govbr_html_inline_text):
        """Test extraction from inline text containing date."""
        soup = BeautifulSoup(govbr_html_inline_text, "html.parser")
        brasilia_tz = timezone(timedelta(hours=-3))

        published_dt, updated_dt = scraper._extract_datetime_from_text(soup)

        assert published_dt == datetime(2026, 2, 5, 13, 47, tzinfo=brasilia_tz)
        assert updated_dt == datetime(2026, 2, 6, 19, 1, tzinfo=brasilia_tz)

    def test_no_date_returns_none(self, scraper, govbr_html_no_date):
        """Test that pages without dates return (None, None)."""
        soup = BeautifulSoup(govbr_html_no_date, "html.parser")

        published_dt, updated_dt = scraper._extract_datetime_from_text(soup)

        assert published_dt is None
        assert updated_dt is None

    def test_malformed_jsonld_falls_back_to_text(self, scraper, govbr_html_malformed_jsonld):
        """Test that when JSON-LD is malformed, text parsing extracts the date from spans."""
        soup = BeautifulSoup(govbr_html_malformed_jsonld, "html.parser")
        brasilia_tz = timezone(timedelta(hours=-3))

        # JSON-LD should fail
        jsonld_result = scraper._extract_datetime_from_jsonld(soup)
        assert jsonld_result is None

        # Text parsing should succeed via Strategy A (separate spans)
        published_dt, updated_dt = scraper._extract_datetime_from_text(soup)
        assert published_dt == datetime(2026, 2, 10, 17, 5, tzinfo=brasilia_tz)


# =============================================================================
# Tests for listing date fallback in extract_news_info
# =============================================================================


class TestListingDateFallback:
    """Tests for fallback to listing page date when article datetime extraction fails."""

    def test_fallback_used_when_article_datetime_missing(self, scraper):
        """Test that news_date from listing is used as fallback when article has no datetime."""
        # Create a mock listing item
        listing_html = """
        <article class="tileItem">
            <a class="summary url" href="https://www.gov.br/test/pt-br/noticias/2026/02/test-article">Test Title</a>
            <span class="subtitle">Category</span>
            <span class="documentByLine">
                <span class="date">11/02/2026</span>
            </span>
        </article>
        """
        item = BeautifulSoup(listing_html, "html.parser").find("article")

        # Mock get_article_content to return None datetimes (simulating fetch failure)
        with patch.object(scraper, "get_article_content") as mock_content:
            mock_content.return_value = (
                "Article content",  # content
                None,  # image_url
                None,  # published_dt - simulating failure
                None,  # updated_dt
                [],  # tags
                None,  # editorial_lead
                None,  # subtitle
                None,  # category
            )

            # Mock extract_date to return a known date
            with patch.object(scraper, "extract_date") as mock_date:
                mock_date.return_value = date(2026, 2, 11)

                scraper.extract_news_info(item)

                # Check that the record was added with fallback datetime
                assert len(scraper.news_data) == 1
                record = scraper.news_data[0]
                brasilia_tz = timezone(timedelta(hours=-3))
                expected_dt = datetime(2026, 2, 11, 0, 0, tzinfo=brasilia_tz)
                assert record["published_at"] == expected_dt

    def test_no_fallback_when_article_datetime_present(self, scraper):
        """Test that listing date is NOT used when article has a datetime."""
        listing_html = """
        <article class="tileItem">
            <a class="summary url" href="https://www.gov.br/test/pt-br/noticias/2026/02/test-article">Test Title</a>
            <span class="subtitle">Category</span>
            <span class="documentByLine">
                <span class="date">11/02/2026</span>
            </span>
        </article>
        """
        item = BeautifulSoup(listing_html, "html.parser").find("article")

        brasilia_tz = timezone(timedelta(hours=-3))
        article_dt = datetime(2026, 2, 11, 17, 5, tzinfo=brasilia_tz)

        with patch.object(scraper, "get_article_content") as mock_content:
            mock_content.return_value = (
                "Article content",
                None,
                article_dt,  # published_dt from article
                None,
                [],
                None,
                None,
                None,
            )

            with patch.object(scraper, "extract_date") as mock_date:
                mock_date.return_value = date(2026, 2, 11)

                scraper.extract_news_info(item)

                assert len(scraper.news_data) == 1
                record = scraper.news_data[0]
                # Should use article datetime, not fallback
                assert record["published_at"] == article_dt
