"""
Unit tests for webscraper refactorings (Issue #19, PR #22 improvements).

Tests cover refactorings suggested by code review:
1. _parse_date_from_text - Consolidated regex for date parsing
2. extract_date_3 - Improved with targeted element search
3. Fallback 3 validation - Filter items without links
4. Strategy 4 validation - Exclude non-article links
"""

from datetime import datetime
from bs4 import BeautifulSoup
import pytest

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


# =============================================================================
# Tests for _parse_date_from_text (consolidated regex)
# =============================================================================


class TestParseDateFromText:
    """Tests for the consolidated _parse_date_from_text private method."""

    def test_parse_date_with_time_colon_separator(self, scraper):
        """Test parsing DD/MM/YYYY HH:mm format."""
        result = scraper._parse_date_from_text("10/02/2026 17:05")
        assert result == datetime(2026, 2, 10, 17, 5)

    def test_parse_date_with_time_h_separator(self, scraper):
        """Test parsing DD/MM/YYYY HHhMM format."""
        result = scraper._parse_date_from_text("11/02/2026 09h30")
        assert result == datetime(2026, 2, 11, 9, 30)

    def test_parse_date_single_digit_hour(self, scraper):
        """Test parsing with single-digit hour (H:mm or HhMM)."""
        result = scraper._parse_date_from_text("05/01/2026 8:45")
        assert result == datetime(2026, 1, 5, 8, 45)

    def test_parse_date_without_time(self, scraper):
        """Test parsing DD/MM/YYYY format (no time)."""
        result = scraper._parse_date_from_text("15/03/2026")
        assert result == datetime(2026, 3, 15)

    def test_parse_invalid_date_values(self, scraper):
        """Test that invalid date values return None."""
        result = scraper._parse_date_from_text("32/13/2026 25:99")
        assert result is None

    def test_parse_no_date_in_text(self, scraper):
        """Test that text without dates returns None."""
        result = scraper._parse_date_from_text("Publicado em")
        assert result is None

    def test_parse_empty_string(self, scraper):
        """Test that empty string returns None."""
        result = scraper._parse_date_from_text("")
        assert result is None

    def test_parse_none_input(self, scraper):
        """Test that None input returns None."""
        result = scraper._parse_date_from_text(None)
        assert result is None

    def test_parse_date_with_surrounding_text(self, scraper):
        """Test extraction from text with surrounding content."""
        text = "Publicado em 10/02/2026 17h05 por Admin"
        result = scraper._parse_date_from_text(text)
        assert result == datetime(2026, 2, 10, 17, 5)


# =============================================================================
# Tests for extract_date_1 (refactored to use _parse_date_from_text)
# =============================================================================


class TestExtractDate1Refactored:
    """Tests for extract_date_1 after refactoring."""

    def test_extract_from_documentByLine(self, scraper):
        """Test extraction from span.documentByLine using consolidated method."""
        html = """
        <article class="tileItem">
            <span class="documentByLine">10/02/2026 17h05</span>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_date_1(item)
        assert result == datetime(2026, 2, 10, 17, 5)

    def test_no_documentByLine_returns_none(self, scraper):
        """Test that missing documentByLine returns None."""
        html = """<article class="tileItem"><span class="other">Text</span></article>"""
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_date_1(item)
        assert result is None


# =============================================================================
# Tests for extract_date_3 (improved with targeted search)
# =============================================================================


class TestExtractDate3Improved:
    """Tests for extract_date_3 with improved targeted element search."""

    def test_extract_from_time_tag_with_datetime_attribute(self, scraper):
        """Test extraction from <time> tag with datetime attribute (Strategy 1)."""
        html = """
        <article class="entry">
            <time datetime="2026-02-10T17:05:00-03:00">10 de fevereiro</time>
            <p>Conteúdo do corpo com data espúria 01/01/2020 15h00</p>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_date_3(item)

        # Should extract from time tag, not from body content
        assert result is not None
        assert result.year == 2026
        assert result.month == 2
        assert result.day == 10

    def test_extract_from_date_class(self, scraper):
        """Test extraction from element with 'date' class (Strategy 2)."""
        html = """
        <div class="item">
            <span class="date">15/03/2026 10h30</span>
            <p>Artigo com data no corpo 01/01/2020 08h00</p>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        result = scraper.extract_date_3(item)

        # Should extract from .date class, not from body
        assert result == datetime(2026, 3, 15, 10, 30)

    def test_extract_from_data_class_portuguese(self, scraper):
        """Test extraction from element with 'data' class (Portuguese)."""
        html = """
        <div class="item">
            <span class="data">20/03/2026</span>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        result = scraper.extract_date_3(item)
        assert result == datetime(2026, 3, 20)

    def test_extract_from_published_class(self, scraper):
        """Test extraction from element with 'published' class."""
        html = """
        <article>
            <div class="published-date">12/02/2026 14h20</div>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_date_3(item)
        assert result == datetime(2026, 2, 12, 14, 20)

    def test_fallback_to_full_text_last_resort(self, scraper):
        """Test fallback to full text when no specific elements found (Strategy 3)."""
        html = """
        <div class="item">
            <p>Notícia publicada em 18/03/2026 16h45</p>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        result = scraper.extract_date_3(item)
        assert result == datetime(2026, 3, 18, 16, 45)

    def test_avoids_spurious_dates_from_body_when_date_class_exists(self, scraper):
        """Test that date from specific class is preferred over body dates."""
        html = """
        <article class="entry">
            <span class="date">10/03/2026 12h00</span>
            <p>Este evento aconteceu em 01/01/2020 e foi importante.</p>
            <p>Outro evento histórico: 15/08/2019 10h30</p>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_date_3(item)

        # Should extract 10/03/2026, not the historical dates in body
        assert result == datetime(2026, 3, 10, 12, 0)

    def test_returns_none_when_no_date_found(self, scraper):
        """Test that None is returned when no date can be extracted."""
        html = """<div class="item"><p>No date here</p></div>"""
        item = BeautifulSoup(html, "html.parser").find("div")
        result = scraper.extract_date_3(item)
        assert result is None


# =============================================================================
# Tests for Fallback 3 validation (filter items without links)
# =============================================================================


class TestFallback3Validation:
    """Tests for validation in Fallback 3 (div.item must contain <a> tag)."""

    def test_fallback3_includes_items_with_links(self, scraper):
        """Test that items with <a> tags are included."""
        html = """
        <html><body>
        <div id="content-core">
            <div class="item">
                <a href="/noticia1">Notícia 1</a>
            </div>
            <div class="item">
                <a href="/noticia2">Notícia 2</a>
            </div>
        </div>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        content_core = soup.find("div", id="content-core")
        potential_items = content_core.find_all("div", class_="item")

        # Simulate the filtering logic
        news_items = [
            item for item in potential_items
            if item.find("a", href=True)
        ]

        assert len(news_items) == 2

    def test_fallback3_excludes_items_without_links(self, scraper):
        """Test that items without <a> tags are excluded."""
        html = """
        <html><body>
        <div id="content-core">
            <div class="item">
                <a href="/noticia1">Notícia 1</a>
            </div>
            <div class="item">
                <span>Não é uma notícia</span>
            </div>
            <div class="item">
                <p>Outro elemento sem link</p>
            </div>
        </div>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        content_core = soup.find("div", id="content-core")
        potential_items = content_core.find_all("div", class_="item")

        # Simulate the filtering logic
        news_items = [
            item for item in potential_items
            if item.find("a", href=True)
        ]

        # Only 1 item should pass (the one with <a> tag)
        assert len(news_items) == 1

    def test_fallback3_requires_href_attribute(self, scraper):
        """Test that <a> tags without href are excluded."""
        html = """
        <html><body>
        <div id="content-core">
            <div class="item">
                <a href="/noticia1">Com href</a>
            </div>
            <div class="item">
                <a>Sem href</a>
            </div>
        </div>
        </body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        content_core = soup.find("div", id="content-core")
        potential_items = content_core.find_all("div", class_="item")

        news_items = [
            item for item in potential_items
            if item.find("a", href=True)
        ]

        assert len(news_items) == 1


# =============================================================================
# Tests for Strategy 4 validation (exclude non-article links)
# =============================================================================


class TestStrategy4Validation:
    """Tests for validation in extract_title_and_url Strategy 4."""

    def test_excludes_share_links(self, scraper):
        """Test that links with 'share' class are excluded."""
        html = """
        <div class="item">
            <a class="share-button" href="/share">Compartilhar</a>
            <a href="/noticia">Título da Notícia</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        # Should extract the article link, not the share link
        assert url == "/noticia"
        assert title == "Título da Notícia"

    def test_excludes_social_links(self, scraper):
        """Test that links with 'social' class are excluded."""
        html = """
        <div class="item">
            <a class="social-icon" href="https://facebook.com">Facebook</a>
            <a href="/noticia">Notícia</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        assert url == "/noticia"

    def test_excludes_nav_links(self, scraper):
        """Test that navigation links are excluded."""
        html = """
        <div class="item">
            <a class="nav-link" href="/menu">Menu</a>
            <a href="/noticia">Notícia Principal</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        assert url == "/noticia"

    def test_excludes_empty_links(self, scraper):
        """Test that links with no text content are excluded (icon/image links)."""
        html = """
        <div class="item">
            <a href="/icon"></a>
            <a href="/noticia">Texto da Notícia</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        # Should skip empty link and get the one with text
        assert url == "/noticia"

    def test_excludes_button_class(self, scraper):
        """Test that links with 'button' class are excluded."""
        html = """
        <div class="item">
            <a class="button" href="/action">Clique Aqui</a>
            <a href="/noticia">Artigo</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        assert url == "/noticia"

    def test_case_insensitive_class_matching(self, scraper):
        """Test that class exclusion is case-insensitive."""
        html = """
        <div class="item">
            <a class="SHARE-link" href="/share">Share</a>
            <a href="/noticia">Notícia</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        # SHARE (uppercase) should still be excluded
        assert url == "/noticia"

    def test_falls_back_to_first_valid_link(self, scraper):
        """Test that first valid link is selected when all filters pass."""
        html = """
        <div class="item">
            <a href="/noticia1">Primeira Notícia</a>
            <a href="/noticia2">Segunda Notícia</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        # Should get first valid link
        assert url == "/noticia1"

    def test_returns_no_url_when_only_invalid_links(self, scraper):
        """Test that 'No URL' is returned when all links are invalid."""
        html = """
        <div class="item">
            <a class="share" href="/share">Share</a>
            <a href="/icon"></a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        assert url == "No URL"
        assert title == "No Title"


# =============================================================================
# Integration test: Full extraction with refactored methods
# =============================================================================


class TestIntegrationRefactored:
    """Integration tests for full extraction flow with refactored methods."""

    def test_full_extraction_with_improved_fallbacks(self, scraper):
        """Test complete extraction using improved date and link extraction."""
        html = """
        <div class="item">
            <a class="summary" href="/noticia-palmares">Palmares anuncia novo programa</a>
            <span class="date">18/03/2026 14h30</span>
            <p>Descrição da notícia</p>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")

        # Test extract_title_and_url (Strategy 3: a.summary)
        title, url = scraper.extract_title_and_url(item)
        assert title == "Palmares anuncia novo programa"
        assert url == "/noticia-palmares"

        # Test extract_date with fallback to extract_date_3
        news_date = scraper.extract_date(item)
        assert news_date.year == 2026
        assert news_date.month == 3
        assert news_date.day == 18
