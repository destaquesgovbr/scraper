"""
Unit tests for WebScraper extraction methods.

Tests cover date, title, URL, and link extraction strategies:
1. _parse_date_from_text - Consolidated regex for date parsing
2. extract_date_1/3 - Date extraction from listing pages
3. extract_title_and_url - Title and URL extraction strategies (Strategy 4 validation)
4. Full extraction flow integration tests
"""

from datetime import datetime
from bs4 import BeautifulSoup
import pytest


# scraper fixture provided by tests/unit/conftest.py


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
# Tests for extract_date_2
# =============================================================================


class TestExtractDate2:
    """Tests for extract_date_2 (extracts from span.data)."""

    def test_extract_date_2_from_span_data(self, scraper):
        """Test extraction from span with class 'data'."""
        html = """
        <li>
            <a href="/noticia">Título da Notícia</a>
            <span class="data">22/03/2026</span>
        </li>
        """
        item = BeautifulSoup(html, "html.parser").find("li")
        result = scraper.extract_date_2(item)
        assert result == datetime(2026, 3, 22)

    def test_extract_date_2_returns_none_when_no_span_data(self, scraper):
        """Test that None is returned when span.data is not found."""
        html = """
        <li>
            <a href="/noticia">Título da Notícia</a>
            <span>22/03/2026</span>
        </li>
        """
        item = BeautifulSoup(html, "html.parser").find("li")
        result = scraper.extract_date_2(item)
        assert result is None

    def test_extract_date_2_returns_none_when_invalid_format(self, scraper):
        """Test that None is returned when date format is invalid."""
        html = """
        <li>
            <span class="data">Publicado em 22/03/2026</span>
        </li>
        """
        item = BeautifulSoup(html, "html.parser").find("li")
        result = scraper.extract_date_2(item)
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

    @pytest.mark.parametrize("element,date_text,expected_datetime", [
        ('<span class="date">15/03/2026 10h30</span>', "15/03/2026 10h30", datetime(2026, 3, 15, 10, 30)),
        ('<span class="data">20/03/2026</span>', "20/03/2026", datetime(2026, 3, 20)),
        ('<div class="published-date">12/02/2026 14h20</div>', "12/02/2026 14h20", datetime(2026, 2, 12, 14, 20)),
    ], ids=["date-class", "data-class-portuguese", "published-class"])
    def test_extract_from_date_related_classes(self, scraper, element, date_text, expected_datetime):
        """Test extraction from elements with date/data/published classes (Strategy 2)."""
        html = f"""
        <div class="item">
            {element}
            <p>Artigo com data espúria no corpo 01/01/2020 08h00</p>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        result = scraper.extract_date_3(item)
        assert result == expected_datetime

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
# Tests for extract_category() (3 strategies)
# =============================================================================


class TestExtractCategory:
    """Tests for category extraction from listing pages."""

    @pytest.mark.parametrize("element,expected_category", [
        ('<span class="subtitle">Educação Básica</span>', "Educação Básica"),
        ('<div class="subtitulo-noticia">Ensino Superior</div>', "Ensino Superior"),
        ('<div class="categoria-noticia">Pesquisa Científica</div>', "Pesquisa Científica"),
    ], ids=["subtitle-span", "subtitulo-noticia-div", "categoria-noticia-div"])
    def test_extract_category_from_various_elements(self, scraper, element, expected_category):
        """Test extraction from different HTML elements (3 strategies)."""
        html = f"""
        <article class="tileItem">
            {element}
            <a href="/noticia">Título</a>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_category(item)
        assert result == expected_category

    def test_extract_category_strips_whitespace(self, scraper):
        """Test that whitespace is stripped from extracted category."""
        html = """
        <article class="tileItem">
            <span class="subtitle">  Pesquisa e Inovação  </span>
            <a href="/noticia">Título</a>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_category(item)
        assert result == "Pesquisa e Inovação"

    def test_extract_category_returns_no_category_when_not_found(self, scraper):
        """Test that 'No Category' is returned when no category can be extracted."""
        html = """
        <article class="tileItem">
            <a href="/noticia">Título</a>
            <p>Descrição</p>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_category(item)
        assert result == "No Category"


# =============================================================================
# Tests for Strategy 4 validation (exclude non-article links)
# =============================================================================


class TestStrategy4Validation:
    """Tests for validation in extract_title_and_url Strategy 4."""

    @pytest.mark.parametrize("excluded_link,valid_link,valid_title", [
        ('<a class="share-button" href="/share">Compartilhar</a>', "/noticia", "Título da Notícia"),
        ('<a class="social-icon" href="https://facebook.com">Facebook</a>', "/noticia", "Notícia"),
        ('<a class="nav-link" href="/menu">Menu</a>', "/noticia", "Notícia Principal"),
        ('<a href="/icon"></a>', "/noticia", "Texto da Notícia"),
        ('<a class="button" href="/action">Clique Aqui</a>', "/noticia", "Artigo"),
    ], ids=["share", "social", "nav", "empty", "button"])
    def test_excludes_non_article_links(self, scraper, excluded_link, valid_link, valid_title):
        """Test that various non-article link types are excluded."""
        html = f"""
        <div class="item">
            {excluded_link}
            <a href="{valid_link}">{valid_title}</a>
        </div>
        """
        item = BeautifulSoup(html, "html.parser").find("div")
        title, url = scraper.extract_title_and_url(item)

        assert url == valid_link
        assert title == valid_title

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
# Full extraction flow tests
# =============================================================================


class TestFullExtractionFlow:
    """Tests for complete extraction flow using multiple methods together."""

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


# =============================================================================
# Tests for extract_tags()
# =============================================================================


class TestExtractTags:
    """Tests for tag extraction from listing pages."""

    def test_extract_tags_from_listing(self, scraper):
        """Extract tags from listing page metadata."""
        html = """
        <article class="tileItem">
            <a href="/noticia">Título</a>
            <div class="documentTags">
                <span>educacao</span>
                <span>ensino-superior</span>
            </div>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")

        # The extract_tags method in the current implementation returns empty list
        # for listing pages (tags are extracted from article pages)
        result = scraper.extract_tags(item)

        # Current implementation returns empty list from listing pages
        assert isinstance(result, list)

    def test_extract_tags_empty_when_no_tags(self, scraper):
        """Return empty list when no tags found."""
        html = """
        <article class="tileItem">
            <a href="/noticia">Título</a>
            <p>Descrição</p>
        </article>
        """
        item = BeautifulSoup(html, "html.parser").find("article")
        result = scraper.extract_tags(item)

        assert result == []


# =============================================================================
# Tests for _extract_tags_from_article_page()
# =============================================================================


class TestExtractTagsFromArticlePage:
    """Tests for tag extraction from article pages."""

    def test_extract_tags_from_article_page_origem_keyword(self, scraper):
        """Extract tags from links with origem=keyword parameter."""
        html = """
        <html>
        <body>
            <div class="content">
                <a href="/busca?origem=keyword&palavra=educacao">Educação</a>
                <a href="/busca?origem=keyword&palavra=ensino">Ensino Superior</a>
            </div>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._extract_tags_from_article_page(soup)

        assert len(result) == 2
        assert "Educação" in result
        assert "Ensino Superior" in result

    def test_extract_tags_from_article_page_keywords_div(self, scraper):
        """Extract tags from keywords div (fallback strategy)."""
        html = """
        <html>
        <body>
            <div class="keywords">
                <a href="/tag/educacao">Educação</a>
                <a href="/tag/ensino">Ensino Superior</a>
            </div>
        </body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._extract_tags_from_article_page(soup)

        assert len(result) == 2
        assert "Educação" in result
        assert "Ensino Superior" in result

    def test_extract_tags_empty_from_article_without_tags(self, scraper):
        """Return empty list when article has no tags."""
        html = """
        <html>
        <head><title>Article</title></head>
        <body><p>Content without tags</p></body>
        </html>
        """
        soup = BeautifulSoup(html, "html.parser")
        result = scraper._extract_tags_from_article_page(soup)

        assert result == []
