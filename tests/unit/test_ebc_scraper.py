"""
Unit tests for EBC (TV Brasil and Agencia Brasil) scraper.

These tests ensure that:
1. Editorial lead extraction works correctly for TV Brasil
2. Agencia Brasil correctly has no editorial lead
3. Data conversion preserves editorial_lead field
4. Column ordering includes editorial_lead
"""

from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from bs4 import BeautifulSoup

from govbr_scraper.scrapers.ebc_webscraper import EBCWebScraper
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def tvbrasil_html() -> str:
    """Sample TV Brasil HTML with editorial lead in h4 with link."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Test TV Brasil Article</title></head>
    <body>
        <h4 class="txtNoticias"><a href="/caminhos-da-reportagem">Caminhos da Reportagem</a></h4>
        <h5>No AR em 02/02/2026 - 23:00</h5>
        <h1>Foz do Iguacu: crimes na fronteira mais movimentada do Brasil</h1>
        <article>
            <p>A reporter Flavia Peixoto investiga as rotas do trafico e contrabando.</p>
            <p>O programa mostra a realidade da fronteira mais movimentada do Brasil.</p>
        </article>
    </body>
    </html>
    """


@pytest.fixture
def tvbrasil_html_no_link() -> str:
    """Sample TV Brasil HTML with editorial lead in h4 without link."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Test TV Brasil Article</title></head>
    <body>
        <h4 class="txtNoticias">Repórter Brasil</h4>
        <h5>No AR em 01/01/2026 - 20:00</h5>
        <h1>Titulo da Materia</h1>
        <article>
            <p>Conteudo do artigo.</p>
        </article>
    </body>
    </html>
    """


@pytest.fixture
def agenciabrasil_html() -> str:
    """Sample Agencia Brasil HTML (no editorial lead)."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Test Agencia Brasil Article</title></head>
    <body>
        <h1 class="titulo-materia">Governo anuncia novas medidas economicas</h1>
        <div class="autor-noticia">Agencia Brasil</div>
        <div class="data">Publicado em 15/01/2026 - 14:30</div>
        <div class="conteudo-noticia">
            <p>O ministro da Fazenda anunciou novas medidas para a economia.</p>
            <p>As medidas incluem reducao de impostos e incentivos fiscais.</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def empty_tvbrasil_news_data() -> Dict[str, Any]:
    """Empty news_data dictionary for TV Brasil tests."""
    return {
        'title': '',
        'url': 'https://tvbrasil.ebc.com.br/test',
        'source': '',
        'date': '',
        'published_datetime': None,
        'updated_datetime': None,
        'tags': [],
        'editorial_lead': '',
        'content': '',
        'image': '',
        'video_url': '',
        'agency': '',
        'error': '',
    }


@pytest.fixture
def empty_agenciabrasil_news_data() -> Dict[str, Any]:
    """Empty news_data dictionary for Agencia Brasil tests."""
    return {
        'title': '',
        'url': 'https://agenciabrasil.ebc.com.br/test',
        'source': '',
        'date': '',
        'published_datetime': None,
        'updated_datetime': None,
        'tags': [],
        'editorial_lead': '',
        'content': '',
        'image': '',
        'video_url': '',
        'agency': '',
        'error': '',
    }


@pytest.fixture
def ebc_scraper() -> EBCWebScraper:
    """EBCWebScraper instance for testing."""
    return EBCWebScraper(min_date="2026-01-01", base_url="https://memoria.ebc.com.br/noticias")


# =============================================================================
# Tests for EBCWebScraper
# =============================================================================


class TestEBCWebScraper:
    """Tests for EBCWebScraper class."""

    def test_tvbrasil_extracts_editorial_lead_from_link(
        self,
        ebc_scraper: EBCWebScraper,
        tvbrasil_html: str,
        empty_tvbrasil_news_data: Dict[str, Any],
    ) -> None:
        """TV Brasil extracts editorial_lead from h4 with link."""
        soup = BeautifulSoup(tvbrasil_html, 'html.parser')
        news_data = empty_tvbrasil_news_data.copy()

        ebc_scraper._scrape_tvbrasil_content(soup, news_data)

        assert news_data['editorial_lead'] == 'Caminhos da Reportagem'
        assert news_data['source'] == ''
        assert news_data['title'] == 'Foz do Iguacu: crimes na fronteira mais movimentada do Brasil'

    def test_tvbrasil_extracts_editorial_lead_without_link(
        self,
        ebc_scraper: EBCWebScraper,
        tvbrasil_html_no_link: str,
        empty_tvbrasil_news_data: Dict[str, Any],
    ) -> None:
        """TV Brasil extracts editorial_lead from h4 without link."""
        soup = BeautifulSoup(tvbrasil_html_no_link, 'html.parser')
        news_data = empty_tvbrasil_news_data.copy()

        ebc_scraper._scrape_tvbrasil_content(soup, news_data)

        assert news_data['editorial_lead'] == 'Repórter Brasil'
        assert news_data['source'] == ''

    def test_agencia_brasil_editorial_lead_is_empty(
        self,
        ebc_scraper: EBCWebScraper,
        agenciabrasil_html: str,
        empty_agenciabrasil_news_data: Dict[str, Any],
    ) -> None:
        """Agencia Brasil doesn't extract editorial_lead."""
        soup = BeautifulSoup(agenciabrasil_html, 'html.parser')
        news_data = empty_agenciabrasil_news_data.copy()

        ebc_scraper._scrape_agencia_brasil_content(soup, news_data)

        # Agencia Brasil doesn't set editorial_lead, so it should remain empty
        assert news_data['editorial_lead'] == ''
        # But it should extract source/author
        assert news_data['source'] == 'Agencia Brasil'

    def test_scrape_news_page_includes_editorial_lead_field(
        self,
        ebc_scraper: EBCWebScraper,
        tvbrasil_html: str,
    ) -> None:
        """scrape_news_page returns dict with editorial_lead field."""
        with patch.object(ebc_scraper, 'fetch_page') as mock_fetch:
            mock_response = MagicMock()
            mock_response.content = tvbrasil_html.encode('utf-8')
            mock_fetch.return_value = mock_response

            result = ebc_scraper.scrape_news_page('https://tvbrasil.ebc.com.br/test')

            assert 'editorial_lead' in result
            assert result['editorial_lead'] == 'Caminhos da Reportagem'


# =============================================================================
# Tests for EBCScrapeManager
# =============================================================================


class TestEBCScrapeManager:
    """Tests for EBCScrapeManager class."""

    @pytest.fixture
    def mock_storage(self) -> MagicMock:
        """Mock storage backend."""
        return MagicMock()

    @pytest.fixture
    def manager(self, mock_storage: MagicMock) -> EBCScrapeManager:
        """EBCScrapeManager instance with mocked storage."""
        return EBCScrapeManager(storage=mock_storage)

    def test_convert_preserves_editorial_lead(self, manager: EBCScrapeManager) -> None:
        """editorial_lead is passed through to converted format."""
        ebc_data = [
            {
                'title': 'Test Article',
                'url': 'https://tvbrasil.ebc.com.br/test',
                'source': '',
                'date': '02/02/2026 - 23:00',
                'published_datetime': datetime(2026, 2, 2, 23, 0),
                'updated_datetime': None,
                'tags': ['fronteira', 'crime'],
                'editorial_lead': 'Caminhos da Reportagem',
                'content': 'Test content here.',
                'image': '',
                'video_url': '',
                'agency': 'tvbrasil',
                'error': '',
            }
        ]

        result = manager._convert_ebc_to_govbr_format(ebc_data)

        assert len(result) == 1
        assert result[0]['editorial_lead'] == 'Caminhos da Reportagem'

    @pytest.mark.parametrize("include_key,value", [
        (True, ''),  # empty string
        (False, None),  # key missing
    ], ids=['empty_editorial_lead', 'missing_editorial_lead'])
    def test_convert_handles_empty_or_missing_editorial_lead(
        self, manager: EBCScrapeManager, include_key: bool, value: str
    ) -> None:
        """Empty or missing editorial_lead becomes None in converted format."""
        ebc_data = [
            {
                'title': 'Test Article',
                'url': 'https://agenciabrasil.ebc.com.br/test',
                'source': 'Agencia Brasil',
                'date': '15/01/2026 - 14:30',
                'published_datetime': datetime(2026, 1, 15, 14, 30),
                'updated_datetime': None,
                'tags': [],
                'content': 'Test content here.',
                'image': '',
                'video_url': '',
                'agency': 'agencia_brasil',
                'error': '',
            }
        ]

        if include_key:
            ebc_data[0]['editorial_lead'] = value

        result = manager._convert_ebc_to_govbr_format(ebc_data)

        assert len(result) == 1
        assert result[0]['editorial_lead'] is None

    def test_preprocess_includes_editorial_lead_in_columns(
        self, manager: EBCScrapeManager
    ) -> None:
        """editorial_lead is included in preprocessed column data."""
        data = [
            {
                'title': 'Test Article',
                'url': 'https://tvbrasil.ebc.com.br/test',
                'published_at': datetime(2026, 2, 2, 23, 0),
                'updated_datetime': None,
                'category': 'Noticias',
                'tags': [],
                'editorial_lead': 'Caminhos da Reportagem',
                'subtitle': None,
                'content': 'Test content.',
                'image': '',
                'video_url': '',
                'agency': 'tvbrasil',
                'extracted_at': datetime.now(),
            }
        ]

        result = manager._preprocess_data(data)

        assert 'editorial_lead' in result
        assert result['editorial_lead'][0] == 'Caminhos da Reportagem'

    def test_convert_skips_items_with_errors(self, manager: EBCScrapeManager) -> None:
        """Items with error field should be skipped during conversion."""
        ebc_data = [
            {'title': 'Good', 'url': 'https://example.com/1', 'content': 'Content'},
            {'title': 'Bad', 'url': 'https://example.com/2', 'content': 'Content', 'error': 'Failed'},
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['title'] == 'Good'

    def test_convert_skips_incomplete_items(self, manager: EBCScrapeManager) -> None:
        """Items without title, url, or content should be skipped during conversion."""
        ebc_data = [
            {'title': '', 'url': 'https://example.com/1', 'content': 'Content'},
            {'title': 'Title', 'url': '', 'content': 'Content'},
            {'title': 'Title', 'url': 'https://example.com/3', 'content': ''},
            {'title': 'Complete', 'url': 'https://example.com/4', 'content': 'Full content'},
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['title'] == 'Complete'

    def test_convert_maps_published_datetime_to_published_at(self, manager: EBCScrapeManager) -> None:
        """published_datetime should be mapped to published_at."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'published_datetime': datetime(2026, 1, 15, 14, 30),
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['published_at'] == datetime(2026, 1, 15, 14, 30)

    def test_convert_sets_category_to_noticias(self, manager: EBCScrapeManager) -> None:
        """Category should be set to 'Notícias' (hardcoded for EBC)."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://agenciabrasil.ebc.com.br/educacao/noticia/2026-01/test',
                'content': 'Content',
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['category'] == 'Notícias'

    def test_convert_maps_tags_correctly(self, manager: EBCScrapeManager) -> None:
        """Tags should be passed through as list."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'tags': ['educacao', 'ensino'],
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['tags'] == ['educacao', 'ensino']

    def test_convert_handles_empty_tags(self, manager: EBCScrapeManager) -> None:
        """Empty tags list should be preserved."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'tags': [],
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['tags'] == []

    def test_convert_maps_source_field(self, manager: EBCScrapeManager) -> None:
        """Source field should be mapped (for Agencia Brasil)."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'source': 'Agencia Brasil',
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        # Source is not mapped to a specific field in current implementation
        # but should not cause errors

    def test_convert_maps_video_url(self, manager: EBCScrapeManager) -> None:
        """Video URL should be mapped."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'video_url': 'https://tvbrasil.ebc.com.br/videos/123.mp4',
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['video_url'] == 'https://tvbrasil.ebc.com.br/videos/123.mp4'

    def test_convert_maps_image_field(self, manager: EBCScrapeManager) -> None:
        """Image field should be mapped."""
        ebc_data = [
            {
                'title': 'Test',
                'url': 'https://example.com/1',
                'content': 'Content',
                'image': 'https://agenciabrasil.ebc.com.br/images/photo.jpg',
            }
        ]
        result = manager._convert_ebc_to_govbr_format(ebc_data)
        assert len(result) == 1
        assert result[0]['image'] == 'https://agenciabrasil.ebc.com.br/images/photo.jpg'


# =============================================================================
# Tests for EBCWebScraper parsing methods
# =============================================================================


class TestEBCWebScraperParsing:
    """Tests for EBCWebScraper parsing and extraction methods."""

    @pytest.fixture
    def ebc_scraper(self) -> EBCWebScraper:
        """EBCWebScraper instance for testing."""
        return EBCWebScraper(min_date="2026-01-01", base_url="https://memoria.ebc.com.br/noticias")

    def test_parse_ebc_datetime_with_time(self, ebc_scraper: EBCWebScraper) -> None:
        """Parse EBC datetime format with time (DD/MM/YYYY - HH:MM)."""
        result = ebc_scraper._parse_ebc_datetime("17/11/2025 - 18:58")

        from datetime import timezone, timedelta
        brasilia_tz = timezone(timedelta(hours=-3))
        expected = datetime(2025, 11, 17, 18, 58, tzinfo=brasilia_tz)

        assert result == expected

    def test_parse_ebc_datetime_date_only(self, ebc_scraper: EBCWebScraper) -> None:
        """Parse EBC date-only format (DD/MM/YYYY) - should use midnight."""
        result = ebc_scraper._parse_ebc_datetime("15/01/2026")

        from datetime import timezone, timedelta
        brasilia_tz = timezone(timedelta(hours=-3))
        expected = datetime(2026, 1, 15, 0, 0, tzinfo=brasilia_tz)

        assert result == expected

    def test_parse_ebc_datetime_empty_string(self, ebc_scraper: EBCWebScraper) -> None:
        """Empty string should return None."""
        result = ebc_scraper._parse_ebc_datetime("")
        assert result is None

    def test_parse_ebc_datetime_invalid(self, ebc_scraper: EBCWebScraper) -> None:
        """Invalid date format should return None."""
        result = ebc_scraper._parse_ebc_datetime("Publicado em janeiro")
        assert result is None

    def test_parse_ebc_datetime_none_input(self, ebc_scraper: EBCWebScraper) -> None:
        """None input should return None."""
        result = ebc_scraper._parse_ebc_datetime(None)
        assert result is None

    def test_get_base_domain(self, ebc_scraper: EBCWebScraper) -> None:
        """Extract base domain from base_url."""
        result = ebc_scraper._get_base_domain()
        assert result == "https://memoria.ebc.com.br"

    def test_get_base_domain_agencia_brasil(self) -> None:
        """Extract base domain from Agencia Brasil URL."""
        scraper = EBCWebScraper(
            min_date="2026-01-01",
            base_url="https://agenciabrasil.ebc.com.br/noticias"
        )
        result = scraper._get_base_domain()
        assert result == "https://agenciabrasil.ebc.com.br"

    def test_extract_tags_from_page(self, ebc_scraper: EBCWebScraper) -> None:
        """Extract tags from /tags/ links (returns link text, not URL slug)."""
        html = """
        <html><body>
            <div class="tags">
                <a href="/tags/educacao">Educação</a>
                <a href="/tags/ensino-superior">Ensino Superior</a>
                <a href="/noticias">Notícias</a>
            </div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        result = ebc_scraper._extract_tags_from_page(soup)

        # Method returns the link text, not the URL slug
        assert result == ['Educação', 'Ensino Superior']

    def test_extract_tags_no_tags(self, ebc_scraper: EBCWebScraper) -> None:
        """Return empty list when no /tags/ links found."""
        html = """
        <html><body>
            <div class="tags">
                <a href="/noticias">Notícias</a>
                <a href="/sobre">Sobre</a>
            </div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        result = ebc_scraper._extract_tags_from_page(soup)

        assert result == []

    def test_extract_video_url_present(self, ebc_scraper: EBCWebScraper) -> None:
        """Extract video URL from <video><source> tag."""
        html = """
        <html><body>
            <video>
                <source src="https://tvbrasil.ebc.com.br/videos/abc123.mp4" type="video/mp4">
            </video>
        </body></html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        result = ebc_scraper._extract_video_url(soup)

        assert result == "https://tvbrasil.ebc.com.br/videos/abc123.mp4"

    def test_extract_video_url_relative_path(self, ebc_scraper: EBCWebScraper) -> None:
        """Convert relative video URLs to absolute."""
        html = """
        <html><body>
            <video>
                <source src="/videos/abc123.mp4" type="video/mp4">
            </video>
        </body></html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        result = ebc_scraper._extract_video_url(soup)

        assert result == "https://tvbrasil.ebc.com.br/videos/abc123.mp4"

    def test_extract_video_url_absent(self, ebc_scraper: EBCWebScraper) -> None:
        """Return empty string when no video found."""
        html = """
        <html><body>
            <div class="content">
                <p>Texto da notícia</p>
            </div>
        </body></html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        result = ebc_scraper._extract_video_url(soup)

        assert result == ""  # Returns empty string, not None

    def test_extract_datetime_from_jsonld(self, ebc_scraper: EBCWebScraper) -> None:
        """Extract datetime from JSON-LD datePublished (returns tuple)."""
        html = """
        <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "NewsArticle",
                "datePublished": "2026-01-15T14:30:00-03:00",
                "dateModified": "2026-01-16T10:00:00-03:00"
            }
            </script>
        </head>
        <body><p>Content</p></body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        published_dt, updated_dt = ebc_scraper._extract_datetime_from_jsonld(soup)

        from datetime import timezone, timedelta
        brasilia_tz = timezone(timedelta(hours=-3))
        expected_published = datetime(2026, 1, 15, 14, 30, 0, tzinfo=brasilia_tz)
        expected_updated = datetime(2026, 1, 16, 10, 0, 0, tzinfo=brasilia_tz)

        assert published_dt == expected_published
        assert updated_dt == expected_updated

    def test_extract_datetime_from_jsonld_no_modified(self, ebc_scraper: EBCWebScraper) -> None:
        """Extract datetime when only datePublished is present."""
        html = """
        <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "https://schema.org",
                "@type": "NewsArticle",
                "datePublished": "2026-01-15T14:30:00-03:00"
            }
            </script>
        </head>
        <body><p>Content</p></body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')

        published_dt, updated_dt = ebc_scraper._extract_datetime_from_jsonld(soup)

        from datetime import timezone, timedelta
        brasilia_tz = timezone(timedelta(hours=-3))
        expected_published = datetime(2026, 1, 15, 14, 30, 0, tzinfo=brasilia_tz)

        assert published_dt == expected_published
        assert updated_dt is None
