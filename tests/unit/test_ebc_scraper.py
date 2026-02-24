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
    return EBCWebScraper(min_date="2026-01-01")


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

    def test_tvbrasil_source_is_empty(
        self,
        ebc_scraper: EBCWebScraper,
        tvbrasil_html: str,
        empty_tvbrasil_news_data: Dict[str, Any],
    ) -> None:
        """TV Brasil source field is empty after extraction."""
        soup = BeautifulSoup(tvbrasil_html, 'html.parser')
        news_data = empty_tvbrasil_news_data.copy()
        news_data['source'] = 'should_be_cleared'

        ebc_scraper._scrape_tvbrasil_content(soup, news_data)

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

    def test_convert_handles_empty_editorial_lead(self, manager: EBCScrapeManager) -> None:
        """Empty editorial_lead becomes None in converted format."""
        ebc_data = [
            {
                'title': 'Test Article',
                'url': 'https://agenciabrasil.ebc.com.br/test',
                'source': 'Agencia Brasil',
                'date': '15/01/2026 - 14:30',
                'published_datetime': datetime(2026, 1, 15, 14, 30),
                'updated_datetime': None,
                'tags': [],
                'editorial_lead': '',  # Empty for Agencia Brasil
                'content': 'Test content here.',
                'image': '',
                'video_url': '',
                'agency': 'agencia_brasil',
                'error': '',
            }
        ]

        result = manager._convert_ebc_to_govbr_format(ebc_data)

        assert len(result) == 1
        assert result[0]['editorial_lead'] is None

    def test_convert_handles_missing_editorial_lead(self, manager: EBCScrapeManager) -> None:
        """Missing editorial_lead key becomes None in converted format."""
        ebc_data = [
            {
                'title': 'Test Article',
                'url': 'https://agenciabrasil.ebc.com.br/test',
                'source': 'Agencia Brasil',
                'date': '15/01/2026 - 14:30',
                'published_datetime': datetime(2026, 1, 15, 14, 30),
                'updated_datetime': None,
                'tags': [],
                # No editorial_lead key at all
                'content': 'Test content here.',
                'image': '',
                'video_url': '',
                'agency': 'agencia_brasil',
                'error': '',
            }
        ]

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
