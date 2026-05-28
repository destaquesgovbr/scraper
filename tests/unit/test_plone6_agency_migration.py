"""
Testes validando que agencias migradas para Plone6APIScraper estao
configuradas corretamente (17 agencias: 12 em data-platform#147 + 5 em scraper#53).
"""
import os

import pytest
from unittest.mock import MagicMock, patch

from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.scrape_manager import ScrapeManager
from govbr_scraper.scrapers.webscraper import WebScraper
from govbr_scraper.scrapers.yaml_config import get_config_dir, load_urls_from_yaml

_SCRAPERS_MODULE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "src",
    "govbr_scraper",
    "scrapers",
    "scrape_manager.py",
)

MIGRATED_AGENCIES = [
    # 12 agencias originais (maio 2026, data-platform#147)
    "censipam",
    "inpp",
    "esd",
    "transferegov",
    "fundacentro",
    "museugoeldi",
    "aids",
    "anpd",
    "florestal",
    "ouvidorias",
    "mulheres",
    "insa",
    # 5 agencias novas (maio 2026, scraper#53)
    "funai",
    "previc",
    "int",
    "portos-e-aeroportos",
    "iphan",
]


class TestMigratedAgenciesConfig:
    """Valida que o YAML tem scraper_type=plone6_api para todas as 17 agencias."""

    @pytest.fixture
    def config_dir(self):
        return get_config_dir(_SCRAPERS_MODULE)

    @pytest.mark.parametrize("agency", MIGRATED_AGENCIES)
    def test_agency_has_plone6_api_scraper_type(self, config_dir, agency):
        """Cada agencia migrada deve ter scraper_type=plone6_api."""
        result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency=agency)
        assert result[agency]["scraper_type"] == "plone6_api", (
            f"{agency} deveria ter scraper_type=plone6_api, "
            f"mas tem '{result[agency]['scraper_type']}'"
        )

    def test_transferegov_url_not_year_specific(self, config_dir):
        """URL do transferegov NAO deve apontar para subfolder de ano."""
        result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="transferegov")
        url = result["transferegov"]["url"]
        assert "/2025" not in url, (
            f"URL do transferegov nao deve conter /2025 (ano especifico). Got: {url}"
        )
        assert "/2026" not in url, (
            f"URL do transferegov nao deve conter /2026 (ano especifico). Got: {url}"
        )

    def test_fundacentro_url_not_ultimas_noticias(self, config_dir):
        """URL da fundacentro NAO deve apontar para ultimas-noticias (retorna 0 itens na API)."""
        result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="fundacentro")
        url = result["fundacentro"]["url"]
        assert "ultimas-noticias" not in url, (
            f"URL da fundacentro nao deve conter 'ultimas-noticias'. Got: {url}"
        )

    def test_funai_url_not_year_specific(self, config_dir):
        """URL da funai NAO deve apontar para subfolder de ano."""
        result = load_urls_from_yaml(config_dir, "site_urls.yaml", agency="funai")
        url = result["funai"]["url"]
        assert "/2025" not in url, (
            f"URL da funai nao deve conter /2025 (ano especifico). Got: {url}"
        )
        assert "/2026" not in url, (
            f"URL da funai nao deve conter /2026 (ano especifico). Got: {url}"
        )


class TestMigratedAgenciesScraperSelection:
    """Valida que ScrapeManager instancia Plone6APIScraper para agencias migradas."""

    @pytest.fixture
    def mock_scrapers(self):
        with patch.object(WebScraper, "__init__", return_value=None) as mock_ws_init, \
             patch.object(WebScraper, "scrape_news", return_value=[]) as mock_ws_scrape, \
             patch.object(Plone6APIScraper, "__init__", return_value=None) as mock_p6_init, \
             patch.object(Plone6APIScraper, "scrape_news", return_value=[]) as mock_p6_scrape:
            yield {
                "ws_init": mock_ws_init,
                "ws_scrape": mock_ws_scrape,
                "p6_init": mock_p6_init,
                "p6_scrape": mock_p6_scrape,
            }

    @pytest.mark.parametrize("agency", MIGRATED_AGENCIES)
    def test_scrape_manager_uses_plone6_scraper(self, agency, mock_scrapers):
        """ScrapeManager deve usar Plone6APIScraper para cada agencia migrada."""
        storage = MagicMock()
        storage.get_recent_urls.return_value = set()
        storage.insert.return_value = 0
        manager = ScrapeManager(storage)

        manager.run_scraper(
            agencies=[agency],
            min_date="2026-05-01",
            max_date="2026-05-08",
            sequential=True,
        )

        mock_scrapers["p6_init"].assert_called_once()
        mock_scrapers["ws_init"].assert_not_called()
