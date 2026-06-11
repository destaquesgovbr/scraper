"""
Testes validando que agencias migradas para Plone6APIScraper estao
configuradas corretamente (17 agencias: 12 em data-platform#147 + 5 em scraper#53).
"""
import os

import pytest

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
    "abc",
    "aids",
    "anatel",
    "ansn",
    "anpd",
    "censipam",
    "ctav",
    "esd",
    "esg",
    "esporte",
    "florestal",
    "funai",
    "fundacentro",
    "hfa",
    "ibc",
    "incra",
    "inpp",
    "insa",
    "int",
    "iphan",
    "mast",
    "memp",
    "memoriasreveladas",
    "mulheres",
    "museugoeldi",
    "ouvidorias",
    "patrimonio",
    "planejamento",
    "pncp",
    "portos-e-aeroportos",
    "povosindigenas",
    "previc",
    "propriedade-intelectual",
    "reconstrucaors",
    "sudeco",
    "susep",
    "transferegov",
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


