"""
mec e antaq com html_changed em toda execução (mec desde 28/10/2026, antaq desde 16/11/2026).

- mec: assuntos/noticias virou uma capa (collective.cover) com um tile de coleção
  (div.collection-item, só as 20 notícias mais recentes, sem paginação: ?b_start:int=20 devolve
  as mesmas 20). O link "Mais notícias..." do tile leva à coleção colecao-noticias-2026, que
  lista em article.tileItem, 20 por página. A API Plone 6 dá 404 no site.
- antaq: noticias redireciona (302) para central-de-conteudos/noticias, que é Plone 6 (Volto):
  o HTML do SSR não traz os itens e a API ++api++ responde na URL nova.

As fixtures em fixtures/govbr/ são versões reduzidas das páginas baixadas em 19/11/2026 pela VM
do Airflow (IP do Brasil), com os cabeçalhos do WebScraper.
"""

import json
import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.models.monitoring import ErrorCategory, classify_exception
from govbr_scraper.scrapers.content_validation import invalid_content_reason
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.unique_id import generate_readable_unique_id
from govbr_scraper.scrapers.webscraper import ScrapingError, WebScraper
from govbr_scraper.scrapers.yaml_config import get_config_dir, load_urls_from_yaml

FIXTURES = Path(__file__).parent / "fixtures" / "govbr"

_SCRAPERS_MODULE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "..",
    "src",
    "govbr_scraper",
    "scrapers",
    "scrape_manager.py",
)

MEC_COVER_URL = "https://www.gov.br/mec/pt-br/assuntos/noticias"
ANTAQ_OLD_URL = "https://www.gov.br/antaq/pt-br/noticias"


def _agency_config(agency: str) -> dict:
    return load_urls_from_yaml(get_config_dir(_SCRAPERS_MODULE), "site_urls.yaml", agency)[agency]


def _html_response(name: str, url: str) -> MagicMock:
    body = (FIXTURES / name).read_bytes()
    response = MagicMock()
    response.content = body
    response.text = body.decode("utf-8")
    response.url = url
    response.status_code = 200
    return response


def _antaq_api() -> dict:
    return json.loads((FIXTURES / "antaq_api_noticias.json").read_text(encoding="utf-8"))


def _antaq_item(fragment: str) -> dict:
    return next(item for item in _antaq_api()["items"] if fragment in item["@id"])


# =============================================================================
# Configuração (site_urls.yaml)
# =============================================================================


class TestConfig:
    def test_mec_usa_a_colecao_e_nao_a_capa(self):
        cfg = _agency_config("mec")
        assert cfg["url"].rstrip("/") != MEC_COVER_URL
        assert cfg["url"].startswith(f"{MEC_COVER_URL}/colecao-noticias-")
        assert cfg["scraper_type"] == "html"

    def test_antaq_usa_plone6_api_na_url_nova(self):
        cfg = _agency_config("antaq")
        assert cfg["url"] == "https://www.gov.br/antaq/pt-br/central-de-conteudos/noticias"
        assert cfg["scraper_type"] == "plone6_api"

    def test_antaq_monta_a_url_da_api_na_url_nova(self):
        scraper = Plone6APIScraper("2026-11-13", _agency_config("antaq")["url"])
        api_url = scraper._build_api_url(0, 25)
        assert api_url.startswith(
            "https://www.gov.br/antaq/++api++/pt-br/central-de-conteudos/noticias/@search?"
        )
        assert scraper.agency == "antaq"


# =============================================================================
# mec: a capa não tem itens que o WebScraper reconheça; a coleção tem
# =============================================================================


@patch("govbr_scraper.scrapers.webscraper.time.sleep")
class TestMec:
    def test_capa_falha_com_html_changed(self, _sleep):
        """A causa raiz: a capa só tem div.collection-item, que nenhum seletor pega."""
        scraper = WebScraper("2026-11-17", MEC_COVER_URL, max_date="2026-11-19")
        page_url = f"{MEC_COVER_URL}?b_start:int=0"
        with patch.object(
            scraper, "fetch_page", return_value=_html_response("mec_capa_noticias.html", page_url)
        ):
            with pytest.raises(
                ScrapingError, match="No articles found on first page of mec"
            ) as exc:
                scraper.scrape_page(page_url)
        assert classify_exception(exc.value) == ErrorCategory.HTML_CHANGED

    def test_colecao_lista_os_itens_com_data_e_hora(self, _sleep):
        url = _agency_config("mec")["url"]
        scraper = WebScraper("2026-11-17", url, max_date="2026-11-19")
        page_url = f"{url}?b_start:int=0"
        article = ("conteúdo " * 30, None, None, None, [], None, None, None)
        with (
            patch.object(
                scraper,
                "fetch_page",
                return_value=_html_response("mec_colecao_noticias_2026.html", page_url),
            ),
            patch.object(scraper, "get_article_content", return_value=article) as get_article,
        ):
            should_continue, items_per_page = scraper.scrape_page(page_url)

        assert (should_continue, items_per_page) == (True, 4)
        assert get_article.call_count == 4
        first = scraper.news_data[0]
        assert first["title"] == "Educação leva pai e filhas a dividirem a mesma universidade"
        assert first["url"] == (
            "https://www.gov.br/mec/pt-br/assuntos/noticias/2026/novembro/"
            "educacao-leva-pai-e-filhas-a-dividirem-a-mesma-universidade"
        )
        assert first["category"] == "EDUCAÇÃO SUPERIOR"
        # Sem data no artigo, vale a da listagem (meia-noite BRT do dia)
        assert first["published_at"].date() == date(2026, 11, 19)
        assert all(item["agency"] == "mec" for item in scraper.news_data)

    def test_colecao_para_na_data_minima(self, _sleep):
        url = _agency_config("mec")["url"]
        scraper = WebScraper("2026-11-20", url, max_date="2026-11-21")
        page_url = f"{url}?b_start:int=0"
        with patch.object(
            scraper,
            "fetch_page",
            return_value=_html_response("mec_colecao_noticias_2026.html", page_url),
        ):
            should_continue, _ = scraper.scrape_page(page_url)
        assert should_continue is False
        assert scraper.news_data == []


# =============================================================================
# antaq: listagem Volto sem itens no HTML; API Plone 6 com os itens
# =============================================================================


class TestAntaqHtml:
    @patch("govbr_scraper.scrapers.webscraper.time.sleep")
    def test_listagem_volto_falha_com_html_changed(self, _sleep):
        """A causa raiz: o HTML da listagem Volto não tem itens (o bloco de busca carrega no cliente)."""
        scraper = WebScraper("2026-11-17", ANTAQ_OLD_URL, max_date="2026-11-19")
        page_url = f"{ANTAQ_OLD_URL}?b_start:int=0"
        final_url = "https://www.gov.br/antaq/pt-br/central-de-conteudos/noticias?b_start:int=0"
        with patch.object(
            scraper,
            "fetch_page",
            return_value=_html_response("antaq_central_noticias_volto.html", final_url),
        ):
            with pytest.raises(ScrapingError, match="No articles found on first page of antaq"):
                scraper.scrape_page(page_url)


class TestAntaqPlone6:
    def test_conteudo_segue_a_ordem_da_pagina(self):
        item = _antaq_item("antaq-apresenta-guia-com-orientacoes")
        scraper = Plone6APIScraper("2026-11-13", _agency_config("antaq")["url"])
        content = scraper._transform_news_item(item, None)["content"]

        # No objeto blocks da API o primeiro parágrafo é o penúltimo da notícia
        first_in_dict = next(b for b in item["blocks"].values() if b.get("@type") == "slate")
        assert first_in_dict["plaintext"].startswith("A publicação dialoga ainda")

        assert content.startswith(
            "A Agência Nacional de Transportes Aquaviários (ANTAQ) lançou, nesta quinta-feira (19)"
        )
        order = [
            "Durante a abertura, Frederico Dias",
            "Mesa-redonda",
            "Representatividade e igualdade racial",
            "O guia também orienta",
            "A publicação dialoga ainda",
        ]
        positions = [content.index(fragment) for fragment in order]
        assert positions == sorted(positions)

    def test_blocos_html_viram_conteudo(self):
        """Notícia migrada do Plone 5 (só blocos html) não pode sair vazia (era pulada)."""
        item = _antaq_item("antaq-prorroga-prazo-para-contribuicoes")
        scraper = Plone6APIScraper("2026-11-13", _agency_config("antaq")["url"])
        transformed = scraper._transform_news_item(item, None)

        content = transformed["content"]
        assert content.startswith(
            "Brasília, 13/11/2026 – A Agência Nacional de Transportes Aquaviários (ANTAQ) prorrogou"
        )
        assert content.index("**Como participar**") < content.index("As contribuições recebidas")
        assert "<p" not in content
        assert invalid_content_reason(transformed) is None

    def test_blocos_slate_e_html_na_mesma_noticia(self):
        item = _antaq_item("tomada-de-subsidios-recebe")
        scraper = Plone6APIScraper("2026-11-13", _agency_config("antaq")["url"])
        content = scraper._transform_news_item(item, None)["content"]

        assert content.startswith("Brasília, 13/11/2026 – A Agência Nacional de Transportes")
        assert content.index("Como participar") < content.index(
            "As contribuições recebidas serão disponibilizadas"
        )
        assert content.index("As contribuições recebidas serão disponibilizadas") < content.index(
            "Assessoria de Comunicação e Cerimonial"
        )

    @patch("govbr_scraper.scrapers.plone6_api_scraper.time.sleep")
    def test_scrape_news_traz_a_janela_com_datas(self, _sleep):
        scraper = Plone6APIScraper(
            "2026-11-13", _agency_config("antaq")["url"], max_date="2026-11-19"
        )
        first_page = _antaq_api()
        end = {"items": [], "items_total": first_page["items_total"]}
        with patch.object(scraper, "_fetch_api_page", side_effect=[first_page, end]):
            news = scraper.scrape_news()

        assert [n["published_at"].date() for n in news] == [
            date(2026, 11, 19),
            date(2026, 11, 13),
            date(2026, 11, 13),
        ]
        assert all(n["agency"] == "antaq" for n in news)
        assert all(invalid_content_reason(n) is None for n in news)
        assert all(n["url"].startswith(_agency_config("antaq")["url"] + "/") for n in news)

    @patch("govbr_scraper.scrapers.plone6_api_scraper.time.sleep")
    def test_unique_id_igual_ao_ja_gravado(self, _sleep):
        """As de 13/11 já estão no banco (raspadas pelo WebScraper até 13/11/2026).

        Com a data da API em UTC, o unique_id seria outro (d375b1 e 46a6a4); a notícia que
        mudasse de URL (as até 03/11 foram para .../noticias/2026/) entraria de novo.
        """
        scraper = Plone6APIScraper(
            "2026-11-13", _agency_config("antaq")["url"], max_date="2026-11-13"
        )
        first_page = _antaq_api()
        end = {"items": [], "items_total": first_page["items_total"]}
        with patch.object(scraper, "_fetch_api_page", side_effect=[first_page, end]):
            news = scraper.scrape_news()

        assert [n["published_at"].isoformat() for n in news] == [
            "2026-11-13T16:03:06-03:00",
            "2026-11-13T12:13:24-03:00",
        ]
        assert [
            generate_readable_unique_id("antaq", n["published_at"], n["title"]) for n in news
        ] == [
            "tomada-de-subsidios-recebe-contribuicoes-para-avaliar-regras-de-liberacao-de-atividades"
            "-economicas_0c5d71",
            "antaq-prorroga-prazo-para-contribuicoes-sobre-transferencia-de-combustivel-entre"
            "-embarcacoes_acf4e7",
        ]
