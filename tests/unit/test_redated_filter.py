"""
Filtro conservador de notícias redatadas.

Depois do defeso eleitoral de 2026, vários sites republicaram conteúdo antigo com data
nova (a data nova aparece também na página do artigo). Como o unique_id usa título +
published_at, essas notícias entravam como novas e com data errada. Dois sinais baratos,
avaliados antes de baixar o artigo:

- Plone 6 (API): ``created`` muito anterior a ``effective`` (>= 30 dias). Ex.: capes
  "abertas-inscricoes-para-especializacao-na-irlanda": created 2022-12-08, effective 2026-10-27.
- HTML: pasta /AAAA/MM/ (ou /AAAA/<mês>/) da URL anterior ao mês da data da listagem por
  mais de 1 mês. Ex.: servidor ".../noticias/2024/agosto/..." listado em 26/10/2026.

Cada item pulado é logado (``skipped_redated``). Com SCRAPER_REDATED_FILTER=observe o
filtro só loga e mantém o item.
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.redated import (
    REDATED_MIN_GAP_DAYS,
    created_gap_days,
    is_stale_url_folder,
    url_folder_month,
)
from govbr_scraper.scrapers.webscraper import WebScraper

TZ_BR = timezone(timedelta(hours=-3))


@pytest.mark.parametrize(
    ("url", "esperado"),
    [
        ("https://www.gov.br/pf/pt-br/assuntos/noticias/2026/10/pf-prende", (2026, 10)),
        ("https://www.gov.br/servidor/pt-br/assuntos/noticias/2024/agosto/encontro", (2024, 8)),
        ("https://www.gov.br/servidor/pt-br/assuntos/noticias/2026/maio-1/copy_of_x", (2026, 5)),
        ("https://www.gov.br/ibict/pt-br/central-de-conteudos/noticias/2023/marco/x", (2023, 3)),
        ("https://www.gov.br/transportes/pt-br/assuntos/noticias/2026/copy_of_10/x", None),
        ("https://www.gov.br/mec/pt-br/assuntos/noticias/programa-novo", None),
        ("https://www.gov.br/planejamento/pt-br/assuntos/noticias/2026/governo-reduz", None),
    ],
)
def test_url_folder_month(url, esperado):
    assert url_folder_month(url) == esperado


@pytest.mark.parametrize(
    ("url", "ref", "esperado"),
    [
        # mais de 1 mês antes: redatado
        (
            "https://www.gov.br/servidor/pt-br/assuntos/noticias/2024/agosto/x",
            date(2026, 10, 26),
            True,
        ),
        (
            "https://www.gov.br/mcti/pt-br/acompanhe-o-mcti/noticias/2025/03/x",
            date(2026, 10, 27),
            True,
        ),
        ("https://www.gov.br/mcom/pt-br/noticias/2026/agosto/x", date(2026, 10, 27), True),
        # até 1 mês: publicação na virada do mês, mantém
        ("https://www.gov.br/pf/pt-br/assuntos/noticias/2026/10/x", date(2026, 11, 1), False),
        ("https://www.gov.br/pf/pt-br/assuntos/noticias/2026/10/x", date(2026, 10, 29), False),
        # pasta "no futuro" ou sem mês: não decide
        ("https://www.gov.br/pf/pt-br/assuntos/noticias/2026/11/x", date(2026, 10, 31), False),
        ("https://www.gov.br/mec/pt-br/assuntos/noticias/x", date(2026, 10, 27), False),
        ("https://www.gov.br/servidor/pt-br/assuntos/noticias/2024/agosto/x", None, False),
    ],
)
def test_is_stale_url_folder(url, ref, esperado):
    assert is_stale_url_folder(url, ref) is esperado


def test_created_gap_days():
    eff = datetime.fromisoformat("2026-10-27T12:18:36+00:00")
    assert created_gap_days("2022-12-08T12:22:05+00:00", eff) == pytest.approx(1418, abs=1)
    assert created_gap_days(None, eff) is None
    assert created_gap_days("lixo", eff) is None
    assert created_gap_days("2026-10-27T10:00:00+00:00", None) is None
    assert REDATED_MIN_GAP_DAYS == 30


# ---------------------------------------------------------------- WebScraper (HTML)


def _webscraper_item(scraper, url, news_date):
    scraper.extract_title_and_url = MagicMock(return_value=("Título", url))
    scraper.extract_category = MagicMock(return_value="Geral")
    scraper.extract_date = MagicMock(return_value=news_date)
    scraper.extract_tags = MagicMock(return_value=[])
    scraper.get_article_content = MagicMock(
        return_value=(
            "Texto do artigo",
            None,
            datetime.combine(news_date, datetime.min.time()).replace(tzinfo=TZ_BR),
            None,
            [],
            None,
            None,
            None,
        )
    )
    return MagicMock()


class TestWebScraperRedated:
    def _scraper(self):
        return WebScraper(
            min_date="2026-10-26",
            base_url="https://www.gov.br/servidor/pt-br/assuntos/noticias",
            max_date="2026-11-02",
        )

    def test_pula_pasta_antiga_sem_baixar_o_artigo(self, caplog):
        s = self._scraper()
        url = (
            "https://www.gov.br/servidor/pt-br/assuntos/noticias/2024/agosto/encontro-da-rede-girc"
        )
        item = _webscraper_item(s, url, date(2026, 10, 26))

        with caplog.at_level("WARNING"):
            assert s.extract_news_info(item) is True

        s.get_article_content.assert_not_called()
        assert s.news_data == []
        assert "skipped_redated" in caplog.text and url in caplog.text
        assert s.skipped_redated == 1

    def test_mantem_pasta_do_mes_anterior(self):
        s = self._scraper()
        url = "https://www.gov.br/servidor/pt-br/assuntos/noticias/2026/10/seminario"
        item = _webscraper_item(s, url, date(2026, 11, 1))

        assert s.extract_news_info(item) is True
        assert len(s.news_data) == 1

    def test_modo_observacao_so_loga(self, monkeypatch, caplog):
        monkeypatch.setenv("SCRAPER_REDATED_FILTER", "observe")
        s = self._scraper()
        url = (
            "https://www.gov.br/servidor/pt-br/assuntos/noticias/2024/agosto/encontro-da-rede-girc"
        )
        item = _webscraper_item(s, url, date(2026, 10, 26))

        with caplog.at_level("WARNING"):
            s.extract_news_info(item)

        assert len(s.news_data) == 1
        assert "observe" in caplog.text


# ---------------------------------------------------------------- Plone6APIScraper


def _api_item(created, effective="2026-10-27T12:18:36+00:00", slug="abertas-inscricoes"):
    item = {
        "@id": f"https://www.gov.br/capes/pt-br/assuntos/noticias/{slug}",
        "title": "Abertas inscrições para especialização na Irlanda",
        "effective": effective,
        "modified": effective,
        "description": "Resumo",
        "text": {"data": "<p>Conteúdo</p>"},
    }
    if created is not None:
        item["created"] = created
    return item


class TestPlone6Redated:
    def _scraper(self):
        return Plone6APIScraper(
            min_date="2026-10-23",
            base_url="https://www.gov.br/capes/pt-br/assuntos/noticias",
            max_date="2026-11-02",
        )

    def test_pula_created_muito_antigo(self, caplog):
        s = self._scraper()
        with caplog.at_level("WARNING"):
            assert s._process_news_item(_api_item("2022-12-08T12:22:05+00:00")) is True
        assert s.news_data == []
        assert s.skipped_redated == 1
        assert "skipped_redated" in caplog.text

    def test_limiar_de_30_dias(self):
        s = self._scraper()
        s._process_news_item(_api_item("2026-09-27T12:18:36+00:00", slug="trinta"))  # 30 dias
        s._process_news_item(_api_item("2026-09-28T12:18:37+00:00", slug="vinte-e-nove"))  # < 30
        assert [i["url"].rsplit("/", 1)[1] for i in s.news_data] == ["vinte-e-nove"]

    def test_mantem_rascunho_recente_e_item_sem_created(self):
        s = self._scraper()
        s._process_news_item(_api_item("2026-10-20T09:00:00+00:00", slug="rascunho"))
        s._process_news_item(_api_item(None, slug="sem-created"))
        assert len(s.news_data) == 2
        assert s.skipped_redated == 0

    def test_modo_observacao_so_loga(self, monkeypatch, caplog):
        monkeypatch.setenv("SCRAPER_REDATED_FILTER", "observe")
        s = self._scraper()
        with caplog.at_level("WARNING"):
            s._process_news_item(_api_item("2022-12-08T12:22:05+00:00"))
        assert len(s.news_data) == 1
        assert "observe" in caplog.text
