"""
Conteúdo inválido não é persistido (scraper#65).

O scraper gravava como notícia a tela de login do Plone, o marcador "Error retrieving
content" e conteúdo vazio. Esses itens agora são pulados e contados, sem chegar ao
storage. Como a URL não é gravada, ela não entra em known_urls e o artigo é recapturado
numa execução seguinte, se voltar a responder com conteúdo.
"""

from unittest.mock import MagicMock, patch

import pytest
from govbr_scraper.scrapers.content_validation import (
    ERROR_CONTENT,
    LOGIN_REQUIRED_CONTENT,
    invalid_content_reason,
)

LOGIN_TEXT = (
    "É necessário autenticar para visualizar essa página. Se você acredita que está "
    "recebendo essa mensagem incorretamente, por favor contate a administração do site."
)


@pytest.mark.parametrize(
    ("content", "esperado"),
    [
        (ERROR_CONTENT, "error_content"),
        (f"  {ERROR_CONTENT}\n", "error_content"),
        (LOGIN_REQUIRED_CONTENT, "login_page"),
        (LOGIN_TEXT, "login_page"),
        ("Necessario autenticar para visualizar esta pagina", "login_page"),
        ("", "empty_content"),
        ("   \n ", "empty_content"),
        (None, "empty_content"),
        ("O Ministério anunciou hoje um novo programa de bolsas.", None),
    ],
)
def test_invalid_content_reason(content, esperado):
    assert invalid_content_reason({"content": content}) == esperado


class TestWebScraperLoginRedirect:
    def test_redirect_para_require_login_vira_marcador_de_login(self):
        from govbr_scraper.scrapers.webscraper import WebScraper

        scraper = WebScraper("2026-10-27", "https://www.gov.br/mdr/pt-br/noticias")
        resp = MagicMock()
        resp.url = (
            "https://www.gov.br/mdr/acl_users/credentials_cookie_auth/require_login"
            "?came_from=https%3A//www.gov.br/mdr/pt-br/noticias/x"
        )
        resp.content = f"<html><div id='content'><p>{LOGIN_TEXT}</p></div></html>".encode()
        scraper.fetch_page = MagicMock(return_value=resp)

        content, *_ = scraper.get_article_content("https://www.gov.br/mdr/pt-br/noticias/x")

        assert content == LOGIN_REQUIRED_CONTENT


class TestScrapeManagerSkipsInvalidContent:
    def _manager(self):
        from govbr_scraper.scrapers.scrape_manager import ScrapeManager

        storage = MagicMock()
        storage.get_recent_urls.return_value = set()
        storage.insert.return_value = 1
        return ScrapeManager(storage), storage

    @pytest.mark.parametrize("sequential", [True, False])
    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_nao_persiste_e_conta_os_pulados(self, mock_load, mock_ws_cls, sequential, caplog):
        mock_load.return_value = {
            "mdr": {"url": "https://www.gov.br/mdr/pt-br/noticias", "scraper_type": "html"}
        }
        base = {"agency": "mdr", "published_at": "2026-10-27", "title": "T"}
        scraper = MagicMock()
        scraper.scrape_news.return_value = [
            {**base, "url": "https://www.gov.br/mdr/pt-br/noticias/ok", "content": "Texto válido"},
            {**base, "url": "https://www.gov.br/mdr/pt-br/noticias/erro", "content": ERROR_CONTENT},
            {**base, "url": "https://www.gov.br/mdr/pt-br/noticias/login", "content": LOGIN_TEXT},
            {**base, "url": "https://www.gov.br/mdr/pt-br/noticias/vazio", "content": ""},
        ]
        mock_ws_cls.return_value = scraper
        manager, storage = self._manager()

        with caplog.at_level("WARNING"):
            result = manager.run_scraper(
                agencies=["mdr"],
                min_date="2026-10-27",
                max_date="2026-10-27",
                sequential=sequential,
            )

        storage.insert.assert_called_once()
        gravados = storage.insert.call_args[0][0]["url"]
        assert list(gravados) == ["https://www.gov.br/mdr/pt-br/noticias/ok"]
        assert result["articles_skipped"] == 3
        for reason in ("error_content", "login_page", "empty_content"):
            assert f"reason={reason}" in caplog.text

    @patch("govbr_scraper.scrapers.scrape_manager.WebScraper")
    @patch("govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml")
    def test_tudo_invalido_nao_chama_insert(self, mock_load, mock_ws_cls):
        mock_load.return_value = {
            "mdr": {"url": "https://www.gov.br/mdr/pt-br/noticias", "scraper_type": "html"}
        }
        scraper = MagicMock()
        scraper.scrape_news.return_value = [
            {
                "agency": "mdr",
                "url": "https://www.gov.br/mdr/x",
                "title": "T",
                "content": ERROR_CONTENT,
            }
        ]
        mock_ws_cls.return_value = scraper
        manager, storage = self._manager()

        result = manager.run_scraper(
            agencies=["mdr"], min_date="2026-10-27", max_date="2026-10-27", sequential=True
        )

        storage.insert.assert_not_called()
        assert result["articles_skipped"] == 1
        run = storage.record_scrape_run.call_args[0][0]
        assert run.status == "success"
        assert run.articles_saved == 0


def test_api_response_expoe_articles_skipped():
    from govbr_scraper.api import ScrapeResponse

    resp = ScrapeResponse(
        status="completed", start_date="2026-11-02", end_date="2026-11-02", message="ok"
    )
    assert resp.articles_skipped == 0
