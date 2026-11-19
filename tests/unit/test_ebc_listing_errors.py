"""Primeira página do índice da EBC: falha vira exceção classificada.

Antes, ``fetch_page`` engolia a falha e ``scrape_index_page`` devolvia ``[]``: com a
listagem fora do ar (rede, 5xx) ou com o HTML mudado, a fonte saía como ``success`` com
0 artigos, e o disjuntor e a marca d'água do estado por agência (Passo 2 da
rearquitetura da ingestão) nunca atuariam para a EBC. Agora a página 0:

- não baixada depois dos retries (5xx, 408, 429, timeout, conexão): ``network_error``;
- sem nenhum link de artigo: ``html_changed``;
- 4xx permanente: ``PermanentHTTPError`` (``url_broken``/``access_denied``), como no gov.br.

Nas páginas seguintes, a falha de download também levanta erro (``network_error``,
passageiro): encerrar como sucesso deixaria parte da janela sem raspar, e a marca d'água
avançaria sobre ela. Página seguinte sem links é o fim da listagem.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from govbr_scraper.models.monitoring import ErrorCategory, classify_exception, is_retryable
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
from govbr_scraper.scrapers.ebc_webscraper import EBCWebScraper
from govbr_scraper.scrapers.errors import ListingPageError, PermanentHTTPError, ScrapingError
from govbr_scraper.scrapers.http_policy import RETRY_TRIES
from govbr_scraper.storage.storage_adapter import InsertCounts

BASE_URL = "https://agenciabrasil.ebc.com.br/ultimas"
ARTICLE_1 = "https://agenciabrasil.ebc.com.br/geral/noticia/2026-11/primeira"
ARTICLE_2 = "https://agenciabrasil.ebc.com.br/geral/noticia/2026-11/segunda"
ARTICLE_3 = "https://agenciabrasil.ebc.com.br/geral/noticia/2026-11/terceira"

INDEX_WITH_LINKS = f"""
<html><body>
  <a class="capa-noticia" href="{ARTICLE_1}">Primeira</a>
  <a class="capa-noticia" href="{ARTICLE_2}">Segunda</a>
</body></html>
""".encode()

INDEX_WITHOUT_LINKS = (
    b"<html><body><div class='nova-estrutura'>" + b"x" * 30000 + b"</div></body></html>"
)


def _response(status: int, content: bytes = b"", url: str = BASE_URL) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.url = url
    response._content = content
    return response


def _article(date_text: str = "19/11/2026 - 10:00") -> dict:
    return {"title": "Notícia", "url": ARTICLE_1, "date": date_text, "error": ""}


@pytest.fixture(autouse=True)
def no_sleep():
    """Neutraliza as esperas do @retry e o smart_sleep entre artigos."""
    with (
        patch("retry.api.time.sleep") as mock_sleep,
        patch("govbr_scraper.scrapers.ebc_webscraper.time.sleep"),
    ):
        yield mock_sleep


@pytest.fixture
def scraper() -> EBCWebScraper:
    return EBCWebScraper("2026-11-19", BASE_URL, max_date="2026-11-19")


class TestFirstIndexPage:
    @pytest.mark.parametrize(
        "side_effect",
        [
            requests.exceptions.ConnectionError("Connection refused"),
            requests.exceptions.ReadTimeout("Read timed out"),
            _response(503),
            _response(429),
        ],
        ids=["conexao", "timeout", "503", "429"],
    )
    def test_network_failure_raises_network_error(self, scraper, side_effect):
        with patch("requests.get", side_effect=lambda *a, **k: _raise_or_return(side_effect)) as g:
            with pytest.raises(ListingPageError) as exc_info:
                scraper.scrape_news()

        assert g.call_count == RETRY_TRIES  # falha transitória: repetida antes de desistir
        assert exc_info.value.error_category == ErrorCategory.NETWORK_ERROR
        assert classify_exception(exc_info.value) == ErrorCategory.NETWORK_ERROR
        assert is_retryable(classify_exception(exc_info.value)) is True
        assert isinstance(exc_info.value, ScrapingError)

    def test_page_without_article_links_raises_html_changed(self, scraper):
        with patch("requests.get", return_value=_response(200, INDEX_WITHOUT_LINKS)) as g:
            with pytest.raises(ListingPageError) as exc_info:
                scraper.scrape_news()

        assert g.call_count == 1
        assert "No articles found on first page" in str(exc_info.value)
        assert classify_exception(exc_info.value) == ErrorCategory.HTML_CHANGED
        assert is_retryable(classify_exception(exc_info.value)) is False

    def test_empty_page_raises_html_changed(self, scraper):
        """Página 0 vazia não é "fim da listagem": /ultimas sempre tem notícias."""
        with patch("requests.get", return_value=_response(200, b"")):
            with pytest.raises(ListingPageError) as exc_info:
                scraper.scrape_news()

        assert classify_exception(exc_info.value) == ErrorCategory.HTML_CHANGED

    @pytest.mark.parametrize(
        ("status", "category"),
        [(404, ErrorCategory.URL_BROKEN), (410, ErrorCategory.URL_BROKEN)]
        + [(401, ErrorCategory.ACCESS_DENIED), (403, ErrorCategory.ACCESS_DENIED)],
    )
    def test_permanent_4xx_propagates_without_retry(self, scraper, status, category):
        with patch("requests.get", return_value=_response(status)) as g:
            with pytest.raises(PermanentHTTPError) as exc_info:
                scraper.scrape_news()

        assert g.call_count == 1
        assert exc_info.value.http_status == status
        assert classify_exception(exc_info.value) == category
        assert is_retryable(classify_exception(exc_info.value)) is False

    def test_first_page_ok_scrapes_articles(self, scraper):
        responses = [_response(200, INDEX_WITH_LINKS), _response(200, b"")]
        with (
            patch("requests.get", side_effect=responses),
            patch.object(scraper, "scrape_news_page", return_value=_article()) as page,
        ):
            data = scraper.scrape_news()

        assert page.call_count == 2
        assert len(data) == 2


class TestLaterIndexPages:
    """Depois da página 0, a falha de download é erro passageiro (a janela não foi coberta);
    página sem links é o fim da listagem e mantém o que foi coletado."""

    @pytest.mark.parametrize(
        ("page_1", "calls"),
        [
            (_response(503), RETRY_TRIES),
            (requests.exceptions.ReadTimeout("Read timed out"), RETRY_TRIES),
            (_response(404), 1),
            (_response(403), 1),
        ],
        ids=["503", "timeout", "404", "403"],
    )
    def test_download_failure_after_first_page_is_transient_error(self, scraper, page_1, calls):
        responses = [_response(200, INDEX_WITH_LINKS)] + [page_1] * RETRY_TRIES
        with (
            patch("requests.get", side_effect=lambda *a, **k: _raise_or_return(responses.pop(0))),
            patch.object(scraper, "scrape_news_page", return_value=_article()),
        ):
            with pytest.raises(ListingPageError) as exc_info:
                scraper.scrape_news()

        assert len(responses) == RETRY_TRIES - calls  # 4xx: sem retry
        assert "page=1" in str(exc_info.value)
        assert classify_exception(exc_info.value) == ErrorCategory.NETWORK_ERROR
        assert is_retryable(classify_exception(exc_info.value)) is True

    def test_page_without_links_ends_the_listing(self, scraper):
        responses = [_response(200, INDEX_WITH_LINKS), _response(200, INDEX_WITHOUT_LINKS)]
        with (
            patch("requests.get", side_effect=responses),
            patch.object(scraper, "scrape_news_page", return_value=_article()),
        ):
            data = scraper.scrape_news()

        assert len(data) == 2

    def test_fence_stops_before_the_next_page(self, scraper):
        """Execução normal: a cerca para na página 0 e a página 1 nem é pedida."""
        scraper.known_urls = {ARTICLE_1, ARTICLE_2, ARTICLE_3}
        index = INDEX_WITH_LINKS.replace(
            b"</body>", f'<a class="capa-noticia" href="{ARTICLE_3}">T</a></body>'.encode()
        )
        with patch("requests.get", side_effect=[_response(200, index)]) as get:
            assert scraper.scrape_news() == []
        assert get.call_count == 1

    def test_scrape_index_page_still_returns_empty_list_on_failure(self, scraper):
        with patch("requests.get", return_value=_response(503)):
            assert scraper.scrape_index_page(f"{BASE_URL}?page=3") == []


def _raise_or_return(value):
    if isinstance(value, BaseException):
        raise value
    return value


class TestEbcManagerRecordsListingFailure:
    """A falha da página 0 chega a errors[] e a scrape_runs com a categoria certa."""

    def _run(self, response):
        storage = MagicMock()
        storage.get_recent_urls.return_value = set()
        storage.insert.return_value = InsertCounts(saved=0, new=0)
        with patch("requests.get", side_effect=lambda *a, **k: _raise_or_return(response)):
            result = EBCScrapeManager(storage).run_scraper(
                min_date="2026-11-19",
                max_date="2026-11-19",
                sequential=True,
                agencies=["agencia_brasil"],
            )
        return result, storage

    def test_later_page_failure_is_an_error_and_nothing_is_saved(self):
        """Página 1 fora do ar: erro passageiro, nada gravado e sem sucesso registrado (a
        marca d'água não avança sobre a parte da janela que não foi coberta)."""
        storage = MagicMock()
        storage.get_recent_urls.return_value = set()
        responses = [_response(200, INDEX_WITH_LINKS)] + [_response(503)] * RETRY_TRIES
        with (
            patch("requests.get", side_effect=responses),
            patch.object(EBCWebScraper, "scrape_news_page", return_value=_article()),
        ):
            result = EBCScrapeManager(storage).run_scraper(
                min_date="2026-11-17",
                max_date="2026-11-19",
                sequential=True,
                agencies=["agencia_brasil"],
            )

        assert result["agencies_processed"] == []
        (error,) = result["errors"]
        assert (error["category"], error["retryable"]) == ("network_error", True)
        storage.insert.assert_not_called()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.status == "error"

    @pytest.mark.parametrize(
        ("response", "category", "retryable"),
        [
            (_response(503), "network_error", True),
            (requests.exceptions.ConnectionError("Connection refused"), "network_error", True),
            (_response(200, INDEX_WITHOUT_LINKS), "html_changed", False),
            (_response(404), "url_broken", False),
        ],
        ids=["503", "conexao", "sem-links", "404"],
    )
    def test_listing_failure_is_an_error_with_category(self, response, category, retryable):
        result, storage = self._run(response)

        assert result["agencies_processed"] == []
        (error,) = result["errors"]
        assert error["agency"] == "agencia_brasil"
        assert error["category"] == category
        assert error["retryable"] is retryable
        storage.insert.assert_not_called()
        run = storage.record_scrape_run.call_args[0][0]
        assert run.status == "error"
        assert str(run.error_category) == category
        assert run.agency_key == "agencia_brasil"
