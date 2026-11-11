"""Política HTTP dos scrapers (infra#223): falhar rápido em 4xx, repetir só falha transitória.

Antes, qualquer RequestException era repetida 5 vezes (~116 s de espera), inclusive 404 e 401,
e uma listagem que redirecionava para o login acionava o fallback Plone 6, que falhava de novo.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from govbr_scraper.models.monitoring import ErrorCategory
from govbr_scraper.scrapers.ebc_webscraper import EBCWebScraper
from govbr_scraper.scrapers.errors import LoginRedirectError, PermanentHTTPError, ScrapingError
from govbr_scraper.scrapers.http_policy import (
    REQUEST_TIMEOUT,
    RETRY_TRIES,
    is_permanent_status,
    raise_for_http_status,
)
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.scrape_manager import ScrapeManager
from govbr_scraper.scrapers.webscraper import WebScraper

LISTING_URL = "https://www.gov.br/sri/pt-br/noticias"
LOGIN_URL = "https://www.gov.br/sri/pt-br/require_login?came_from=https%3A//www.gov.br/sri"


def _response(status: int, url: str = LISTING_URL, content: bytes = b"") -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.url = url
    response._content = content
    return response


@pytest.fixture
def sleep():
    """Neutraliza as esperas do @retry (e as do scraper) e conta as chamadas."""
    with patch("retry.api.time.sleep") as mock_sleep:
        yield mock_sleep


@pytest.fixture
def web_scraper():
    return WebScraper("2026-11-10", LISTING_URL, max_date="2026-11-11")


@pytest.fixture
def plone_scraper():
    return Plone6APIScraper("2026-11-10", LISTING_URL, max_date="2026-11-11")


class TestIsPermanentStatus:
    @pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 451])
    def test_4xx_is_permanent(self, status):
        assert is_permanent_status(status)

    @pytest.mark.parametrize("status", [200, 301, 408, 429, 500, 502, 503, 504])
    def test_others_are_not_permanent(self, status):
        assert not is_permanent_status(status)

    def test_raise_for_http_status_carries_status_and_url(self):
        with pytest.raises(PermanentHTTPError) as exc_info:
            raise_for_http_status(_response(404))
        assert exc_info.value.http_status == 404
        assert exc_info.value.url == LISTING_URL

    def test_permanent_error_is_scraping_error_not_request_exception(self):
        assert issubclass(PermanentHTTPError, ScrapingError)
        assert not issubclass(PermanentHTTPError, requests.exceptions.RequestException)


class TestWebScraperFetchPage:
    @pytest.mark.parametrize("status", [404, 401, 403, 410])
    def test_permanent_4xx_is_not_retried(self, web_scraper, sleep, status):
        with patch("requests.get", return_value=_response(status)) as mock_get:
            with pytest.raises(PermanentHTTPError) as exc_info:
                web_scraper.fetch_page(LISTING_URL)

        assert mock_get.call_count == 1
        assert exc_info.value.http_status == status
        sleep.assert_not_called()

    @pytest.mark.parametrize("status", [503, 500, 429, 408])
    def test_transient_status_is_retried(self, web_scraper, sleep, status):
        with patch("requests.get", return_value=_response(status)) as mock_get:
            with pytest.raises(requests.exceptions.HTTPError):
                web_scraper.fetch_page(LISTING_URL)

        assert mock_get.call_count == RETRY_TRIES == 3
        assert sleep.call_count == RETRY_TRIES - 1

    def test_503_then_200_returns_response(self, web_scraper, sleep):
        ok = _response(200, content=b"<html></html>")
        with patch("requests.get", side_effect=[_response(503), ok]) as mock_get:
            assert web_scraper.fetch_page(LISTING_URL) is ok
        assert mock_get.call_count == 2

    def test_timeout_is_retried(self, web_scraper, sleep):
        with patch(
            "requests.get", side_effect=requests.exceptions.ReadTimeout("read timeout")
        ) as m:
            with pytest.raises(requests.exceptions.Timeout):
                web_scraper.fetch_page(LISTING_URL)
        assert m.call_count == RETRY_TRIES

    def test_connection_error_is_retried(self, web_scraper, sleep):
        ok = _response(200)
        with patch(
            "requests.get", side_effect=[requests.exceptions.ConnectionError("reset"), ok]
        ) as mock_get:
            assert web_scraper.fetch_page(LISTING_URL) is ok
        assert mock_get.call_count == 2

    def test_uses_connect_and_read_timeout(self, web_scraper, sleep):
        with patch("requests.get", return_value=_response(200)) as mock_get:
            web_scraper.fetch_page(LISTING_URL)
        assert mock_get.call_args.kwargs["timeout"] == REQUEST_TIMEOUT == (5, 15)

    def test_retry_waits_are_short(self, web_scraper, sleep):
        """Esperas do retry: 1 s e depois 2-3 s (antes: 2, 7-9, 22-30, ~80 s)."""
        with patch("requests.get", return_value=_response(503)):
            with pytest.raises(requests.exceptions.HTTPError):
                web_scraper.fetch_page(LISTING_URL)
        waits = [c.args[0] for c in sleep.call_args_list]
        assert waits[0] == 1
        assert 2 <= waits[1] <= 3
        assert sum(waits) <= 4


class TestWebScraperScrapeNews:
    def test_404_listing_fails_fast_with_status(self, web_scraper, sleep):
        with patch("requests.get", return_value=_response(404)) as mock_get:
            with pytest.raises(PermanentHTTPError) as exc_info:
                web_scraper.scrape_news()
        assert mock_get.call_count == 1
        assert exc_info.value.http_status == 404

    def test_login_redirect_raises_permanent_error(self, web_scraper, sleep):
        login_page = _response(200, url=LOGIN_URL, content=b"x" * 20000)
        with patch("requests.get", return_value=login_page) as mock_get:
            with pytest.raises(LoginRedirectError) as exc_info:
                web_scraper.scrape_news()
        assert mock_get.call_count == 1
        assert "login redirect" in str(exc_info.value).lower()

    def test_login_redirect_on_later_page_stops_without_error(self, web_scraper, sleep):
        """Página 2+ redirecionando para login só encerra a paginação (como antes: 0 itens)."""
        login_page = _response(200, url=LOGIN_URL, content=b"x" * 20000)
        with patch("requests.get", return_value=login_page):
            assert web_scraper.scrape_page(f"{LISTING_URL}?b_start:int=30") == (False, 0)

    def test_503_exhausted_still_wrapped_as_network_scraping_error(self, web_scraper, sleep):
        with patch("requests.get", return_value=_response(503)):
            with pytest.raises(ScrapingError, match="Failed to fetch page after retries"):
                web_scraper.scrape_news()


class TestPlone6FetchApiPage:
    @pytest.mark.parametrize("status", [404, 401])
    def test_permanent_4xx_is_not_retried(self, plone_scraper, sleep, status):
        with patch("requests.get", return_value=_response(status)) as mock_get:
            with pytest.raises(PermanentHTTPError) as exc_info:
                plone_scraper._fetch_api_page("https://www.gov.br/sri/++api++/pt-br/@search")
        assert mock_get.call_count == 1
        assert exc_info.value.http_status == status
        sleep.assert_not_called()

    def test_503_is_retried(self, plone_scraper, sleep):
        ok = _response(200, content=b'{"items": [], "items_total": 0}')
        with patch("requests.get", side_effect=[_response(503), _response(503), ok]) as m:
            assert plone_scraper._fetch_api_page("https://x/@search") == {
                "items": [],
                "items_total": 0,
            }
        assert m.call_count == 3

    def test_login_redirect_raises_permanent_error(self, plone_scraper, sleep):
        with patch("requests.get", return_value=_response(200, url=LOGIN_URL, content=b"<html>")):
            with pytest.raises(LoginRedirectError):
                plone_scraper._fetch_api_page("https://x/@search")

    def test_scrape_news_propagates_permanent_error(self, plone_scraper, sleep):
        with patch("requests.get", return_value=_response(401)):
            with pytest.raises(PermanentHTTPError) as exc_info:
                plone_scraper.scrape_news()
        assert exc_info.value.http_status == 401


class TestEBCFetchPage:
    @pytest.fixture
    def ebc(self):
        return EBCWebScraper("2026-11-10", "https://agenciabrasil.ebc.com.br/ultimas")

    def test_404_returns_none_without_retry(self, ebc, sleep):
        with patch("requests.get", return_value=_response(404)) as mock_get:
            assert ebc.fetch_page("https://agenciabrasil.ebc.com.br/x") is None
        assert mock_get.call_count == 1

    def test_503_is_retried_then_returns_none(self, ebc, sleep):
        """Antes o try/except dentro do @retry devolvia None e o retry nunca acontecia."""
        with patch("requests.get", return_value=_response(503)) as mock_get:
            assert ebc.fetch_page("https://agenciabrasil.ebc.com.br/x") is None
        assert mock_get.call_count == RETRY_TRIES


def _manager():
    storage = MagicMock()
    storage.get_recent_urls.return_value = set()
    storage.insert.return_value = 0
    return ScrapeManager(storage)


class TestScrapeManagerPermanentErrors:
    """Erro permanente não aciona o fallback Plone 6 e é gravado com a categoria certa."""

    @pytest.fixture
    def run(self, sleep):
        def _run(agency_config):
            with (
                patch(
                    "govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml",
                    return_value={"sri": agency_config},
                ),
                patch("govbr_scraper.scrapers.scrape_manager.log_scrape_result") as mock_log,
                patch.object(Plone6APIScraper, "scrape_news") as mock_p6,
            ):
                mock_log.return_value = MagicMock()
                result = _manager().run_scraper(
                    agencies=["sri"], min_date="2026-11-10", max_date="2026-11-11", sequential=True
                )
            return result, mock_log, mock_p6

        return _run

    def test_login_redirect_does_not_trigger_plone6_fallback(self, run):
        login_page = _response(200, url=LOGIN_URL, content=b"x" * 20000)
        with patch("requests.get", return_value=login_page) as mock_get:
            result, mock_log, mock_p6 = run(
                {"url": LISTING_URL, "scraper_type": "html", "active": True}
            )

        mock_p6.assert_not_called()
        assert mock_get.call_count == 1
        kwargs = mock_log.call_args.kwargs
        assert kwargs["status"] == "error"
        assert kwargs["error_category"] == ErrorCategory.ACCESS_DENIED
        assert len(result["errors"]) == 1

    def test_404_is_logged_as_url_broken_without_fallback(self, run):
        with patch("requests.get", return_value=_response(404)) as mock_get:
            _result, mock_log, mock_p6 = run(
                {"url": LISTING_URL, "scraper_type": "html", "active": True}
            )

        mock_p6.assert_not_called()
        assert mock_get.call_count == 1
        assert mock_log.call_args.kwargs["error_category"] == ErrorCategory.URL_BROKEN

    def test_plone6_401_is_logged_as_access_denied(self, sleep):
        with (
            patch(
                "govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml",
                return_value={
                    "museudoindio": {
                        "url": "https://www.gov.br/museudoindio/pt-br/assuntos/noticias",
                        "scraper_type": "plone6_api",
                        "active": True,
                    }
                },
            ),
            patch("govbr_scraper.scrapers.scrape_manager.log_scrape_result") as mock_log,
            patch("requests.get", return_value=_response(401)) as mock_get,
        ):
            mock_log.return_value = MagicMock()
            _manager().run_scraper(
                agencies=["museudoindio"],
                min_date="2026-11-10",
                max_date="2026-11-11",
                sequential=True,
            )

        assert mock_get.call_count == 1
        assert mock_log.call_args.kwargs["error_category"] == ErrorCategory.ACCESS_DENIED

    def test_html_changed_still_triggers_fallback(self, run):
        """Regressão: "no articles" (HTML_CHANGED) continua acionando o Plone 6."""
        page = _response(200, content=b"<html><body>" + b"x" * 20000 + b"</body></html>")
        with patch("requests.get", return_value=page):
            _result, _mock_log, mock_p6 = run(
                {"url": LISTING_URL, "scraper_type": "html", "active": True}
            )
        mock_p6.assert_called_once()
