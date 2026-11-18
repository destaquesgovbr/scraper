"""Tests for error classification — maps scraping errors to categories."""

import pytest

from govbr_scraper.models.monitoring import (
    PERMANENT_ERROR_CATEGORIES,
    UNKNOWN_AGENCY,
    ErrorCategory,
    agency_error,
    classify_error,
    classify_exception,
    is_retryable,
)
from govbr_scraper.scrapers.errors import LoginRedirectError, PermanentHTTPError, ScrapingError


class TestClassifyError:
    """classify_error() is a pure function: no IO, deterministic."""

    def test_classify_timeout_returns_network_error(self):
        assert classify_error("Connection timed out") == ErrorCategory.NETWORK_ERROR

    def test_classify_connection_refused_returns_network_error(self):
        assert (
            classify_error("Network error scraping mec: ConnectionError")
            == ErrorCategory.NETWORK_ERROR
        )

    def test_classify_request_exception_returns_network_error(self):
        assert (
            classify_error("Failed to fetch page after retries for mec: ReadTimeout")
            == ErrorCategory.NETWORK_ERROR
        )

    def test_classify_anti_bot_returns_anti_bot(self):
        msg = (
            "Anti-bot protection detected for mec. "
            "The site returned a JS challenge page instead of content. "
            "URL: https://www.gov.br/mec/pt-br/assuntos/noticias"
        )
        assert classify_error(msg) == ErrorCategory.ANTI_BOT

    def test_classify_403_returns_access_denied(self):
        assert classify_error("HTTP 403 Forbidden", http_status=403) == ErrorCategory.ACCESS_DENIED

    def test_classify_401_returns_access_denied(self):
        assert classify_error("HTTP 401", http_status=401) == ErrorCategory.ACCESS_DENIED

    def test_classify_404_returns_url_broken(self):
        assert classify_error("Not Found", http_status=404) == ErrorCategory.URL_BROKEN

    def test_classify_410_returns_url_broken(self):
        assert classify_error("Gone", http_status=410) == ErrorCategory.URL_BROKEN

    def test_classify_login_redirect_message_returns_access_denied(self):
        msg = (
            "Login redirect for sri: https://www.gov.br/sri/pt-br/noticias?b_start:int=0 -> "
            "https://www.gov.br/sri/pt-br/require_login?came_from=..."
        )
        assert classify_error(msg) == ErrorCategory.ACCESS_DENIED

    def test_classify_no_articles_large_page_returns_html_changed(self):
        msg = (
            "No articles found on first page of mec but response "
            "was 15432 bytes. This may indicate anti-bot blocking "
            "or a changed page structure. URL: https://www.gov.br/mec"
        )
        assert classify_error(msg) == ErrorCategory.HTML_CHANGED

    def test_classify_no_news_returns_empty_content(self):
        assert classify_error("No news found for mec.") == ErrorCategory.EMPTY_CONTENT

    def test_classify_unknown_message_returns_unknown(self):
        assert classify_error("Something completely unexpected happened") == ErrorCategory.UNKNOWN

    def test_classify_is_case_insensitive(self):
        assert classify_error("ANTI-BOT PROTECTION DETECTED") == ErrorCategory.ANTI_BOT
        assert classify_error("connection timed out") == ErrorCategory.NETWORK_ERROR

    def test_classify_empty_message_returns_unknown(self):
        assert classify_error("") == ErrorCategory.UNKNOWN

    def test_http_status_takes_priority_over_message(self):
        """HTTP status 404 should classify as URL_BROKEN even if message says something else."""
        assert classify_error("Some random error", http_status=404) == ErrorCategory.URL_BROKEN

    @pytest.mark.parametrize("status", [408, 429])
    def test_transient_4xx_returns_network_error(self, status):
        """408 e 429 são os 4xx que http_policy repete: passageiros, não URL quebrada."""
        assert classify_error(f"HTTP {status}", http_status=status) == ErrorCategory.NETWORK_ERROR

    @pytest.mark.parametrize("status", [400, 405, 406, 414, 422, 451])
    def test_other_permanent_4xx_returns_url_broken(self, status):
        """Antes caíam em UNKNOWN (passageiro) e gastavam os retries da DAG."""
        msg = f"HTTP {status} (permanent, not retried): https://www.gov.br/x"
        assert classify_error(msg, http_status=status) == ErrorCategory.URL_BROKEN

    def test_5xx_status_falls_back_to_message(self):
        assert classify_error("Server exploded", http_status=503) == ErrorCategory.UNKNOWN
        assert classify_error("Read timed out", http_status=503) == ErrorCategory.NETWORK_ERROR

    @pytest.mark.parametrize(
        "msg",
        [
            "Erro de rede ao acessar API de mec: 503 Server Error: Service Unavailable for url: x",
            "Erro de rede ao acessar API de mec: 429 Client Error: Too Many Requests for url: x",
            "Erro de rede ao acessar API de mec: HTTPSConnectionPool(host='www.gov.br'): "
            "Max retries exceeded (NameResolutionError)",
            "ERRO DE REDE ao acessar API de mec: qualquer coisa",
        ],
    )
    def test_plone6_network_wrapper_returns_network_error(self, msg):
        """O Plone 6 embrulha a RequestException em português; antes virava UNKNOWN."""
        assert classify_error(msg) == ErrorCategory.NETWORK_ERROR

    def test_both_scrapers_failed_keeps_html_changed(self):
        """O primário falhou por HTML: o fallback com erro de rede não muda a categoria."""
        msg = (
            "Both scrapers failed. Primary: No articles found on first page of mec but "
            "response was 9000 bytes. Fallback: Erro de rede ao acessar API de mec: 503"
        )
        assert classify_error(msg) == ErrorCategory.HTML_CHANGED


class TestClassifyException:
    """classify_exception() usa o http_status do PermanentHTTPError quando houver."""

    def test_permanent_404_returns_url_broken(self):
        exc = PermanentHTTPError("HTTP 404 (permanent, not retried): https://x", http_status=404)
        assert classify_exception(exc) == ErrorCategory.URL_BROKEN

    def test_permanent_401_returns_access_denied(self):
        exc = PermanentHTTPError("HTTP 401 (permanent, not retried): https://x", http_status=401)
        assert classify_exception(exc) == ErrorCategory.ACCESS_DENIED

    def test_login_redirect_returns_access_denied(self):
        exc = LoginRedirectError("Login redirect for sri: https://a -> https://a/require_login")
        assert classify_exception(exc) == ErrorCategory.ACCESS_DENIED

    def test_exception_without_status_falls_back_to_message(self):
        exc = ScrapingError("No articles found on first page of mec but response was 9000 bytes")
        assert classify_exception(exc) == ErrorCategory.HTML_CHANGED

    def test_permanent_400_returns_url_broken(self):
        exc = PermanentHTTPError("HTTP 400 (permanent, not retried): https://x", http_status=400)
        assert classify_exception(exc) == ErrorCategory.URL_BROKEN

    def test_plone6_network_error_exception_returns_network_error(self):
        exc = ScrapingError("Erro de rede ao acessar API de mec: 502 Server Error")
        assert classify_exception(exc) == ErrorCategory.NETWORK_ERROR


class TestIsRetryable:
    """Permanente = repetir na mesma execução não resolve; a DAG falha sem retry."""

    def test_permanent_set(self):
        assert PERMANENT_ERROR_CATEGORIES == {
            ErrorCategory.URL_BROKEN,
            ErrorCategory.ACCESS_DENIED,
            ErrorCategory.HTML_CHANGED,
            ErrorCategory.ANTI_BOT,
        }

    @pytest.mark.parametrize(
        "category",
        [
            ErrorCategory.URL_BROKEN,
            ErrorCategory.ACCESS_DENIED,
            ErrorCategory.HTML_CHANGED,
            ErrorCategory.ANTI_BOT,
            "url_broken",
            "html_changed",
            UNKNOWN_AGENCY,
        ],
    )
    def test_permanent_is_not_retryable(self, category):
        assert is_retryable(category) is False

    @pytest.mark.parametrize(
        "category",
        [
            ErrorCategory.NETWORK_ERROR,
            ErrorCategory.UNKNOWN,
            ErrorCategory.EMPTY_CONTENT,
            "network_error",
            "unknown",
            None,
        ],
    )
    def test_transient_is_retryable(self, category):
        assert is_retryable(category) is True


class TestAgencyError:
    def test_permanent_error_entry(self):
        assert agency_error("mec", "HTTP 404", ErrorCategory.URL_BROKEN) == {
            "agency": "mec",
            "error": "HTTP 404",
            "category": "url_broken",
            "retryable": False,
        }

    def test_transient_error_entry(self):
        entry = agency_error("mec", "Read timed out", ErrorCategory.NETWORK_ERROR)
        assert entry["category"] == "network_error"
        assert entry["retryable"] is True

    def test_category_is_plain_string(self):
        """Vai para o JSON da API: string, não o enum."""
        entry = agency_error("mec", "x", ErrorCategory.ANTI_BOT)
        assert type(entry["category"]) is str

    def test_without_category_is_retryable(self):
        assert agency_error("config", "x", None) == {
            "agency": "config",
            "error": "x",
            "category": None,
            "retryable": True,
        }
