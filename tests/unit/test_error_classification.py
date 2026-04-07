"""Tests for error classification — maps scraping errors to categories."""

import pytest

from govbr_scraper.models.monitoring import ErrorCategory, classify_error


class TestClassifyError:
    """classify_error() is a pure function: no IO, deterministic."""

    def test_classify_timeout_returns_network_error(self):
        assert classify_error("Connection timed out") == ErrorCategory.NETWORK_ERROR

    def test_classify_connection_refused_returns_network_error(self):
        assert classify_error("Network error scraping mec: ConnectionError") == ErrorCategory.NETWORK_ERROR

    def test_classify_request_exception_returns_network_error(self):
        assert classify_error("Failed to fetch page after retries for mec: ReadTimeout") == ErrorCategory.NETWORK_ERROR

    def test_classify_anti_bot_returns_anti_bot(self):
        msg = (
            "Anti-bot protection detected for mec. "
            "The site returned a JS challenge page instead of content. "
            "URL: https://www.gov.br/mec/pt-br/assuntos/noticias"
        )
        assert classify_error(msg) == ErrorCategory.ANTI_BOT

    def test_classify_403_returns_url_broken(self):
        assert classify_error("HTTP 403 Forbidden", http_status=403) == ErrorCategory.URL_BROKEN

    def test_classify_404_returns_url_broken(self):
        assert classify_error("Not Found", http_status=404) == ErrorCategory.URL_BROKEN

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
