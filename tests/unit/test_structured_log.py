"""Tests for structured logging of scrape results."""

from unittest.mock import patch

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult
from govbr_scraper.monitoring.structured_log import log_scrape_result


class TestLogScrapeResult:

    def test_returns_scrape_run_result_with_all_fields(self):
        result = log_scrape_result(
            agency_key="mec",
            status="success",
            articles_scraped=10,
            articles_saved=8,
            execution_time_seconds=2.5,
        )
        assert isinstance(result, ScrapeRunResult)
        assert result.agency_key == "mec"
        assert result.status == "success"
        assert result.articles_scraped == 10
        assert result.articles_saved == 8
        assert result.execution_time_seconds == 2.5
        assert result.scraped_at is not None

    def test_success_has_no_error_fields(self):
        result = log_scrape_result(agency_key="mec", status="success")
        assert result.error_category is None
        assert result.error_message is None

    def test_error_includes_category_and_message(self):
        result = log_scrape_result(
            agency_key="mec",
            status="error",
            error_category=ErrorCategory.ANTI_BOT,
            error_message="Anti-bot protection detected",
        )
        assert result.error_category == ErrorCategory.ANTI_BOT
        assert result.error_message == "Anti-bot protection detected"

    @patch("govbr_scraper.monitoring.structured_log.logger")
    def test_calls_loguru_with_bound_context(self, mock_logger):
        mock_bound = mock_logger.bind.return_value
        log_scrape_result(agency_key="mec", status="success", articles_scraped=5)
        mock_logger.bind.assert_called_once()
        bound_kwargs = mock_logger.bind.call_args[1]
        assert bound_kwargs["agency_key"] == "mec"
        assert bound_kwargs["status"] == "success"
        assert bound_kwargs["articles_scraped"] == 5
        mock_bound.info.assert_called_once()

    @patch("govbr_scraper.monitoring.structured_log.logger")
    def test_error_status_logs_as_error_level(self, mock_logger):
        mock_bound = mock_logger.bind.return_value
        log_scrape_result(
            agency_key="mec",
            status="error",
            error_category=ErrorCategory.NETWORK_ERROR,
            error_message="Connection timed out",
        )
        mock_bound.error.assert_called_once()
