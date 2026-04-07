"""Structured logging for scrape results."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult


def log_scrape_result(
    agency_key: str,
    status: str,
    articles_scraped: int = 0,
    articles_saved: int = 0,
    error_category: Optional[ErrorCategory] = None,
    error_message: Optional[str] = None,
    execution_time_seconds: Optional[float] = None,
) -> ScrapeRunResult:
    """Create a structured scrape result, log it via loguru, and return it.

    Args:
        agency_key: The agency identifier.
        status: "success" or "error".
        articles_scraped: Number of articles found.
        articles_saved: Number of articles persisted.
        error_category: Classified error category (if error).
        error_message: Error description (if error).
        execution_time_seconds: How long the scrape took.

    Returns:
        ScrapeRunResult for persistence.
    """
    result = ScrapeRunResult(
        agency_key=agency_key,
        status=status,
        error_category=error_category,
        error_message=error_message,
        articles_scraped=articles_scraped,
        articles_saved=articles_saved,
        execution_time_seconds=execution_time_seconds,
        scraped_at=datetime.now(timezone.utc),
    )

    bound = logger.bind(
        agency_key=agency_key,
        status=status,
        articles_scraped=articles_scraped,
        articles_saved=articles_saved,
        error_category=str(error_category) if error_category else None,
        execution_time_seconds=execution_time_seconds,
    )

    if status == "error":
        bound.error("Scrape failed for {agency}: {error}", agency=agency_key, error=error_message)
    else:
        bound.info(
            "Scrape completed for {agency}: {scraped} scraped, {saved} saved",
            agency=agency_key,
            scraped=articles_scraped,
            saved=articles_saved,
        )

    return result
