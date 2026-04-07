"""Monitoring models and error classification for scraper observability."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel


class ErrorCategory(StrEnum):
    """Categories of scraping errors for structured classification."""

    NETWORK_ERROR = "network_error"
    ANTI_BOT = "anti_bot"
    EMPTY_CONTENT = "empty_content"
    URL_BROKEN = "url_broken"
    HTML_CHANGED = "html_changed"
    UNKNOWN = "unknown"


# HTTP status codes indicating broken URLs
_BROKEN_URL_STATUSES = {403, 404, 410}

# Pattern pairs: (substring to match in lowercase, corresponding category)
# Order matters — more specific patterns first.
_ERROR_PATTERNS: list[tuple[str, ErrorCategory]] = [
    # "No articles found...bytes" = HTML structure changed (must come before anti-bot)
    ("no articles found", ErrorCategory.HTML_CHANGED),
    ("no news found", ErrorCategory.EMPTY_CONTENT),
    ("anti-bot", ErrorCategory.ANTI_BOT),
    ("js challenge", ErrorCategory.ANTI_BOT),
    ("cloudflare", ErrorCategory.ANTI_BOT),
    ("timed out", ErrorCategory.NETWORK_ERROR),
    ("timeout", ErrorCategory.NETWORK_ERROR),
    ("connectionerror", ErrorCategory.NETWORK_ERROR),
    ("connection error", ErrorCategory.NETWORK_ERROR),
    ("connection refused", ErrorCategory.NETWORK_ERROR),
    ("network error", ErrorCategory.NETWORK_ERROR),
    ("failed to fetch page", ErrorCategory.NETWORK_ERROR),
]


def classify_error(
    error_message: str, http_status: int | None = None
) -> ErrorCategory:
    """Classify a scraping error by its message and optional HTTP status code.

    Pure function — no IO, deterministic. HTTP status takes priority over message.

    Args:
        error_message: The error message string from the exception.
        http_status: Optional HTTP status code from the response.

    Returns:
        The ErrorCategory that best matches the error.
    """
    if http_status is not None and http_status in _BROKEN_URL_STATUSES:
        return ErrorCategory.URL_BROKEN

    msg_lower = error_message.lower()
    for pattern, category in _ERROR_PATTERNS:
        if pattern in msg_lower:
            return category

    return ErrorCategory.UNKNOWN


class ScrapeRunResult(BaseModel):
    """Structured result of a single agency scrape execution."""

    agency_key: str
    status: str  # "success", "error"
    error_category: Optional[ErrorCategory] = None
    error_message: Optional[str] = None
    articles_scraped: int = 0
    articles_saved: int = 0
    execution_time_seconds: Optional[float] = None
    scraped_at: datetime
