"""Monitoring models and error classification for scraper observability."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class ErrorCategory(StrEnum):
    """Categories of scraping errors for structured classification."""

    NETWORK_ERROR = "network_error"
    ANTI_BOT = "anti_bot"
    EMPTY_CONTENT = "empty_content"
    URL_BROKEN = "url_broken"
    ACCESS_DENIED = "access_denied"
    HTML_CHANGED = "html_changed"
    UNKNOWN = "unknown"


# HTTP status codes indicating broken URLs (a página não existe mais)
_BROKEN_URL_STATUSES = {404, 410}
# HTTP status codes indicating the page exists but requires authentication/authorization
_ACCESS_DENIED_STATUSES = {401, 403}

# Pattern pairs: (substring to match in lowercase, corresponding category)
# Order matters — more specific patterns first.
_ERROR_PATTERNS: list[tuple[str, ErrorCategory]] = [
    # Listagem redirecionada para o login do Plone (http_policy.login_redirect_error)
    ("login redirect", ErrorCategory.ACCESS_DENIED),
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


def classify_error(error_message: str, http_status: int | None = None) -> ErrorCategory:
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
    if http_status is not None and http_status in _ACCESS_DENIED_STATUSES:
        return ErrorCategory.ACCESS_DENIED

    msg_lower = error_message.lower()
    for pattern, category in _ERROR_PATTERNS:
        if pattern in msg_lower:
            return category

    return ErrorCategory.UNKNOWN


def classify_exception(exc: BaseException) -> ErrorCategory:
    """Classify an exception, using its ``http_status`` attribute when present.

    ``PermanentHTTPError`` (scrapers/errors.py) carries the status of the 4xx response.
    """
    http_status = getattr(exc, "http_status", None)
    return classify_error(str(exc), http_status if isinstance(http_status, int) else None)


class ScrapeRunResult(BaseModel):
    """Structured result of a single agency scrape execution."""

    agency_key: str
    status: str  # "success", "error"
    error_category: ErrorCategory | None = None
    error_message: str | None = None
    articles_scraped: int = 0
    articles_saved: int = 0
    execution_time_seconds: float | None = None
    scraped_at: datetime
    primary_scraper: str | None = None
    fallback_triggered: bool = False
    fallback_scraper: str | None = None
    fallback_success: bool | None = None
