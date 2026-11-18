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


# Categorias de erro permanente: repetir a raspagem na mesma execução não resolve (URL
# quebrada, login, estrutura do HTML mudou, anti-bot). As outras (network_error, unknown,
# empty_content) e a ausência de categoria são passageiras. A API devolve a decisão em
# errors[].retryable e as DAGs falham sem retry (AirflowFailException) quando todos os
# erros são permanentes.
PERMANENT_ERROR_CATEGORIES: frozenset[ErrorCategory] = frozenset(
    {
        ErrorCategory.URL_BROKEN,
        ErrorCategory.ACCESS_DENIED,
        ErrorCategory.HTML_CHANGED,
        ErrorCategory.ANTI_BOT,
    }
)

# Categoria só da resposta da API (não vai para scrape_runs): agência fora do
# site_urls.yaml/ebc_urls.yaml ou inativa. Também permanente.
UNKNOWN_AGENCY = "unknown_agency"

# HTTP status codes indicating broken URLs (a página não existe mais)
_BROKEN_URL_STATUSES = {404, 410}
# HTTP status codes indicating the page exists but requires authentication/authorization
_ACCESS_DENIED_STATUSES = {401, 403}
# 4xx passageiros (Request Timeout e Too Many Requests): os mesmos que http_policy repete
_TRANSIENT_HTTP_STATUSES = {408, 429}

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
    # Plone 6: "Erro de rede ao acessar API de <agência>: ..." embrulha a RequestException
    # (5xx, 408 e 429 depois dos retries, DNS, conexão).
    ("erro de rede", ErrorCategory.NETWORK_ERROR),
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
    if http_status is not None:
        if http_status in _BROKEN_URL_STATUSES:
            return ErrorCategory.URL_BROKEN
        if http_status in _ACCESS_DENIED_STATUSES:
            return ErrorCategory.ACCESS_DENIED
        if http_status in _TRANSIENT_HTTP_STATUSES:
            return ErrorCategory.NETWORK_ERROR
        if 400 <= http_status < 500:
            # Outro 4xx permanente (400, 405, 406, 414...): a URL configurada não serve.
            return ErrorCategory.URL_BROKEN

    msg_lower = error_message.lower()
    for pattern, category in _ERROR_PATTERNS:
        if pattern in msg_lower:
            return category

    return ErrorCategory.UNKNOWN


def classify_exception(exc: BaseException) -> ErrorCategory:
    """Classify an exception, using its ``error_category`` or ``http_status`` attribute.

    ``FallbackFailedError`` (scrapers/errors.py) carries the category already decided by the
    ScrapeManager (the message of both scrapers would always read as html_changed);
    ``PermanentHTTPError`` carries the status of the 4xx response.
    """
    category = getattr(exc, "error_category", None)
    if isinstance(category, ErrorCategory):
        return category
    http_status = getattr(exc, "http_status", None)
    return classify_error(str(exc), http_status if isinstance(http_status, int) else None)


def is_retryable(category: str | None) -> bool:
    """True se vale repetir a raspagem: erro passageiro ou sem categoria.

    Permanentes: ``PERMANENT_ERROR_CATEGORIES`` e ``UNKNOWN_AGENCY``. Aceita o enum ou a
    string gravada em ``scrape_runs.error_category``.
    """
    if category is None:
        return True
    return category not in PERMANENT_ERROR_CATEGORIES and category != UNKNOWN_AGENCY


def agency_error(agency: str, message: str, category: str | None) -> dict:
    """Erro de uma agência no formato de ``errors[]`` da API (``AgencyError``).

    A categoria é a mesma gravada em ``scrape_runs`` e ``retryable`` sai dela.
    """
    return {
        "agency": agency,
        "error": message,
        "category": str(category) if category is not None else None,
        "retryable": is_retryable(category),
    }


class ScrapeRunResult(BaseModel):
    """Structured result of a single agency scrape execution."""

    agency_key: str
    status: str  # "success", "error"
    error_category: ErrorCategory | None = None
    error_message: str | None = None
    articles_scraped: int = 0
    articles_saved: int = 0
    # Linhas de fato inseridas em news (sem as atualizadas). None quando não foi medido:
    # modo não sequencial e execução com erro.
    articles_new: int | None = None
    execution_time_seconds: float | None = None
    scraped_at: datetime
    primary_scraper: str | None = None
    fallback_triggered: bool = False
    fallback_scraper: str | None = None
    fallback_success: bool | None = None
