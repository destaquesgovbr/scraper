"""Structured logging for scrape results."""

from __future__ import annotations

from datetime import UTC, datetime

from loguru import logger

from govbr_scraper.models.monitoring import ErrorCategory, ScrapeRunResult


def log_scrape_result(
    agency_key: str,
    status: str,
    articles_scraped: int = 0,
    articles_saved: int = 0,
    articles_new: int | None = None,
    error_category: ErrorCategory | None = None,
    error_message: str | None = None,
    execution_time_seconds: float | None = None,
    primary_scraper: str | None = None,
    fallback_triggered: bool = False,
    fallback_scraper: str | None = None,
    fallback_success: bool | None = None,
) -> ScrapeRunResult:
    """Create a structured scrape result, log it via loguru, and return it.

    Args:
        agency_key: The agency identifier.
        status: "success" or "error".
        articles_scraped: Number of articles found.
        articles_saved: Number of articles persisted (inserted + updated).
        articles_new: Number of articles actually inserted (None when not measured).
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
        articles_new=articles_new,
        execution_time_seconds=execution_time_seconds,
        scraped_at=datetime.now(UTC),
        primary_scraper=primary_scraper,
        fallback_triggered=fallback_triggered,
        fallback_scraper=fallback_scraper,
        fallback_success=fallback_success,
    )

    bound = logger.bind(
        agency_key=agency_key,
        status=status,
        articles_scraped=articles_scraped,
        articles_saved=articles_saved,
        articles_new=articles_new,
        error_category=str(error_category) if error_category else None,
        execution_time_seconds=execution_time_seconds,
        primary_scraper=primary_scraper,
        fallback_triggered=fallback_triggered,
        fallback_scraper=fallback_scraper,
        fallback_success=fallback_success,
    )

    if status == "error":
        bound.error("Scrape failed for {agency}: {error}", agency=agency_key, error=error_message)
    else:
        if fallback_triggered and fallback_success:
            bound.info(
                "Scrape completed for {agency} via {fallback} fallback: "
                "{scraped} scraped, {saved} saved, {new} new",
                agency=agency_key,
                fallback=fallback_scraper,
                scraped=articles_scraped,
                saved=articles_saved,
                new=articles_new,
            )
        else:
            bound.info(
                "Scrape completed for {agency}: {scraped} scraped, {saved} saved, {new} new",
                agency=agency_key,
                scraped=articles_scraped,
                saved=articles_saved,
                new=articles_new,
            )

    return result


def record_scrape_run_safe(storage, run: ScrapeRunResult, agency_key: str) -> None:
    """Record a scrape run, logging but not raising on failure."""
    try:
        storage.record_scrape_run(run)
    except Exception as err:
        logger.warning(
            "Failed to record scrape run for {agency}: {err}", agency=agency_key, err=err
        )


# =============================================================================
# Eventos estruturados (jsonPayload no Cloud Logging; métricas baseadas em log)
# =============================================================================

EVENT_SCRAPE_ATTEMPT = "scrape_attempt"
EVENT_CIRCUIT_TRANSITION = "circuit_transition"

# outcome do scrape_attempt
OUTCOME_SUCCESS = "success"
OUTCOME_FAILED = "failed"
OUTCOME_SKIPPED = "skipped"


def log_scrape_attempt(
    *,
    agency: str,
    outcome: str,
    category: str | None = None,
    retryable: bool | None = None,
    articles_new: int | None = None,
    duration_s: float | None = None,
    tier: str | None = None,
    explicit_window: bool = False,
    trigger: str = "dag",
    **extra,
) -> None:
    """Evento ``scrape_attempt``: uma vez por agência por tentativa, inclusive quando pulada.

    ``outcome`` é success/failed/skipped; ``category`` vira ``"none"`` quando não há (o label
    da métrica não pode ser nulo). ``trigger``: dag, task ou backfill. Campos extras (janela,
    cabeçalhos do Cloud Tasks) vão juntos no jsonPayload.
    """
    fields = {
        "event": EVENT_SCRAPE_ATTEMPT,
        "agency": agency,
        "outcome": outcome,
        "category": str(category) if category else "none",
        "retryable": retryable,
        "articles_new": articles_new,
        "duration_s": round(duration_s, 3) if duration_s is not None else None,
        "tier": tier,
        "explicit_window": explicit_window,
        "trigger": trigger,
        **extra,
    }
    message = (
        f"scrape_attempt agency={agency} outcome={outcome} category={fields['category']} "
        f"articles_new={articles_new} tier={tier}"
    )
    bound = logger.bind(**fields)
    if outcome == OUTCOME_FAILED:
        bound.warning(message)
    else:
        bound.info(message)


def log_circuit_transition(transition) -> None:
    """Evento ``circuit_transition`` (abertura ou fechamento do disjuntor de uma agência)."""

    def _value(v):
        return getattr(v, "value", v)

    from_state = _value(transition.from_state)
    to_state = _value(transition.to_state)
    fields = {
        "event": EVENT_CIRCUIT_TRANSITION,
        "agency": transition.agency_key,
        "from_state": from_state,
        "to_state": to_state,
        "reason": transition.reason,
        "category": transition.category or "none",
    }
    message = (
        f"circuit_transition agency={transition.agency_key} {from_state}->{to_state} "
        f"reason={transition.reason} category={fields['category']}"
    )
    bound = logger.bind(**fields)
    if to_state == "open":
        bound.warning(message)
    else:
        bound.info(message)
