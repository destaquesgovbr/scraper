"""Notification helpers for scraper monitoring DAGs.

Sends alerts via Telegram Bot API. Falls back to logging if not configured.
"""

import logging

import httpx

logger = logging.getLogger(__name__)


def send_telegram_alert(token: str, chat_id: str, message: str) -> bool:
    """Send an alert via the Telegram Bot API.

    Args:
        token: Telegram bot token.
        chat_id: Target chat/group ID.
        message: HTML-formatted message text.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10.0,
        )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")
        return False


def send_alert(
    message: str,
    telegram_token: str | None = None,
    telegram_chat_id: str | None = None,
    webhook_url: str | None = None,
) -> bool:
    """Send an alert via the best available channel.

    Priority: Telegram > webhook > log-only.

    Args:
        message: Alert text (HTML for Telegram, plain for webhook).
        telegram_token: Telegram bot token (None = skip Telegram).
        telegram_chat_id: Target chat ID (None = skip Telegram).
        webhook_url: Fallback webhook URL for POST (None = skip).

    Returns:
        True if alert was delivered via at least one channel.
    """
    if telegram_token and telegram_chat_id:
        if send_telegram_alert(telegram_token, telegram_chat_id, message):
            return True
        logger.warning("Telegram alert failed, trying fallback...")

    if webhook_url:
        try:
            resp = httpx.post(webhook_url, json={"text": message}, timeout=10.0)
            if resp.status_code < 300:
                return True
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")

    # Fallback: log the alert
    logger.info(f"Alert (log-only): {message}")
    return False
