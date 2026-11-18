"""Notification helpers for scraper monitoring DAGs.

Canais, em ordem de prioridade (``send_alert``):

1. Telegram, com o bot próprio do scraper (Variables ``scraper_telegram_*``);
2. Pub/Sub, no tópico ``dgb.ops.alerts``: o alert-relay (data-platform,
   ``workers/alert_relay``) repassa a mensagem para o Telegram da equipe;
3. webhook;
4. só log (``Alert (log-only): ...``).
"""

import base64
import json
import logging
import os
import re

import httpx

logger = logging.getLogger(__name__)

ALERTS_TOPIC_NAME = "dgb.ops.alerts"
PUBSUB_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]
PUBSUB_TIMEOUT_SECONDS = 10.0
ALERT_SEVERITIES = ("info", "warning", "critical")

# O alert-relay escapa o HTML de "text" e o corta em 3500 caracteres (formatter.py,
# _FIELD_LIMITS); 3400 deixa folga e o corte fica em fim de linha, com o marcador abaixo.
PUBSUB_MAX_TEXT_CHARS = 3400
_TRUNCATED_MARKER = "... (alerta truncado; lista completa no log da task)"

# Nome completo do tópico; também impede que a Variable injete outro caminho na URL da API.
_TOPIC_RE = re.compile(
    r"projects/[a-z][a-z0-9-]{4,28}[a-z0-9]/topics/[A-Za-z][A-Za-z0-9_.~%+-]{2,254}"
)
_BOLD_TAG_RE = re.compile(r"</?b>")


def send_telegram_alert(token: str, chat_id: str, message: str) -> bool:
    """Send an alert via the Telegram Bot API.

    Args:
        token: Telegram bot token.
        chat_id: Target chat/group ID.
        message: HTML-formatted message text.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    if not token or ":" not in token:
        logger.error("Invalid Telegram bot token format (token not logged for security)")
        return False
    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=5.0,
        )
        if response.status_code == 401:
            logger.error("Telegram bot token is invalid (HTTP 401)")
            return False
        return response.status_code == 200
    except Exception as e:
        # Log only exception type to avoid leaking the token via URL in the traceback
        logger.error(f"Failed to send Telegram alert: {type(e).__name__}")
        return False


def default_alert_topic() -> str | None:
    """Tópico ``dgb.ops.alerts`` do projeto em ``GCP_PROJECT_ID``, ou None sem a env.

    ``GCP_PROJECT_ID`` é definida no Composer (e no Airflow próprio que vai substituí-lo).
    As DAGs usam este valor como default da Variable ``scraper_alert_pubsub_topic``.
    """
    project = os.environ.get("GCP_PROJECT_ID")
    return f"projects/{project}/topics/{ALERTS_TOPIC_NAME}" if project else None


def to_plain_text(message: str) -> str:
    """Remove as tags ``<b>``/``</b>`` (HTML do Telegram) dos alertas das DAGs.

    O alert-relay escapa o HTML recebido, então as tags apareceriam literalmente.
    """
    return _BOLD_TAG_RE.sub("", message)


def _truncate_lines(text: str, limit: int) -> str:
    """Corta o texto em fim de linha para caber em ``limit``, com um marcador no fim."""
    if len(text) <= limit:
        return text
    budget = limit - len(_TRUNCATED_MARKER) - 1
    kept: list[str] = []
    size = 0
    for line in text.split("\n"):
        extra = len(line) + (1 if kept else 0)
        if size + extra > budget:
            if not kept:
                # Primeira linha já maior que o limite: corta no meio dela.
                kept.append(line[:budget])
            break
        kept.append(line)
        size += extra
    return "\n".join(kept + [_TRUNCATED_MARKER])


def build_pubsub_payload(
    message: str,
    title: str | None = None,
    severity: str = "warning",
    source: str | None = None,
) -> dict:
    """Mensagem genérica do alert-relay: ``{"text", "title"?, "severity"?, "source"?}``.

    O texto vai sem HTML e com no máximo ``PUBSUB_MAX_TEXT_CHARS`` caracteres.
    """
    payload = {
        "text": _truncate_lines(to_plain_text(message), PUBSUB_MAX_TEXT_CHARS),
        "severity": severity if severity in ALERT_SEVERITIES else "warning",
    }
    if title:
        payload["title"] = title
    if source:
        payload["source"] = source
    return payload


def publish_pubsub_alert(
    topic: str,
    message: str,
    title: str | None = None,
    severity: str = "warning",
    source: str | None = None,
) -> bool:
    """Publica o alerta no Pub/Sub pela API REST (sem depender do google-cloud-pubsub).

    Usa as credenciais padrão do ambiente (no Composer, a SA do ambiente, que precisa de
    ``roles/pubsub.publisher`` no tópico).

    Args:
        topic: Nome completo, ``projects/<projeto>/topics/<tópico>``.
        message: Texto do alerta (as tags ``<b>`` são removidas).
        title: Título exibido em negrito pelo relay.
        severity: ``info``, ``warning`` ou ``critical``.
        source: Origem exibida em itálico pelo relay.

    Returns:
        True se o Pub/Sub aceitou a mensagem (HTTP 200).
    """
    if not isinstance(topic, str) or not _TOPIC_RE.fullmatch(topic):
        logger.error(
            "Invalid Pub/Sub alert topic %r (expected projects/<project>/topics/<topic>)", topic
        )
        return False

    payload = build_pubsub_payload(message, title=title, severity=severity, source=source)
    data = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode("ascii")
    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(scopes=PUBSUB_SCOPES)
        with AuthorizedSession(credentials) as session:
            response = session.post(
                f"https://pubsub.googleapis.com/v1/{topic}:publish",
                json={"messages": [{"data": data}]},
                timeout=PUBSUB_TIMEOUT_SECONDS,
            )
    except Exception as e:
        # Só o tipo e a mensagem do erro: credenciais e token nunca vão para o log.
        logger.error(f"Failed to publish Pub/Sub alert: {type(e).__name__}: {str(e)[:200]}")
        return False

    if response.status_code != 200:
        # O corpo é o erro da API do Pub/Sub (ex.: PERMISSION_DENIED), sem credenciais.
        logger.error(
            "Failed to publish Pub/Sub alert: HTTP %s %s",
            response.status_code,
            response.text[:300],
        )
        return False
    logger.info("Alert published to %s (%d chars)", topic, len(payload["text"]))
    return True


def send_alert(
    message: str,
    telegram_token: str | None = None,
    telegram_chat_id: str | None = None,
    webhook_url: str | None = None,
    pubsub_topic: str | None = None,
    title: str | None = None,
    severity: str = "warning",
    source: str | None = None,
) -> bool:
    """Send an alert via the best available channel.

    Priority: Telegram > Pub/Sub (``dgb.ops.alerts``) > webhook > log-only. Each channel
    that fails falls through to the next one.

    Args:
        message: Alert text (HTML ``<b>`` for Telegram; stripped for Pub/Sub; as is for webhook).
        telegram_token: Telegram bot token (None = skip Telegram).
        telegram_chat_id: Target chat ID (None = skip Telegram).
        webhook_url: Fallback webhook URL for POST (None = skip).
        pubsub_topic: ``projects/<p>/topics/<t>`` relayed to Telegram (None = skip).
        title: Title for the Pub/Sub message.
        severity: Severity for the Pub/Sub message (info, warning, critical).
        source: Source for the Pub/Sub message (e.g. the DAG name).

    Returns:
        True if alert was delivered via at least one channel.
    """
    if telegram_token and telegram_chat_id:
        if send_telegram_alert(telegram_token, telegram_chat_id, message):
            return True
        logger.warning("Telegram alert failed, trying fallback...")

    if pubsub_topic:
        if publish_pubsub_alert(
            pubsub_topic, message, title=title, severity=severity, source=source
        ):
            return True
        logger.warning("Pub/Sub alert failed, trying fallback...")

    if webhook_url:
        try:
            resp = httpx.post(webhook_url, json={"text": message}, timeout=5.0)
            if resp.status_code < 300:
                return True
        except Exception as e:
            logger.error(f"Webhook alert failed: {e}")

    # Fallback: log the alert
    logger.info(f"Alert (log-only): {message}")
    return False
