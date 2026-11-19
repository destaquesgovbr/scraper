"""Alertas operacionais do scraper-api no tópico ``dgb.ops.alerts`` (alert-relay → Telegram).

O tópico vem da env ``PUBSUB_TOPIC_OPS_ALERTS`` (caminho completo
``projects/<projeto>/topics/dgb.ops.alerts``). O payload é o JSON genérico do alert-relay do
data-platform: ``{"text", "title", "severity", "source", "test"}``.
"""

from __future__ import annotations

import json
import os

from loguru import logger

from govbr_scraper.state.model import (
    REASON_FORCED_SUCCESS,
    REASON_PERMANENT_ERRORS,
    REASON_PROBE_SUCCESS,
    REASON_TRANSIENT_24H,
    CircuitState,
    Transition,
)
from govbr_scraper.state.policy import PROBE_DELAYS

OPS_ALERTS_TOPIC_ENV = "PUBSUB_TOPIC_OPS_ALERTS"
ALERT_SOURCE = "scraper-api"
_SEVERITIES = ("info", "warning", "critical")
# O relay corta em 3500; 3400 deixa folga, como o notify.py das DAGs.
_MAX_TEXT_CHARS = 3400
_PUBLISH_TIMEOUT_SECONDS = 10

_REASON_TEXT = {
    REASON_PERMANENT_ERRORS: "erros permanentes seguidos",
    REASON_TRANSIENT_24H: "falhas seguidas há mais de 24 h",
    REASON_PROBE_SUCCESS: "a sonda raspou com sucesso",
    REASON_FORCED_SUCCESS: "raspagem forçada (force) com sucesso",
}


def _fit(text: str) -> str:
    if len(text) <= _MAX_TEXT_CHARS:
        return text
    cut = text.rfind("\n", 0, _MAX_TEXT_CHARS)
    return text[: cut if cut > 0 else _MAX_TEXT_CHARS]


def publish_ops_alert(text: str, title: str, severity: str, *, test: bool = False) -> bool:
    """Publica um alerta em ``dgb.ops.alerts`` e espera a confirmação (até 10 s).

    Sem a env ``PUBSUB_TOPIC_OPS_ALERTS``, só loga um warning e devolve False. Nunca levanta
    exceção: o alerta não pode derrubar a raspagem. True quando o Pub/Sub confirmou.
    """
    topic = os.getenv(OPS_ALERTS_TOPIC_ENV, "").strip()
    if not topic:
        logger.warning(
            "{env} não definido: alerta não publicado ({title}): {text}",
            env=OPS_ALERTS_TOPIC_ENV,
            title=title,
            text=text,
        )
        return False
    payload = {
        "text": _fit(text),
        "title": title,
        "severity": severity if severity in _SEVERITIES else "warning",
        "source": ALERT_SOURCE,
        "test": bool(test),
    }
    try:
        from google.cloud import pubsub_v1

        client = pubsub_v1.PublisherClient()
        future = client.publish(topic, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
        future.result(timeout=_PUBLISH_TIMEOUT_SECONDS)
    except Exception as e:
        logger.warning(
            "Falha ao publicar alerta em {topic} ({title}): {err}", topic=topic, title=title, err=e
        )
        return False
    logger.info("Alerta publicado em {topic}: {title}", topic=topic, title=title)
    return True


def _hours(delay) -> str:
    return f"{int(delay.total_seconds() // 3600)} h"


def alert_for_transition(t: Transition) -> dict[str, str]:
    """Alerta de uma transição do disjuntor: ``{"text", "title", "severity"}``.

    🔴 ao abrir (warning) e 🟢 ao fechar (info), com a agência, o motivo e a categoria.
    """
    reason = _REASON_TEXT.get(t.reason, t.reason)
    category = t.category or "sem categoria"
    if t.to_state == CircuitState.OPEN:
        text = (
            f"🔴 Disjuntor aberto: {t.agency_key}\n"
            f"Motivo: {reason} ({t.reason})\n"
            f"Categoria: {category}\n"
            f"A agência sai da raspagem regular. Sondas em "
            f"{', '.join(_hours(d) for d in PROBE_DELAYS)} e depois a cada "
            f"{_hours(PROBE_DELAYS[-1])}; o circuito fecha na primeira sonda com sucesso."
        )
        return {
            "text": text,
            "title": f"Scraper: disjuntor aberto ({t.agency_key})",
            "severity": "warning",
        }
    text = (
        f"🟢 Disjuntor fechado: {t.agency_key}\n"
        f"Motivo: {reason} ({t.reason})\n"
        f"Categoria do erro que abriu: {category}\n"
        "A agência volta à raspagem regular."
    )
    return {
        "text": text,
        "title": f"Scraper: disjuntor fechado ({t.agency_key})",
        "severity": "info",
    }


def announce_transition(t: Transition) -> bool:
    """Loga o evento ``circuit_transition`` e publica o alerta (depois do commit)."""
    from govbr_scraper.monitoring.structured_log import log_circuit_transition

    log_circuit_transition(t)
    return publish_ops_alert(**alert_for_transition(t))
