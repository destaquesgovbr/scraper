"""Configuração dos logs: JSON por linha no stderr (Cloud Logging) ou texto (local e testes).

No Cloud Run, cada linha JSON do stderr vira um ``jsonPayload``: ``severity`` dá o nível,
``message`` o texto e os campos de ``logger.bind(...)``/extra vão no nível raiz, o que
permite métricas baseadas em log (``jsonPayload.event="scrape_attempt"`` etc.).

- ``LOG_FORMAT``: ``json`` (padrão) ou ``text`` (formato antigo do loguru).
- ``LOG_LEVEL``: nível mínimo (padrão INFO).

No modo JSON, os logs do ``logging`` da stdlib (managers, api) também passam pelo loguru,
para saírem em JSON com severity.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from datetime import UTC
from typing import Any

from loguru import logger

# Nível do loguru → severity do Cloud Logging.
_SEVERITY = {
    "TRACE": "DEBUG",
    "DEBUG": "DEBUG",
    "INFO": "INFO",
    "SUCCESS": "NOTICE",
    "WARNING": "WARNING",
    "ERROR": "ERROR",
    "CRITICAL": "CRITICAL",
}
_BASE_KEYS = ("severity", "message", "time")
_SOURCE_LOCATION = "logging.googleapis.com/sourceLocation"


def json_log_line(record: dict[str, Any]) -> str:
    """Uma linha JSON a partir de um record do loguru."""
    message = record["message"]
    exception = record.get("exception")
    if exception is not None and exception.type is not None:
        message += "\n" + "".join(
            traceback.format_exception(exception.type, exception.value, exception.traceback)
        )
    payload: dict[str, Any] = {
        "severity": _SEVERITY.get(record["level"].name, "DEFAULT"),
        "message": message,
        "time": record["time"].astimezone(UTC).isoformat(),
        "logger": record["name"],
        _SOURCE_LOCATION: {
            "file": record["file"].path if record.get("file") else None,
            "line": record["line"],
            "function": record["function"],
        },
    }
    for key, value in record["extra"].items():
        if key not in _BASE_KEYS:
            payload[key] = value
    return json.dumps(payload, ensure_ascii=False, default=str)


def _json_sink(message) -> None:
    # sys.stderr é lido a cada linha (e não fixado no add) para respeitar redirecionamentos.
    sys.stderr.write(json_log_line(message.record) + "\n")
    sys.stderr.flush()


class _InterceptHandler(logging.Handler):
    """Repassa os registros do ``logging`` da stdlib ao loguru (receita da documentação)."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = logging.currentframe(), 2
        while frame is not None and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).bind(stdlib_logger=record.name).log(
            level, record.getMessage()
        )


def _stdlib_level(level: str) -> int:
    if level == "TRACE":
        return logging.DEBUG
    if level == "SUCCESS":
        return logging.INFO
    return logging.getLevelNamesMapping().get(level, logging.INFO)


def configure_logging(log_format: str | None = None, level: str | None = None) -> str:
    """Troca o sink do loguru conforme ``LOG_FORMAT`` e devolve o formato aplicado.

    Pode ser chamada de novo (testes): remove os sinks e o handler de interceptação
    anteriores antes de instalar os novos.
    """
    fmt = (log_format or os.getenv("LOG_FORMAT") or "json").strip().lower()
    lvl = (level or os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    root = logging.getLogger()
    root.handlers = [h for h in root.handlers if not isinstance(h, _InterceptHandler)]
    logger.remove()
    if fmt == "text":
        logger.add(sys.stderr, level=lvl)
        return "text"
    logger.add(_json_sink, level=lvl, format="{message}")
    root.addHandler(_InterceptHandler())
    root.setLevel(_stdlib_level(lvl))
    return "json"
