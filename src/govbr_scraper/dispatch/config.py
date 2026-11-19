"""Configuração do despacho por fila (``POST /dispatch``): modo e envs do Cloud Tasks.

Envs definidas pelo Terraform (repo ``infra``, ``terraform/scraper-api.tf``):

- ``SCRAPE_DISPATCH_MODE``: ``off`` (padrão: o ``/dispatch`` só loga ``dispatch_run``),
  ``dry_run`` (calcula, loga e recalcula as faixas, sem criar tarefa nem reconciliar) ou
  ``live``;
- ``SCRAPE_TASKS_QUEUE``: fila completa (``projects/…/locations/…/queues/scrape-govbr``);
- ``SCRAPE_TASKS_INVOKER_SA``: SA do OIDC das tarefas (``scrape-tasks-invoker@…``);
- ``SCRAPE_TASKS_TARGET_URL``: URL do próprio scraper-api, alvo das tarefas e audience do
  OIDC.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum

from loguru import logger

DISPATCH_MODE_ENV = "SCRAPE_DISPATCH_MODE"
TASKS_QUEUE_ENV = "SCRAPE_TASKS_QUEUE"
TASKS_INVOKER_SA_ENV = "SCRAPE_TASKS_INVOKER_SA"
TASKS_TARGET_URL_ENV = "SCRAPE_TASKS_TARGET_URL"


class DispatchMode(str, Enum):
    """Modo do ``/dispatch``."""

    OFF = "off"
    DRY_RUN = "dry_run"
    LIVE = "live"


class DispatchConfigError(RuntimeError):
    """Env do Cloud Tasks ausente com o despacho em ``live``."""


def dispatch_mode_from_env(raw: str | None = None) -> DispatchMode:
    """Modo do despacho a partir de ``SCRAPE_DISPATCH_MODE`` (ou de ``raw``).

    Ausente ou vazio vale ``off``. Valor desconhecido também vale ``off``, com warning: o
    ``off`` é a chave de pausa, e um erro de digitação não pode virar despacho.
    """
    value = (raw if raw is not None else os.getenv(DISPATCH_MODE_ENV, "")).strip().lower()
    if not value:
        return DispatchMode.OFF
    try:
        return DispatchMode(value)
    except ValueError:
        logger.warning("{env} inválido ({value!r}): usando off", env=DISPATCH_MODE_ENV, value=value)
        return DispatchMode.OFF


def effective_mode(env_mode: DispatchMode, dry_run: bool) -> DispatchMode:
    """Modo desta chamada: ``dry_run`` no corpo rebaixa ``live`` para ``dry_run``.

    ``off`` continua ``off``: é a pausa do despacho e vale também para o backfill.
    """
    if dry_run and env_mode == DispatchMode.LIVE:
        return DispatchMode.DRY_RUN
    return env_mode


@dataclass(frozen=True)
class TasksConfig:
    """Fila, SA invocadora e URL-alvo das tarefas do Cloud Tasks."""

    queue: str
    invoker_sa: str
    target_url: str

    @classmethod
    def from_env(cls) -> TasksConfig:
        """Lê as três envs; levanta ``DispatchConfigError`` com as que faltam."""
        values = {
            env: os.getenv(env, "").strip()
            for env in (TASKS_QUEUE_ENV, TASKS_INVOKER_SA_ENV, TASKS_TARGET_URL_ENV)
        }
        missing = [env for env, value in values.items() if not value]
        if missing:
            raise DispatchConfigError(
                f"despacho em live sem {', '.join(missing)}: nenhuma tarefa pode ser criada"
            )
        return cls(
            queue=values[TASKS_QUEUE_ENV].rstrip("/"),
            invoker_sa=values[TASKS_INVOKER_SA_ENV],
            target_url=values[TASKS_TARGET_URL_ENV].rstrip("/"),
        )
