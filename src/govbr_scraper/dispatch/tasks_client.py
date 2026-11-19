"""Cliente mínimo do Cloud Tasks pela API REST (``google.auth`` + ``AuthorizedSession``).

Sem dependência nova: ``google-auth`` e ``requests`` já vêm com o google-cloud-pubsub. Só
cria tarefas HTTP para ``{TARGET_URL}/scrape/agency`` com OIDC da SA invocadora (audience
= ``TARGET_URL``) e ``dispatchDeadline`` de 900 s.

O nome da tarefa é a deduplicação: o Cloud Tasks responde 409 (``ALREADY_EXISTS``) para um
nome já usado (inclusive de tarefa já executada, por cerca de 1 h), o que conta como
duplicada. Qualquer outro erro (HTTP ou de rede) conta como falha daquela tarefa; o cliente
nunca levanta exceção.
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

from govbr_scraper.state.policy import BRT

CLOUD_TASKS_API = "https://cloudtasks.googleapis.com/v2"
CLOUD_TASKS_SCOPES = ("https://www.googleapis.com/auth/cloud-platform",)
DISPATCH_DEADLINE_SECONDS = 900  # o timeout do scraper-api
REQUEST_TIMEOUT_SECONDS = 10
SCRAPE_AGENCY_PATH = "/scrape/agency"

CREATED = "created"
DUPLICATE = "duplicate"
FAILED = "failed"

# Nome de tarefa: letras, dígitos, hífen e sublinhado (até 500 caracteres).
_INVALID_TASK_ID_CHARS = re.compile(r"[^A-Za-z0-9_-]")


def _safe(value: str) -> str:
    return _INVALID_TASK_ID_CHARS.sub("-", value)


def _brt(now: datetime) -> datetime:
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    return now.astimezone(BRT)


def tick_task_id(agency_key: str, now: datetime) -> str:
    """``scrape-<agência>-<YYYYMMDDHHmm do tick em BRT, arredondado para baixo a 15 min>``.

    Dois ticks no mesmo quarto de hora (ex.: o Scheduler e uma chamada manual) geram o mesmo
    nome, e a segunda tarefa vira duplicada.
    """
    local = _brt(now)
    local = local.replace(minute=local.minute - local.minute % 15, second=0, microsecond=0)
    return f"scrape-{_safe(agency_key)}-{local:%Y%m%d%H%M}"


def backfill_task_id(agency_key: str, start: date, end: date, now: datetime) -> str:
    """``backfill-<agência>-<início>-<fim>-<HHmm em BRT>`` (datas em ``YYYYMMDD``)."""
    return f"backfill-{_safe(agency_key)}-{start:%Y%m%d}-{end:%Y%m%d}-{_brt(now):%H%M}"


@dataclass(frozen=True)
class CreateResult:
    """Resultado da criação de uma tarefa: ``created``, ``duplicate`` ou ``failed``."""

    status: str
    task_id: str
    http_status: int | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        """Criada ou duplicada: a agência foi despachada."""
        return self.status in (CREATED, DUPLICATE)


class CloudTasksClient:
    """Cria tarefas HTTP na fila ``queue`` (``projects/…/locations/…/queues/…``)."""

    def __init__(
        self,
        queue: str,
        target_url: str,
        invoker_sa: str,
        *,
        session: Any = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        self.queue = queue.rstrip("/")
        self.target_url = target_url.rstrip("/")
        self.invoker_sa = invoker_sa
        self.timeout = timeout
        self._session = session
        self._owns_session = session is None

    @classmethod
    def from_config(cls, config, **kwargs) -> CloudTasksClient:
        """A partir de um ``TasksConfig``."""
        return cls(config.queue, config.target_url, config.invoker_sa, **kwargs)

    def build_task(self, task_id: str, body: dict[str, Any]) -> dict[str, Any]:
        """O recurso ``Task`` da API: POST JSON para ``/scrape/agency`` com OIDC."""
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        return {
            "name": f"{self.queue}/tasks/{task_id}",
            "dispatchDeadline": f"{DISPATCH_DEADLINE_SECONDS}s",
            "httpRequest": {
                "url": f"{self.target_url}{SCRAPE_AGENCY_PATH}",
                "httpMethod": "POST",
                "headers": {"Content-Type": "application/json"},
                "body": base64.b64encode(payload).decode("ascii"),
                "oidcToken": {
                    "serviceAccountEmail": self.invoker_sa,
                    "audience": self.target_url,
                },
            },
        }

    def _get_session(self):
        if self._session is None:
            import google.auth
            from google.auth.transport.requests import AuthorizedSession

            credentials, _ = google.auth.default(scopes=list(CLOUD_TASKS_SCOPES))
            self._session = AuthorizedSession(credentials)
        return self._session

    def create_task(self, task_id: str, body: dict[str, Any]) -> CreateResult:
        """Cria a tarefa. 2xx = criada; 409 = duplicada; o resto = falha (sem exceção)."""
        request = {"task": self.build_task(task_id, body), "responseView": "BASIC"}
        try:
            response = self._get_session().post(
                f"{CLOUD_TASKS_API}/{self.queue}/tasks", json=request, timeout=self.timeout
            )
        except Exception as e:
            # Só o tipo e a mensagem: credenciais e token nunca vão para o log.
            return CreateResult(FAILED, task_id, error=f"{type(e).__name__}: {str(e)[:200]}")
        status = response.status_code
        if status == 409:
            return CreateResult(DUPLICATE, task_id, http_status=status)
        if 200 <= status < 300:
            return CreateResult(CREATED, task_id, http_status=status)
        # O corpo é o erro da API (ex.: PERMISSION_DENIED, NOT_FOUND), sem credenciais.
        return CreateResult(FAILED, task_id, http_status=status, error=str(response.text)[:300])

    def close(self) -> None:
        """Fecha a sessão HTTP criada pelo cliente (a injetada é de quem a passou)."""
        if self._owns_session and self._session is not None:
            try:
                self._session.close()
            except Exception:  # nosec B110 - fechar a sessão é o melhor esforço
                pass
            self._session = None
