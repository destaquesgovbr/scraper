"""
DAG manual de backfill da raspagem: ``scrape_backfill``.

Passo 3 da rearquitetura da ingestão (``_plan/rearquitetura-ingestao.md``). A ingestão
regular sai do Airflow: o Cloud Scheduler chama o ``POST /dispatch`` do scraper-api a cada
15 min, e o despacho cria uma tarefa por agência vencida na fila ``scrape-govbr`` do Cloud
Tasks. Esta DAG fica com o backfill:

- Disparo manual (``schedule=None``), com os Params ``agencies`` (lista de siglas; vazia =
  todas as ativas), ``start_date`` e ``end_date`` (obrigatórios, YYYY-MM-DD, datas de
  Brasília), ``force`` (ignora o disjuntor) e ``dry_run`` (só lista o que seria despachado).
- Uma task só: ``POST /dispatch`` com ``{"reason": "backfill", ...}``, autenticada com ID
  token, na Variable ``scraper_api_url`` (a mesma das outras DAGs). O ``/dispatch`` cria uma
  tarefa por agência com a janela explícita, que não mexe no estado da agência
  (``scrape_agency_state``), e responde na hora: a raspagem roda depois, na fila.
- ``dedup_key`` (hash do ``run_id``) vai no nome das tarefas: a repetição da task na mesma
  execução (timeout ou 5xx depois de criar parte das tarefas) recebe 409 nas já criadas, que
  contam como duplicadas, em vez de raspar a mesma janela duas vezes.
- Falha sem retry (``AirflowFailException``): params inválidos, 4xx, despacho em ``off``
  (pausado), despacho fora de ``live`` sem ``dry_run`` (nada criado) e tarefas que o Cloud
  Tasks recusou (a mensagem lista as agências, para disparar de novo só com elas). Erro de
  rede, 5xx e corpo sem JSON repetem 1 vez, depois de 5 min (intervalo fixo, sem backoff
  exponencial: ver scrape_agencies.py).

O resultado de cada agência sai no log do Cloud Run (``jsonPayload.event="scrape_attempt"``,
``trigger="backfill"``). Ver docs/runbook.md, "Backfill (DAG scrape_backfill)".
"""

import hashlib
import json
import logging
import re
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

logger = logging.getLogger(__name__)

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_AGENCY_RE = re.compile(r"[A-Za-z0-9_-]+")

# Tempo de resposta do /dispatch: um backfill de todas as agências cria ~150 tarefas, cada
# uma com até 10 s de timeout no Cloud Tasks.
DISPATCH_TIMEOUT_SECONDS = 300


def _parse_param_date(value, name: str) -> date:
    """Valida um param de data obrigatório no formato YYYY-MM-DD."""
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ValueError(f"{name} é obrigatório, no formato YYYY-MM-DD: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"{name} não é uma data válida: {value!r}") from e


def _parse_agencies(value) -> list[str]:
    """Lista de siglas do param ``agencies`` (vazia = todas as ativas).

    Aceita lista ou texto separado por vírgula, espaço ou quebra de linha; remove vazios e
    repetidos, mantendo a ordem.
    """
    if value is None:
        return []
    if isinstance(value, str):
        items = re.split(r"[\s,]+", value)
    elif isinstance(value, list | tuple):
        items = [str(v) for v in value]
    else:
        raise ValueError(f"agencies deve ser uma lista de siglas: {value!r}")
    agencies = [item.strip() for item in items if item and item.strip()]
    invalid = [a for a in agencies if not _AGENCY_RE.fullmatch(a)]
    if invalid:
        raise ValueError(f"siglas inválidas em agencies: {', '.join(invalid)}")
    return list(dict.fromkeys(agencies))


def _dedup_key(run_id) -> str | None:
    """Sufixo estável do nome das tarefas: 12 hex do sha256 do ``run_id`` (None sem ele)."""
    if not run_id:
        return None
    return hashlib.sha256(str(run_id).encode("utf-8")).hexdigest()[:12]


def _backfill_payload(params=None, run_id=None) -> dict:
    """Corpo de ``POST /dispatch`` para o backfill. Params inválidos levantam ValueError."""
    params = params or {}
    start = _parse_param_date(params.get("start_date"), "start_date")
    end = _parse_param_date(params.get("end_date"), "end_date")
    if start > end:
        raise ValueError(f"start_date ({start}) é posterior a end_date ({end})")
    payload = {
        "reason": "backfill",
        "agencies": _parse_agencies(params.get("agencies")),
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "force": params.get("force") is True,
        "dry_run": params.get("dry_run") is True,
    }
    key = _dedup_key(run_id)
    if key:
        payload["dedup_key"] = key
    return payload


def _check_dispatch_response(status_code: int, body, payload: dict) -> dict:
    """Confere a resposta do ``/dispatch`` e devolve o resumo (XCom); levanta na falha.

    - 4xx: ``AirflowFailException`` (payload recusado; repetir não adianta).
    - 5xx ou corpo que não é JSON de objeto: ``AirflowException`` (repete 1 vez).
    - ``mode`` off: ``AirflowFailException`` (despacho pausado).
    - ``mode`` dry_run sem o param ``dry_run``: ``AirflowFailException`` (nada foi criado).
    - ``failed`` > 0: ``AirflowFailException`` com as agências recusadas (um disparo novo é
      outra execução, com outro ``dedup_key``: dispare de novo só com as listadas).
    """
    from airflow.exceptions import AirflowException, AirflowFailException

    if 400 <= status_code < 500:
        raise AirflowFailException(
            f"/dispatch recusou o backfill (HTTP {status_code}): {body!r}"[:1000]
        )
    if status_code >= 500 or not isinstance(body, dict):
        raise AirflowException(f"/dispatch falhou (HTTP {status_code}): {body!r}"[:1000])

    mode = body.get("mode")
    if mode == "off":
        raise AirflowFailException(
            "O despacho está em off (SCRAPE_DISPATCH_MODE, pausado): nenhuma tarefa criada"
        )
    if mode != "live" and not payload.get("dry_run"):
        raise AirflowFailException(
            f"O despacho está em {mode}: nenhuma tarefa criada. Use dry_run=true para só "
            "listar, ou a recuperação pelos params start_date/end_date da scrape_agencies"
        )

    agencies = body.get("agencies") if isinstance(body.get("agencies"), list) else []
    failed = [
        a.get("agency") for a in agencies if isinstance(a, dict) and a.get("result") == "failed"
    ]
    summary = {
        "mode": mode,
        "start_date": payload["start_date"],
        "end_date": payload["end_date"],
        "selected": body.get("due", 0),
        "dispatched": body.get("dispatched", 0),
        "duplicates": body.get("duplicates", 0),
        "failed": failed,
        "skipped_circuit": body.get("skipped_circuit") or [],
        "unknown_agencies": body.get("unknown_agencies") or [],
    }
    if failed:
        raise AirflowFailException(
            f"O Cloud Tasks recusou {len(failed)} tarefa(s); dispare de novo com "
            f"agencies = {', '.join(failed)}"
        )
    return summary


_AGENCIES_HELP = (
    "Siglas das agências (as chaves do site_urls.yaml/ebc_urls.yaml, ex.: mec, "
    "agencia_brasil). Vazia = todas as ativas."
)
_DATE_HELP = (
    "Obrigatório, YYYY-MM-DD, em datas de Brasília. A janela vai explícita: a raspagem não "
    "mexe no estado da agência (marca d'água, disjuntor). Cada agência é uma tarefa de até "
    "900 s: até ~100 notícias por agência (agencia_brasil 2 dias, pf e tvbrasil 4; runbook)."
)
_FORCE_HELP = "Ignora o disjuntor: inclui as agências com o circuito aberto."
_DRY_RUN_HELP = "Só lista as agências que seriam despachadas, sem criar tarefas."


@dag(
    dag_id="scrape_backfill",
    description="Backfill manual da raspagem: POST /dispatch com uma janela explícita",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "backfill"],
    params={
        "agencies": Param(
            [],
            type="array",
            items={"type": "string"},
            title="Agências",
            description=_AGENCIES_HELP,
        ),
        "start_date": Param(
            type="string",
            format="date",
            title="Data inicial",
            description=_DATE_HELP,
        ),
        "end_date": Param(
            type="string",
            format="date",
            title="Data final",
            description=_DATE_HELP,
        ),
        "force": Param(False, type="boolean", title="Ignorar o disjuntor", description=_FORCE_HELP),
        "dry_run": Param(False, type="boolean", title="Só listar", description=_DRY_RUN_HELP),
    },
    default_args={
        "owner": "scraper",
        # Intervalo fixo, sem backoff exponencial (ver scrape_agencies.py).
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
    },
)
def scrape_backfill_dag():
    @task
    def dispatch_backfill():
        """Chama ``POST /dispatch`` com ``reason=backfill`` e confere a resposta."""
        import google.auth.transport.requests
        import google.oauth2.id_token
        import httpx
        from airflow.exceptions import AirflowFailException
        from airflow.models import Variable
        from airflow.sdk import get_current_context

        context = get_current_context()
        try:
            payload = _backfill_payload(context.get("params"), context.get("run_id"))
        except ValueError as e:
            # Params inválidos no disparo manual: repetir não adianta.
            raise AirflowFailException(str(e)) from e

        scraper_api_url = Variable.get("scraper_api_url", default_var="")
        if not scraper_api_url:
            raise ValueError("Missing required Airflow Variable: scraper_api_url")

        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

        logger.info(
            "Backfill %s a %s: %s%s%s",
            payload["start_date"],
            payload["end_date"],
            ", ".join(payload["agencies"]) or "todas as ativas",
            " (force)" if payload["force"] else "",
            " (dry_run)" if payload["dry_run"] else "",
        )
        # Erro de rede/timeout do httpx propaga e o Airflow repete.
        response = httpx.post(
            f"{scraper_api_url}/dispatch",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=float(DISPATCH_TIMEOUT_SECONDS),
        )
        try:
            body = response.json()
        except ValueError:
            body = None
            logger.warning(
                "/dispatch respondeu HTTP %s sem JSON: %s",
                response.status_code,
                str(response.text)[:500],
            )
        else:
            logger.info(
                "/dispatch (HTTP %s):\n%s",
                response.status_code,
                json.dumps(body, indent=2, ensure_ascii=False),
            )

        summary = _check_dispatch_response(response.status_code, body, payload)
        if summary["skipped_circuit"]:
            logger.warning(
                "Puladas pelo disjuntor (use force=true para incluir): %s",
                ", ".join(summary["skipped_circuit"]),
            )
        if summary["unknown_agencies"]:
            logger.warning(
                "Fora das agências ativas (ignoradas): %s", ", ".join(summary["unknown_agencies"])
            )
        logger.info(
            "Backfill despachado: %d selecionadas, %d tarefas criadas, %d duplicadas",
            summary["selected"],
            summary["dispatched"],
            summary["duplicates"],
        )
        return summary

    dispatch_backfill()


dag_instance = scrape_backfill_dag()
