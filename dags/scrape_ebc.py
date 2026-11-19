"""DAG para scraping de notícias EBC (Agência Brasil, TV Brasil) via Cloud Run API.

Mesma agenda da ``scrape_agencies``: a cada 2 horas, das 06h às 22h em horário de Brasília
(``0 6-22/2 * * *``). Manda as duas fontes explicitamente (``agencies: ["agencia_brasil",
"tvbrasil"]``) e sem datas: a API usa a marca d'água de cada fonte. Params opcionais
``start_date``/``end_date`` (YYYY-MM-DD) para recuperar um período depois de uma queda
(janela explícita, que não mexe no estado) e ``force`` (ignora o disjuntor).

Fonte com o circuito aberto é pulada pela API (``skipped``) e não falha a task; com as duas
puladas, a task termina como skipped (``AirflowSkipException``).

Erro permanente (todos os ``errors[]`` com ``retryable: false``) falha sem retry
(``AirflowFailException``); se algum erro é passageiro, o corpo não é JSON ou a API é antiga
(sem ``retryable``), a task repete (2x, a cada 5 min), como antes.

As funções de janela e de leitura da resposta são cópia das de ``scrape_agencies.py`` de
propósito: cada arquivo de DAG é autocontido (não importa outro módulo de DAG).
"""

import json
import logging
import re
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task
from airflow.sdk import Param

logger = logging.getLogger(__name__)


# As fontes ativas do ebc_urls.yaml do scraper-api, cada uma com o próprio estado.
EBC_SOURCES = ["agencia_brasil", "tvbrasil"]

# status do corpo de /scrape/ebc que indica falha (a API também responde 500/207).
_FAILED_BODY_STATUSES = ("failed", "partial")

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _parse_param_date(value, name: str) -> date:
    """Valida um param de data no formato YYYY-MM-DD."""
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ValueError(f"{name} deve estar no formato YYYY-MM-DD: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"{name} não é uma data válida: {value!r}") from e


def _resolve_window(params=None) -> tuple[str, str] | None:
    """Janela explícita dos params ``start_date``/``end_date``, ou None.

    None (os dois vazios ou ausentes): a DAG não manda datas e a API usa a marca d'água de
    cada fonte. Os params servem para recuperar um período depois de uma queda (disparo
    manual) e só valem juntos. Erro de formato, só um dos dois ou início depois do fim
    levantam ValueError.
    """
    params = params or {}
    start = params.get("start_date") or None
    end = params.get("end_date") or None
    if start is None and end is None:
        return None
    if start is None or end is None:
        raise ValueError("Informe start_date e end_date juntos (YYYY-MM-DD), ou nenhum dos dois")
    start_date = _parse_param_date(start, "start_date")
    end_date = _parse_param_date(end, "end_date")
    if start_date > end_date:
        raise ValueError(f"start_date ({start}) é posterior a end_date ({end})")
    return start_date.isoformat(), end_date.isoformat()


def _scrape_payload(agency_keys: list[str], params=None) -> dict:
    """Corpo de /scrape/*: sem datas por padrão (marca d'água); com a janela dos params, se
    houver; ``force`` do param (ignora o disjuntor). Levanta ValueError com params inválidos.
    """
    params = params or {}
    payload = {
        "agencies": list(agency_keys),
        "allow_update": False,
        "sequential": True,
        "force": params.get("force") is True,
    }
    window = _resolve_window(params)
    if window is not None:
        payload["start_date"], payload["end_date"] = window
    return payload


def _on_scrape_failure(context):
    """Callback para log estruturado de falha na DAG de scraping."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "unknown"
    task_id = ti.task_id if ti else "unknown"
    exception = context.get("exception", "unknown")
    try_number = ti.try_number if ti else 0
    logger.error(
        "Scrape DAG failure: dag=%s task=%s try=%d error=%s",
        dag_id,
        task_id,
        try_number,
        exception,
    )


def _response_errors(body, agency_key: str | None = None) -> list[dict]:
    """Itens de ``errors[]`` do corpo de /scrape/* (só os da agência, se informada).

    Corpo que não é o JSON de /scrape/* (``{"detail": ...}`` de exceção, página de erro do
    Cloud Run, texto) devolve [].
    """
    if not isinstance(body, dict):
        return []
    errors = body.get("errors")
    if not isinstance(errors, list):
        return []
    items = [e for e in errors if isinstance(e, dict)]
    if agency_key is not None:
        items = [e for e in items if e.get("agency") == agency_key]
    return items


def _all_permanent(errors: list[dict]) -> bool:
    """True se há erro e todos são permanentes (``retryable`` é False).

    API antiga, sem ``retryable``: conta como passageiro e repete, como antes.
    """
    return bool(errors) and all(e.get("retryable") is False for e in errors)


def _describe_errors(errors: list[dict]) -> str:
    """Resumo legível dos erros para a mensagem da exceção (agência, categoria, mensagem)."""
    return "; ".join(
        f"{e.get('agency')} [{e.get('category') or 'sem categoria'}]: {e.get('error')}"
        for e in errors
    )


def _response_skipped(body, agency_key: str | None = None) -> list[dict]:
    """Itens de ``skipped[]`` do corpo de /scrape/* (só os da agência, se informada).

    A API pula a agência com o circuito aberto cuja sonda ainda não venceu. API antiga, sem
    o campo, devolve [].
    """
    if not isinstance(body, dict):
        return []
    skipped = body.get("skipped")
    if not isinstance(skipped, list):
        return []
    items = [s for s in skipped if isinstance(s, dict)]
    if agency_key is not None:
        items = [s for s in items if s.get("agency") == agency_key]
    return items


def _check_scrape_response(status_code: int, body, agency_key: str | None) -> None:
    """Levanta exceção se a resposta de /scrape/* indica falha; não faz nada no sucesso.

    Falha: HTTP >= 400, corpo que não é JSON de objeto ou ``status`` failed/partial no corpo
    (o corpo é lido antes do status HTTP: o 500 de "todas falharam" traz os erros).

    - Todos os erros (da agência, se informada) permanentes: ``AirflowFailException``, sem
      retry.
    - O resto (erro passageiro, sem ``errors[]``, API antiga sem ``retryable``):
      ``AirflowException``, que o Airflow repete.
    """
    from airflow.exceptions import AirflowException, AirflowFailException

    body_status = body.get("status") if isinstance(body, dict) else None
    if status_code < 400 and isinstance(body, dict) and body_status not in _FAILED_BODY_STATUSES:
        return

    label = body_status or f"HTTP {status_code}"
    target = agency_key or "EBC"
    errors = _response_errors(body, agency_key)
    if _all_permanent(errors):
        categories = ", ".join(sorted({str(e.get("category")) for e in errors}))
        raise AirflowFailException(
            f"Scraping {label} for {target}: erro permanente ({categories}), sem retry: "
            f"{_describe_errors(errors)}"
        )
    if errors:
        detail = _describe_errors(errors)
    elif body is None:
        detail = "resposta sem JSON (corpo no log da task)"
    else:
        detail = repr(body)[:500]
    raise AirflowException(f"Scraping {label} for {target}: {detail}")


def _scrape_result(agency_key: str, body: dict) -> dict:
    """Resultado da task scrape (XCom) consumido pela summarize: pequeno, com a agência.

    ``articles_new`` é None com uma API antiga, que não devolve o campo.
    """
    articles_new = body.get("articles_new")
    articles_saved = body.get("articles_saved")
    return {
        "agency": agency_key,
        "articles_new": articles_new if isinstance(articles_new, int) else None,
        "articles_saved": articles_saved if isinstance(articles_saved, int) else 0,
    }


_DATE_PARAM_HELP = (
    "Opcional, YYYY-MM-DD, em datas de Brasília. Para recuperar um período depois de uma "
    "queda, informe start_date e end_date juntos: a janela vai explícita e não mexe no "
    "estado da agência. Vazios, a API usa a marca d'água de cada agência (desde o último "
    "sucesso, até 7 dias)."
)
_FORCE_PARAM_HELP = (
    "Ignora o disjuntor: raspa também as agências com o circuito aberto (erro permanente "
    "seguido). Um sucesso fecha o circuito."
)


@dag(
    dag_id="scrape_ebc",
    description="Scrape notícias EBC (Agência Brasil, TV Brasil)",
    schedule="0 6-22/2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "ebc"],
    params={
        "start_date": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Data inicial (recuperação)",
            description=_DATE_PARAM_HELP,
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
            title="Data final (recuperação)",
            description=_DATE_PARAM_HELP,
        ),
        "force": Param(
            False,
            type="boolean",
            title="Ignorar o disjuntor",
            description=_FORCE_PARAM_HELP,
        ),
    },
    default_args={
        "owner": "scraper",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": _on_scrape_failure,
    },
)
def scrape_ebc_dag():
    @task
    def scrape_ebc(**context):
        """Chama Scraper API no Cloud Run para scraping EBC."""
        import google.auth.transport.requests
        import google.oauth2.id_token
        import httpx
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        from airflow.models import Variable

        try:
            payload = _scrape_payload(EBC_SOURCES, context.get("params"))
        except ValueError as e:
            # Params inválidos no disparo manual: repetir não adianta.
            raise AirflowFailException(str(e)) from e

        scraper_api_url = Variable.get("scraper_api_url", default_var="")
        if not scraper_api_url:
            raise ValueError("Missing required Airflow Variable: scraper_api_url")

        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

        window = (
            f"{payload['start_date']} a {payload['end_date']}"
            if "start_date" in payload
            else "marca d'água"
        )
        logger.info(f"Calling scraper API for EBC ({', '.join(EBC_SOURCES)}): janela {window}")

        # Erro de rede/timeout do httpx propaga e o Airflow repete.
        response = httpx.post(
            f"{scraper_api_url}/scrape/ebc",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=900.0,
        )
        # O corpo vem antes do status HTTP: o 500/207 traz os erros e se vale repetir.
        try:
            result = response.json()
        except ValueError:
            result = None
            logger.warning(
                "Scraper API respondeu HTTP %s sem JSON: %s",
                response.status_code,
                str(response.text)[:500],
            )
        else:
            logger.info(
                "Scraper API response (HTTP %s):\n%s",
                response.status_code,
                json.dumps(result, indent=2, ensure_ascii=False),
            )

        # Fonte com o circuito aberto não é raspada e não falha a task. As duas puladas: a
        # task termina como skipped.
        skipped = {s.get("agency") for s in _response_skipped(result)}
        if skipped:
            logger.info("Fontes puladas (circuito aberto): %s", ", ".join(sorted(skipped)))
        if set(EBC_SOURCES) <= skipped:
            raise AirflowSkipException(
                f"EBC: circuito aberto em todas as fontes ({', '.join(EBC_SOURCES)})"
            )

        # Duas fontes (agencia_brasil e tvbrasil): só falha sem retry se todos os erros
        # forem permanentes; se algum for passageiro, repete a task.
        _check_scrape_response(response.status_code, result, None)
        return _scrape_result("ebc", result)

    scrape_ebc()


dag_instance = scrape_ebc_dag()
