"""DAG para scraping de notícias EBC (Agência Brasil, TV Brasil) via Cloud Run API.

Mesma agenda e mesma janela da ``scrape_agencies``: a cada 2 horas, das 06h às 22h em
horário de Brasília (``0 6-22/2 * * *``), com 9 h para trás. Params opcionais
``start_date``/``end_date`` (YYYY-MM-DD) para recuperar um período depois de uma queda.

Erro permanente (todos os ``errors[]`` com ``retryable: false``) falha sem retry
(``AirflowFailException``); se algum erro é passageiro, o corpo não é JSON ou a API é antiga
(sem ``retryable``), a task repete (2x, a cada 5 min), como antes.

As funções de janela e de leitura da resposta são cópia das de ``scrape_agencies.py`` de
propósito: cada arquivo de DAG é autocontido (não importa outro módulo de DAG).
"""

import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from airflow.decorators import dag, task
from airflow.sdk import Param

logger = logging.getLogger(__name__)


BRASILIA = ZoneInfo("America/Sao_Paulo")

# A execução das 06h precisa cobrir a madrugada sem scraping, desde a execução das 22h do
# dia anterior: 8 h de intervalo + 1 h de folga.
SCRAPE_LOOKBACK_HOURS = 9

# status do corpo de /scrape/ebc que indica falha (a API também responde 500/207).
_FAILED_BODY_STATUSES = ("failed", "partial")

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _scrape_window(
    logical_date: datetime, lookback_hours: int = SCRAPE_LOOKBACK_HOURS
) -> tuple[str, str]:
    """Janela (min_date, max_date) da execução, em datas de Brasília.

    As listagens datam as notícias no horário de Brasília. Em UTC, das 22:00 às 23:59 BRT a
    data já é o dia seguinte e a execução pulava as notícias do dia. A janela vai de
    ``lookback_hours`` antes da execução até a execução. Com 9 h, as execuções das 06h e
    das 08h ainda incluem o dia anterior (o que saiu depois da execução das 22h); a partir
    das 10h, só o dia corrente. Datetime sem fuso é tratado como UTC.

    No Airflow 3 o cron usa o CronTriggerTimetable (``create_cron_data_intervals = False``,
    o padrão): a logical_date é o próprio horário agendado (06:00, 08:00...), não o início
    do intervalo anterior.
    """
    # O logical_date do Airflow é pendulum.DateTime, cuja aritmética e astimezone não
    # preservam o fuso de um ZoneInfo (em produção a janela saiu invertida). Só o instante
    # (timestamp) é usado, convertido para um datetime da stdlib em Brasília.
    if logical_date.tzinfo is None:
        # timezone.utc (não datetime.UTC, 3.11+): o arquivo roda no Python do Composer.
        logical_date = logical_date.replace(tzinfo=timezone.utc)  # noqa: UP017
    local = datetime.fromtimestamp(logical_date.timestamp(), tz=BRASILIA)
    start = local - timedelta(hours=lookback_hours)
    return start.strftime("%Y-%m-%d"), local.strftime("%Y-%m-%d")


def _parse_param_date(value, name: str) -> date:
    """Valida um param de data no formato YYYY-MM-DD."""
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise ValueError(f"{name} deve estar no formato YYYY-MM-DD: {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as e:
        raise ValueError(f"{name} não é uma data válida: {value!r}") from e


def _resolve_window(logical_date: datetime, params=None) -> tuple[str, str]:
    """Janela da execução: a dos params ``start_date``/``end_date`` ou a de ``_scrape_window``.

    Os params servem para recuperar um período depois de uma queda (disparo manual). Só
    valem juntos; vazios ou ausentes, vale a janela calculada. Erro de formato, só um dos
    dois ou início depois do fim levantam ValueError.
    """
    params = params or {}
    start = params.get("start_date") or None
    end = params.get("end_date") or None
    if start is None and end is None:
        return _scrape_window(logical_date)
    if start is None or end is None:
        raise ValueError("Informe start_date e end_date juntos (YYYY-MM-DD), ou nenhum dos dois")
    start_date = _parse_param_date(start, "start_date")
    end_date = _parse_param_date(end, "end_date")
    if start_date > end_date:
        raise ValueError(f"start_date ({start}) é posterior a end_date ({end})")
    return start_date.isoformat(), end_date.isoformat()


def _run_instant(context) -> datetime:
    """Instante de referência da execução para a janela.

    Execução agendada: a logical_date (o horário do cron). Disparo manual pela CLI/API do
    Airflow 3 pode vir sem logical_date: vale o run_after do DagRun ou, na falta, agora.
    """
    logical_date = context.get("logical_date")
    if logical_date is not None:
        return logical_date
    run_after = getattr(context.get("dag_run"), "run_after", None)
    if run_after is not None:
        return run_after
    return datetime.now(timezone.utc)  # noqa: UP017


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
    "queda, informe start_date e end_date juntos: eles substituem a janela calculada. "
    "Vazios, vale a janela normal (9 h antes da execução até a execução)."
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
        from airflow.exceptions import AirflowFailException
        from airflow.models import Variable

        try:
            min_date, max_date = _resolve_window(_run_instant(context), context.get("params"))
        except ValueError as e:
            # Params inválidos no disparo manual: repetir não adianta.
            raise AirflowFailException(str(e)) from e

        scraper_api_url = Variable.get("scraper_api_url", default_var="")
        if not scraper_api_url:
            raise ValueError("Missing required Airflow Variable: scraper_api_url")

        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

        logger.info(f"Calling scraper API for EBC: {min_date} to {max_date}")

        # Erro de rede/timeout do httpx propaga e o Airflow repete.
        response = httpx.post(
            f"{scraper_api_url}/scrape/ebc",
            json={
                "start_date": min_date,
                "end_date": max_date,
                "allow_update": False,
                "sequential": True,
            },
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

        # Duas fontes (agencia_brasil e tvbrasil): só falha sem retry se todos os erros
        # forem permanentes; se algum for passageiro, repete a task.
        _check_scrape_response(response.status_code, result, None)
        return _scrape_result("ebc", result)

    scrape_ebc()


dag_instance = scrape_ebc_dag()
