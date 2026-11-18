"""
DAG única de scraping das agências gov.br: ``scrape_agencies``.

- Roda a cada 2 horas, das 06h às 22h (``0 6-22/2 * * *``, 9 execuções por dia). O cron é
  avaliado no fuso padrão do Airflow (``core.default_timezone = America/Sao_Paulo`` no
  Composer), ou seja, em horário de Brasília.
- Uma task mapeada (``scrape``) por agência ativa do ``config/site_urls.yaml``, com a sigla
  da agência como map index na UI (``map_index_template``).
- No máximo ``MAX_CONCURRENT_AGENCIES`` agências ao mesmo tempo (``max_active_tis_per_dag``;
  env ``SCRAPE_MAX_CONCURRENT_AGENCIES`` lido no parse, padrão 4): protege o www.gov.br (as
  147 agências estão no mesmo host) e espalha a rajada de eventos no pipeline downstream.
- Cada task chama a Scraper API no Cloud Run via HTTP, com o mesmo payload de antes.
- Erro permanente (todos os ``errors[]`` da agência com ``retryable: false``: url_broken,
  access_denied, html_changed, anti_bot, unknown_agency) falha sem retry
  (``AirflowFailException``). Erro passageiro, corpo que não é JSON e API antiga sem
  ``retryable`` repetem: 2x, a cada 5 min (intervalo fixo; ver default_args). Timeout: 10 min
  por task.
- Task final ``summarize`` (``trigger_rule="all_done"``, única folha): registra as agências
  que falharam e só derruba a execução se nenhuma funcionou ou se a fração de falhas passou
  da Variable ``scraper_max_failed_ratio`` (padrão 0,10). Vermelho = problema de plataforma;
  a agência quebrada aparece na própria task mapeada.
- Janela: ver ``_scrape_window`` (9 h para trás, para a execução das 06h cobrir a madrugada).
- Params opcionais ``start_date``/``end_date`` (YYYY-MM-DD) para recuperar um período depois
  de uma queda (disparo manual; ver docs/runbook.md).

Substitui as ~147 DAGs ``scrape_<agencia>`` (uma por agência, a cada 10 min), que eram 96,7%
das task runs do Composer (infra#223). Com uma DAG só, um DagRun zumbi deixa de parar uma
agência isolada em silêncio (caso do scrape_esg, sem execução desde 11/11/2026): se acontecer,
para todas as agências, e o monitor_scraping_health acusa na execução seguinte.
"""

import json
import logging
import os
import re
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import yaml
from airflow.decorators import dag, task
from airflow.sdk import Param

logger = logging.getLogger(__name__)


BRASILIA = ZoneInfo("America/Sao_Paulo")

# A execução das 06h precisa cobrir a madrugada sem scraping, desde a execução das 22h do
# dia anterior: 8 h de intervalo + 1 h de folga.
SCRAPE_LOOKBACK_HOURS = 9

# Agências raspadas ao mesmo tempo dentro de uma execução (max_active_tis_per_dag).
DEFAULT_MAX_CONCURRENT_AGENCIES = 4

# Fração de agências com falha acima da qual a summarize derruba a execução (Variable
# scraper_max_failed_ratio, entre 0 e 1).
DEFAULT_MAX_FAILED_RATIO = 0.10

# status do corpo de /scrape/agencies que indica falha (a API também responde 500/207).
_FAILED_BODY_STATUSES = ("failed", "partial")

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _max_concurrent_agencies(raw: str | None) -> int:
    """Limite de agências simultâneas a partir do env ``SCRAPE_MAX_CONCURRENT_AGENCIES``.

    Lido no parse: ausente ou inválido vale 4 (um ValueError no import sumiria com a DAG, e o
    import check do CI não pega); 0 ou negativo vira 1, porque com 0 o scheduler nunca roda
    nenhuma task mapeada e, com max_active_runs=1, a DAG para.
    """
    if raw is None or not raw.strip():
        return DEFAULT_MAX_CONCURRENT_AGENCIES
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "SCRAPE_MAX_CONCURRENT_AGENCIES inválido (%r): usando %d",
            raw,
            DEFAULT_MAX_CONCURRENT_AGENCIES,
        )
        return DEFAULT_MAX_CONCURRENT_AGENCIES
    return max(1, value)


MAX_CONCURRENT_AGENCIES = _max_concurrent_agencies(os.environ.get("SCRAPE_MAX_CONCURRENT_AGENCIES"))


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
    """Callback para log estruturado de falha de uma task de scraping (com a agência)."""
    ti = context.get("task_instance")
    dag_id = ti.dag_id if ti else "unknown"
    task_id = ti.task_id if ti else "unknown"
    map_index = getattr(ti, "map_index", -1) if ti else -1
    # A task grava a sigla no contexto (map_index_template); o rendered_map_index é o
    # mesmo valor, já renderizado pelo Airflow.
    agency = context.get("agency_key") or (getattr(ti, "rendered_map_index", None) if ti else None)
    exception = context.get("exception", "unknown")
    try_number = ti.try_number if ti else 0
    logger.error(
        "Scrape DAG failure: dag=%s task=%s agency=%s map_index=%s try=%d error=%s",
        dag_id,
        task_id,
        agency or "unknown",
        map_index,
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
    target = agency_key or "todas"
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


def _parse_max_failed_ratio(raw) -> float:
    """Variable ``scraper_max_failed_ratio`` como fração entre 0 e 1; inválida vale 0,10."""
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = float("nan")
    if not 0 <= value <= 1:  # NaN também cai aqui
        logger.warning(
            "scraper_max_failed_ratio inválida (%r): usando %.2f", raw, DEFAULT_MAX_FAILED_RATIO
        )
        return DEFAULT_MAX_FAILED_RATIO
    return value


def _summarize_results(results, expected_agencies) -> dict:
    """Agrega os resultados da task mapeada scrape.

    ``results`` é o LazyXComSequence do Airflow: só tem os resultados das tasks que deram
    certo, compactados (a posição não é o map_index). Por isso cada resultado leva a sigla, e
    as falhas são as esperadas que não aparecem. Só iteração: fatiar (``results[:]``) levanta
    TypeError no Airflow 3.0.1.
    """
    ok: set[str] = set()
    articles_new = 0
    articles_saved = 0
    without_new: set[str] = set()
    for item in results:
        if not isinstance(item, dict) or not item.get("agency"):
            continue
        agency = item["agency"]
        ok.add(agency)
        if isinstance(item.get("articles_new"), int):
            articles_new += item["articles_new"]
        else:
            without_new.add(agency)
        if isinstance(item.get("articles_saved"), int):
            articles_saved += item["articles_saved"]
    expected = set(expected_agencies)
    failed = sorted(expected - ok)
    total = len(expected)
    return {
        "total": total,
        "ok": len(ok & expected),
        "failed": failed,
        "failed_ratio": round(len(failed) / total, 4) if total else 1.0,
        "articles_new": articles_new,
        "articles_saved": articles_saved,
        "agencies_without_articles_new": len(without_new),
    }


def _summary_failure(summary: dict, max_failed_ratio: float) -> str | None:
    """Motivo para derrubar a execução, ou None se ela fica verde."""
    if summary["ok"] == 0:
        return f"Nenhuma agência raspada com sucesso ({summary['total']} esperadas)"
    if summary["failed_ratio"] > max_failed_ratio:
        return (
            f"{len(summary['failed'])} de {summary['total']} agências falharam "
            f"({summary['failed_ratio']:.0%}, acima do limite de {max_failed_ratio:.0%}): "
            f"{', '.join(summary['failed'])}"
        )
    return None


def _read_agencies() -> dict:
    """Lê todas as agências do YAML, ativas e inativas: {agency_key: config}."""
    config_path = os.path.join(os.path.dirname(__file__), "config", "site_urls.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)["agencies"]


def _active_agencies(agencies: dict) -> dict:
    """Filtra as agências ativas (``active`` ausente conta como True): {agency_key: url}."""
    return {key: data.get("url") for key, data in agencies.items() if data.get("active", True)}


def _load_agencies_config() -> dict:
    """Carrega config de agências ativas do YAML.

    Suporta formato dicionário com campos:
    - url: str (obrigatório)
    - active: bool (opcional, default: True)
    - disabled_reason: str (opcional)
    - disabled_date: str (opcional)

    Returns:
        dict: Mapeamento {agency_key: url} apenas para agências ativas
    """
    return _active_agencies(_read_agencies())


_DATE_PARAM_HELP = (
    "Opcional, YYYY-MM-DD, em datas de Brasília. Para recuperar um período depois de uma "
    "queda, informe start_date e end_date juntos: eles substituem a janela calculada. "
    "Vazios, vale a janela normal (9 h antes da execução até a execução)."
)


@dag(
    dag_id="scrape_agencies",
    description="Scrape notícias das agências gov.br ativas (uma task mapeada por agência)",
    schedule="0 6-22/2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["scraper", "govbr"],
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
        # Intervalo fixo, SEM retry_exponential_backoff: no Airflow 3.1.0 (Composer), o
        # scheduler calcula a próxima tentativa de uma task MAPEADA em retry lendo
        # task.max_retry_delay, que o MappedOperator não tem. O AttributeError derruba o
        # loop do scheduler inteiro (todas as DAGs paradas de 18/11 02:00Z a 09:00Z).
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": _on_scrape_failure,
    },
)
def scrape_agencies_dag():
    @task(map_index_template="{{ agency_key }}", max_active_tis_per_dag=MAX_CONCURRENT_AGENCIES)
    def scrape(agency_key: str):
        """Chama a Scraper API no Cloud Run para a agência desta task mapeada."""
        import google.auth.transport.requests
        import google.oauth2.id_token
        import httpx
        from airflow.exceptions import AirflowFailException
        from airflow.models import Variable
        from airflow.sdk import get_current_context

        context = get_current_context()
        # Renderiza o map_index_template: a UI mostra a sigla no lugar do índice. Vem antes
        # de tudo para a task que falhar também aparecer com a sigla.
        context["agency_key"] = agency_key

        try:
            min_date, max_date = _resolve_window(_run_instant(context), context.get("params"))
        except ValueError as e:
            # Params inválidos no disparo manual: repetir não adianta.
            raise AirflowFailException(str(e)) from e

        scraper_api_url = Variable.get("scraper_api_url", default_var="")
        if not scraper_api_url:
            raise ValueError("Missing required Airflow Variable: scraper_api_url")

        # Token IAM para autenticação no Cloud Run
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, scraper_api_url)

        logger.info(f"Calling scraper API for {agency_key}: {min_date} to {max_date}")

        # Erro de rede/timeout do httpx propaga e o Airflow repete.
        response = httpx.post(
            f"{scraper_api_url}/scrape/agencies",
            json={
                "start_date": min_date,
                "end_date": max_date,
                "agencies": [agency_key],
                "allow_update": False,
                "sequential": True,
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=900.0,
        )
        # O corpo vem antes do status HTTP: o 500 de "todas falharam" diz se vale repetir.
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

        # Erro permanente: AirflowFailException (sem retry). O resto: AirflowException.
        _check_scrape_response(response.status_code, result, agency_key)
        return _scrape_result(agency_key, result)

    # retries=0 e trigger_rule all_done: roda depois de todas as agências, com ou sem falha, e
    # é a única folha, então decide sozinha a cor da execução.
    @task(trigger_rule="all_done", retries=0)
    def summarize(results, expected_agencies):
        """Resume a execução e só falha se nenhuma agência funcionou ou se muitas falharam."""
        from airflow.exceptions import AirflowFailException
        from airflow.models import Variable

        summary = _summarize_results(results, expected_agencies)
        max_failed_ratio = _parse_max_failed_ratio(
            Variable.get("scraper_max_failed_ratio", default_var="0.10")
        )
        logger.info(
            "Resumo scrape_agencies: %d/%d agências OK, %d com falha (%.1f%%, limite %.0f%%); "
            "%d notícias novas, %d linhas gravadas",
            summary["ok"],
            summary["total"],
            len(summary["failed"]),
            summary["failed_ratio"] * 100,
            max_failed_ratio * 100,
            summary["articles_new"],
            summary["articles_saved"],
        )
        if summary["failed"]:
            logger.warning("Agências com falha: %s", ", ".join(summary["failed"]))
        if summary["agencies_without_articles_new"]:
            logger.info(
                "%d agência(s) sem articles_new na resposta (API anterior ao campo)",
                summary["agencies_without_articles_new"],
            )

        reason = _summary_failure(summary, max_failed_ratio)
        if reason:
            raise AirflowFailException(reason)
        return summary

    # Lista calculada no parse: incluir ou desativar uma agência no YAML muda as tasks
    # mapeadas da próxima execução, sem criar nem remover DAGs. A summarize recebe a mesma
    # lista (serializada com a DAG) para saber quais agências faltaram.
    scraped = scrape.expand(agency_key=ACTIVE_AGENCY_KEYS)
    summarize(scraped, ACTIVE_AGENCY_KEYS)


ACTIVE_AGENCY_KEYS = sorted(_load_agencies_config())
dag_instance = scrape_agencies_dag()
