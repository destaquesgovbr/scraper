"""
DAG única de scraping das agências gov.br: ``scrape_agencies``.

- Roda a cada 2 horas, das 06h às 22h (``0 6-22/2 * * *``, 9 execuções por dia). O cron é
  avaliado no fuso padrão do Airflow (``core.default_timezone = America/Sao_Paulo`` no
  Composer), ou seja, em horário de Brasília.
- Uma task mapeada (``scrape``) por agência ativa do ``config/site_urls.yaml``, com a sigla
  da agência como map index na UI (``map_index_template``).
- No máximo 4 agências ao mesmo tempo (``max_active_tis_per_dag=4``): espalha a carga no
  scraper-api e a rajada de eventos no pipeline downstream (Pub/Sub, enrichment-worker).
- Cada task chama a Scraper API no Cloud Run via HTTP, com o mesmo payload de antes.
- Retry: 2x, a cada 5 min (intervalo fixo; ver default_args). Timeout: 10 min por task.
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

# Agências raspadas ao mesmo tempo dentro de uma execução.
MAX_CONCURRENT_AGENCIES = 4

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
        from airflow.exceptions import AirflowException, AirflowFailException
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
        response.raise_for_status()
        result = response.json()
        logger.info("Scraper API response:\n%s", json.dumps(result, indent=2, ensure_ascii=False))

        # Verificar status da resposta (API pode retornar 200 com falha lógica)
        if result.get("status") in ("failed", "partial"):
            errors = result.get("errors", [])
            raise AirflowException(f"Scraping {result['status']} for {agency_key}: {errors}")

    # Lista calculada no parse: incluir ou desativar uma agência no YAML muda as tasks
    # mapeadas da próxima execução, sem criar nem remover DAGs.
    scrape.expand(agency_key=ACTIVE_AGENCY_KEYS)


ACTIVE_AGENCY_KEYS = sorted(_load_agencies_config())
dag_instance = scrape_agencies_dag()
