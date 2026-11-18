"""
Janela de datas e agenda das DAGs de scraping (scrape_agencies e scrape_ebc).

As listagens gov.br/EBC datam as notícias no horário de Brasília, então a janela é
calculada em America/Sao_Paulo (com a janela em UTC, das 22:00 às 23:59 BRT a data já era
o dia seguinte e as notícias do dia ficavam de fora; nos 30 dias até 02/11/2026 não havia
nenhum artigo gov.br publicado entre 22h e 23h59 BRT).

Desde a DAG única de 2 em 2 horas (06h às 22h BRT), a janela volta 9 horas: a execução
das 06h cobre a madrugada sem scraping desde a das 22h do dia anterior (8 h + 1 h de folga).
"""

import sys
import types
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, mock_open, patch
from zoneinfo import ZoneInfo

import pytest
import yaml

SAMPLE_YAML = {"agencies": {"mec": {"url": "https://www.gov.br/mec/pt-br/noticias"}}}
BRT = ZoneInfo("America/Sao_Paulo")


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class AirflowException(Exception):
    pass


class AirflowFailException(AirflowException):
    pass


def _airflow_mocks(dag_calls, tasks):
    def fake_dag(**kwargs):
        dag_calls.append(kwargs)

        def wrapper(fn):
            return fn

        return wrapper

    def fake_task(fn=None, **task_kwargs):
        def wrap(f):
            mock_callable = MagicMock(name=f"task_{f.__name__}")
            mock_callable._original_fn = f
            tasks[f.__name__] = mock_callable
            return mock_callable

        return wrap(fn) if callable(fn) else wrap

    decorators = MagicMock()
    decorators.dag = fake_dag
    decorators.task = fake_task

    sdk = types.ModuleType("airflow.sdk")
    sdk.Param = FakeParam
    sdk.get_current_context = MagicMock(return_value={})

    exceptions = types.ModuleType("airflow.exceptions")
    exceptions.AirflowException = AirflowException
    exceptions.AirflowFailException = AirflowFailException

    return {
        "airflow": MagicMock(),
        "airflow.decorators": decorators,
        "airflow.models": MagicMock(),
        "airflow.exceptions": exceptions,
        "airflow.sdk": sdk,
    }


def _load(module_name):
    for key in list(sys.modules):
        if key.startswith(module_name):
            del sys.modules[key]
    dag_calls: list[dict] = []
    tasks: dict = {}
    mocks = _airflow_mocks(dag_calls, tasks)
    with (
        patch.dict(sys.modules, mocks),
        patch("builtins.open", mock_open(read_data=yaml.dump(SAMPLE_YAML))),
    ):
        module = __import__(module_name, fromlist=["_scrape_window"])
    module._test_dag_calls = dag_calls
    module._test_tasks = tasks
    module._test_mocks = mocks
    return module


@pytest.fixture(params=["dags.scrape_agencies", "dags.scrape_ebc"])
def dag_module(request):
    module = _load(request.param)
    yield module
    for key in list(sys.modules):
        if key.startswith(request.param):
            del sys.modules[key]


def _brt(day, hour, minute=0):
    return datetime(2026, 11, day, hour, minute, tzinfo=BRT)


def test_lookback_is_nine_hours(dag_module):
    assert dag_module.SCRAPE_LOOKBACK_HOURS == 9


@pytest.mark.parametrize(
    ("instante", "esperado"),
    [
        # 06:00 BRT de 17/11 (primeira execução do dia): volta às 21h de 16/11
        (_brt(17, 6), ("2026-11-16", "2026-11-17")),
        # 08:00 BRT: 23h de 16/11, ainda inclui o dia anterior
        (_brt(17, 8), ("2026-11-16", "2026-11-17")),
        # 10:00 BRT: 01h de 17/11, só o dia corrente
        (_brt(17, 10), ("2026-11-17", "2026-11-17")),
        (_brt(17, 14), ("2026-11-17", "2026-11-17")),
        # 22:00 BRT (última execução): em UTC já seria 18/11
        (_brt(17, 22), ("2026-11-17", "2026-11-17")),
        # Disparo manual na madrugada
        (_brt(17, 3, 30), ("2026-11-16", "2026-11-17")),
    ],
)
def test_scrape_window_volta_9h_em_horario_de_brasilia(dag_module, instante, esperado):
    assert dag_module._scrape_window(instante.astimezone(UTC)) == esperado
    assert dag_module._scrape_window(instante) == esperado


def test_scrape_window_aceita_lookback_explicito(dag_module):
    # 00:30 BRT com 1 h: a sobreposição antiga ainda pega o fim do dia anterior
    assert dag_module._scrape_window(_brt(3, 0, 30), lookback_hours=1) == (
        "2026-11-02",
        "2026-11-03",
    )
    assert dag_module._scrape_window(_brt(3, 1, 30), lookback_hours=1) == (
        "2026-11-03",
        "2026-11-03",
    )


@pytest.mark.parametrize(
    ("ingenuo", "esperado"),
    [
        # 09:00 sem fuso = 09:00 UTC = 06:00 BRT
        (datetime(2026, 11, 17, 9, 0), ("2026-11-16", "2026-11-17")),
        # 01:00 UTC de 18/11 = 22:00 BRT de 17/11 (se fosse BRT, a janela seria 18/11)
        (datetime(2026, 11, 18, 1, 0), ("2026-11-17", "2026-11-17")),
    ],
)
def test_scrape_window_trata_datetime_ingenuo_como_utc(dag_module, ingenuo, esperado):
    assert dag_module._scrape_window(ingenuo) == esperado


class _PendulumLike:
    """Imita o logical_date do Airflow (pendulum.DateTime): a aritmética e o astimezone do
    pendulum não preservam o fuso de um ZoneInfo como o datetime da stdlib. Em produção
    (03/11/2026, 01:42Z) isso inverteu a janela ("from 2026-11-03 to 2026-11-02"). A
    função só pode depender do instante (timestamp) e do tzinfo."""

    def __init__(self, dt):
        self._dt = dt
        self.tzinfo = dt.tzinfo

    def timestamp(self):
        return self._dt.timestamp()

    def astimezone(self, tz=None):
        raise AssertionError("não use astimezone do objeto do Airflow")

    def __sub__(self, other):
        raise AssertionError("não use a aritmética do objeto do Airflow")

    def strftime(self, fmt):
        raise AssertionError("não formate o objeto do Airflow diretamente")


@pytest.mark.parametrize(
    ("instante", "esperado"),
    [
        (datetime(2026, 11, 17, 9, 0, tzinfo=UTC), ("2026-11-16", "2026-11-17")),
        (datetime(2026, 11, 17, 17, 0, tzinfo=UTC), ("2026-11-17", "2026-11-17")),
        (datetime(2026, 11, 18, 1, 0, tzinfo=UTC), ("2026-11-17", "2026-11-17")),
    ],
)
def test_scrape_window_nao_depende_da_aritmetica_do_pendulum(dag_module, instante, esperado):
    assert dag_module._scrape_window(_PendulumLike(instante)) == esperado


# =============================================================================
# Params start_date/end_date (recuperação manual depois de uma queda)
# =============================================================================

SIX_AM = datetime(2026, 11, 17, 9, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "params",
    [None, {}, {"start_date": None, "end_date": None}, {"start_date": "", "end_date": ""}],
)
def test_resolve_window_sem_params_usa_janela_calculada(dag_module, params):
    assert dag_module._resolve_window(SIX_AM, params) == ("2026-11-16", "2026-11-17")


def test_resolve_window_params_substituem_a_janela(dag_module):
    params = {"start_date": "2026-11-10", "end_date": "2026-11-12"}
    assert dag_module._resolve_window(SIX_AM, params) == ("2026-11-10", "2026-11-12")


def test_resolve_window_aceita_um_dia_so(dag_module):
    params = {"start_date": "2026-11-10", "end_date": "2026-11-10"}
    assert dag_module._resolve_window(SIX_AM, params) == ("2026-11-10", "2026-11-10")


@pytest.mark.parametrize(
    ("params", "erro"),
    [
        ({"start_date": "2026-11-10"}, "juntos"),
        ({"end_date": "2026-11-10"}, "juntos"),
        ({"start_date": "10/11/2026", "end_date": "2026-11-12"}, "YYYY-MM-DD"),
        ({"start_date": "2026-11-10", "end_date": "20261112"}, "YYYY-MM-DD"),
        ({"start_date": "2026-02-30", "end_date": "2026-03-01"}, "data válida"),
        ({"start_date": "2026-11-12", "end_date": "2026-11-10"}, "posterior"),
    ],
)
def test_resolve_window_valida_params(dag_module, params, erro):
    with pytest.raises(ValueError, match=erro):
        dag_module._resolve_window(SIX_AM, params)


def test_run_instant_usa_logical_date(dag_module):
    assert dag_module._run_instant({"logical_date": SIX_AM}) == SIX_AM


def test_run_instant_sem_logical_date_usa_run_after(dag_module):
    """Disparo manual pela CLI/API do Airflow 3 pode vir sem logical_date."""
    dag_run = MagicMock(run_after=SIX_AM)
    assert dag_module._run_instant({"logical_date": None, "dag_run": dag_run}) == SIX_AM


def test_run_instant_sem_nada_usa_agora_com_fuso(dag_module):
    instante = dag_module._run_instant({})
    assert instante.tzinfo is not None
    assert abs(datetime.now(UTC) - instante) < timedelta(minutes=1)


# =============================================================================
# Agenda e params das duas DAGs
# =============================================================================


def test_agenda_de_2_em_2_horas_das_06h_as_22h(dag_module):
    (kwargs,) = dag_module._test_dag_calls
    assert kwargs["schedule"] == "0 6-22/2 * * *"
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1


def test_params_opcionais_de_recuperacao(dag_module):
    (kwargs,) = dag_module._test_dag_calls
    params = kwargs["params"]
    assert set(params) == {"start_date", "end_date"}
    for param in params.values():
        assert param.value is None
        assert param.schema["type"] == ["null", "string"]
        assert param.schema["format"] == "date"


# =============================================================================
# Corpo da task scrape_ebc
# =============================================================================


@pytest.fixture
def ebc():
    module = _load("dags.scrape_ebc")
    yield module
    for key in list(sys.modules):
        if key.startswith("dags.scrape_ebc"):
            del sys.modules[key]


def _run_ebc(module, context):
    variable = module._test_mocks["airflow.models"].Variable
    variable.get = MagicMock(return_value="https://scraper.run.app")
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"status": "completed", "articles_saved": 0, "errors": []}
    with (
        patch.dict(sys.modules, module._test_mocks),
        patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response) as post,
    ):
        module._test_tasks["scrape_ebc"]._original_fn(**context)
    return post


def test_ebc_usa_janela_de_9h(ebc):
    post = _run_ebc(ebc, {"logical_date": SIX_AM, "params": {}})
    assert post.call_args[0][0] == "https://scraper.run.app/scrape/ebc"
    payload = post.call_args[1]["json"]
    assert payload == {
        "start_date": "2026-11-16",
        "end_date": "2026-11-17",
        "allow_update": False,
        "sequential": True,
    }


def test_ebc_params_substituem_a_janela(ebc):
    params = {"start_date": "2026-11-01", "end_date": "2026-11-02"}
    post = _run_ebc(ebc, {"logical_date": SIX_AM, "params": params})
    payload = post.call_args[1]["json"]
    assert (payload["start_date"], payload["end_date"]) == ("2026-11-01", "2026-11-02")


def test_ebc_params_invalidos_falham_sem_retry(ebc):
    with pytest.raises(AirflowFailException):
        _run_ebc(ebc, {"logical_date": SIX_AM, "params": {"end_date": "2026-11-02"}})
