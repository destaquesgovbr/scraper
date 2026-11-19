"""
Janela, payload e agenda das DAGs de scraping (scrape_agencies e scrape_ebc).

Desde o Passo 2 da rearquitetura da ingestão a DAG não calcula mais a janela: sem datas no
corpo, a API usa a marca d'água de cada agência (scrape_agency_state: desde o último
sucesso, até 7 dias ou ~80 notícias da agência). Os params start_date/end_date continuam para recuperar um período
(janela explícita, que não mexe no estado) e o param force ignora o disjuntor.
"""

import sys
import types
from datetime import UTC, datetime
from unittest.mock import MagicMock, mock_open, patch

import pytest
import yaml

SAMPLE_YAML = {"agencies": {"mec": {"url": "https://www.gov.br/mec/pt-br/noticias"}}}
SIX_AM = datetime(2026, 11, 17, 9, 0, tzinfo=UTC)


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class AirflowException(Exception):
    pass


class AirflowFailException(AirflowException):
    pass


class AirflowSkipException(AirflowException):
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
    exceptions.AirflowSkipException = AirflowSkipException

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
        module = __import__(module_name, fromlist=["_resolve_window"])
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


def test_window_is_no_longer_computed_by_the_dag(dag_module):
    """_scrape_window e o lookback de 9 h saíram: a janela é da API (marca d'água)."""
    for name in ("_scrape_window", "SCRAPE_LOOKBACK_HOURS", "_run_instant", "BRASILIA"):
        assert not hasattr(dag_module, name)


# =============================================================================
# Params start_date/end_date (recuperação manual) e force
# =============================================================================


@pytest.mark.parametrize(
    "params",
    [None, {}, {"start_date": None, "end_date": None}, {"start_date": "", "end_date": ""}],
)
def test_resolve_window_sem_params_e_marca_dagua(dag_module, params):
    assert dag_module._resolve_window(params) is None


def test_resolve_window_params_explicitos(dag_module):
    params = {"start_date": "2026-11-10", "end_date": "2026-11-12"}
    assert dag_module._resolve_window(params) == ("2026-11-10", "2026-11-12")


def test_resolve_window_aceita_um_dia_so(dag_module):
    params = {"start_date": "2026-11-10", "end_date": "2026-11-10"}
    assert dag_module._resolve_window(params) == ("2026-11-10", "2026-11-10")


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
        dag_module._resolve_window(params)


def test_payload_padrao_sem_datas(dag_module):
    assert dag_module._scrape_payload(["mec"], {}) == {
        "agencies": ["mec"],
        "allow_update": False,
        "sequential": True,
        "force": False,
    }


def test_payload_com_janela_e_force(dag_module):
    params = {"start_date": "2026-11-01", "end_date": "2026-11-02", "force": True}
    assert dag_module._scrape_payload(["mec"], params) == {
        "agencies": ["mec"],
        "allow_update": False,
        "sequential": True,
        "force": True,
        "start_date": "2026-11-01",
        "end_date": "2026-11-02",
    }


@pytest.mark.parametrize("force", [None, False, "true", 1])
def test_payload_force_so_com_booleano_true(dag_module, force):
    assert dag_module._scrape_payload(["mec"], {"force": force})["force"] is False


# =============================================================================
# Agenda e params das duas DAGs
# =============================================================================


def test_agenda_de_2_em_2_horas_das_06h_as_22h(dag_module):
    (kwargs,) = dag_module._test_dag_calls
    assert kwargs["schedule"] == "0 6-22/2 * * *"
    assert kwargs["catchup"] is False
    assert kwargs["max_active_runs"] == 1


def test_params_opcionais_de_recuperacao_e_force(dag_module):
    (kwargs,) = dag_module._test_dag_calls
    params = kwargs["params"]
    assert set(params) == {"start_date", "end_date", "force"}
    for name in ("start_date", "end_date"):
        assert params[name].value is None
        assert params[name].schema["type"] == ["null", "string"]
        assert params[name].schema["format"] == "date"
        assert "marca d'água" in params[name].schema["description"]
    assert params["force"].value is False
    assert params["force"].schema["type"] == "boolean"
    assert "disjuntor" in params["force"].schema["description"]


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


def _run_ebc(module, context, agency_key="agencia_brasil"):
    variable = module._test_mocks["airflow.models"].Variable
    variable.get = MagicMock(return_value="https://scraper.run.app")
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"status": "completed", "articles_saved": 0, "errors": []}
    module._test_mocks["airflow.sdk"].get_current_context = MagicMock(return_value=context)
    with (
        patch.dict(sys.modules, module._test_mocks),
        patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response) as post,
    ):
        module._test_tasks["scrape_ebc"]._original_fn(agency_key)
    return post


@pytest.mark.parametrize("source", ["agencia_brasil", "tvbrasil"])
def test_ebc_uma_chamada_por_fonte_sem_datas(ebc, source):
    """Cada fonte na própria requisição, com o próprio limite de 900 s do Cloud Run."""
    context = {"logical_date": SIX_AM, "params": {}}
    post = _run_ebc(ebc, context, source)
    assert post.call_args[0][0] == "https://scraper.run.app/scrape/ebc"
    assert post.call_args[1]["json"] == {
        "agencies": [source],
        "allow_update": False,
        "sequential": True,
        "force": False,
    }
    assert post.call_args[1]["timeout"] == 900.0
    assert context["agency_key"] == source  # map_index_template


def test_ebc_timeout_da_task_logo_acima_do_limite_da_requisicao(ebc):
    (kwargs,) = ebc._test_dag_calls
    timeout = kwargs["default_args"]["execution_timeout"]
    assert ebc._API_TIMEOUT_SECONDS < timeout.total_seconds() <= ebc._API_TIMEOUT_SECONDS + 120


def test_ebc_params_vao_explicitos(ebc):
    params = {"start_date": "2026-11-01", "end_date": "2026-11-02", "force": True}
    post = _run_ebc(ebc, {"logical_date": SIX_AM, "params": params})
    payload = post.call_args[1]["json"]
    assert (payload["start_date"], payload["end_date"]) == ("2026-11-01", "2026-11-02")
    assert payload["force"] is True


def test_ebc_params_invalidos_falham_sem_retry(ebc):
    with pytest.raises(AirflowFailException):
        _run_ebc(ebc, {"logical_date": SIX_AM, "params": {"end_date": "2026-11-02"}})


def test_ajuda_do_force_diz_que_com_datas_o_estado_nao_muda(dag_module):
    (kwargs,) = dag_module._test_dag_calls
    help_text = kwargs["params"]["force"].schema["description"]
    assert "Sem datas, um sucesso fecha o circuito" in help_text
    assert "start_date/end_date o estado da agência não muda" in help_text
