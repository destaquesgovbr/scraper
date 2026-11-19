"""Leitura da resposta de /scrape/* nas DAGs de scraping (scrape_agencies e scrape_ebc).

As duas DAGs têm cópias das mesmas funções (cada arquivo de DAG é autocontido): erro
permanente (todos os errors[] com retryable false) falha sem retry; o resto repete.
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


def _airflow_mocks(tasks):
    def fake_dag(**kwargs):
        return lambda fn: fn

    def fake_task(fn=None, **task_kwargs):
        def wrap(f):
            mock_callable = MagicMock(name=f"task_{f.__name__}")
            mock_callable._original_fn = f
            mock_callable.task_kwargs = task_kwargs
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


def _drop(module_name):
    for key in list(sys.modules):
        if key.startswith(module_name):
            del sys.modules[key]


@pytest.fixture(params=["dags.scrape_agencies", "dags.scrape_ebc"])
def dag_module(request):
    _drop(request.param)
    tasks: dict = {}
    mocks = _airflow_mocks(tasks)
    with (
        patch.dict(sys.modules, mocks),
        patch("builtins.open", mock_open(read_data=yaml.dump(SAMPLE_YAML))),
    ):
        module = __import__(request.param, fromlist=["_check_scrape_response"])
    module._test_tasks = tasks
    module._test_mocks = mocks
    with patch.dict(sys.modules, mocks):
        yield module
    _drop(request.param)


def _error(category, retryable, agency="mec"):
    return {"agency": agency, "error": "boom", "category": category, "retryable": retryable}


# =============================================================================
# Funções de leitura da resposta (iguais nas duas DAGs)
# =============================================================================


class TestResponseErrors:
    @pytest.mark.parametrize("body", [None, "texto", [], {"detail": "x"}, {"errors": "x"}])
    def test_non_scrape_body_has_no_errors(self, dag_module, body):
        assert dag_module._response_errors(body) == []

    def test_filters_by_agency_and_drops_non_dict_items(self, dag_module):
        body = {"errors": [_error("html_changed", False), "boom", _error("x", True, "mds")]}
        assert dag_module._response_errors(body, "mec") == [_error("html_changed", False)]
        assert len(dag_module._response_errors(body)) == 2


class TestResponseSkipped:
    @pytest.mark.parametrize("body", [None, "texto", {"detail": "x"}, {"skipped": "x"}])
    def test_body_without_skipped(self, dag_module, body):
        assert dag_module._response_skipped(body) == []

    def test_filters_by_agency(self, dag_module):
        body = {
            "skipped": [
                {"agency": "mec", "reason": "circuit_open", "next_probe_at": "2026-11-19T16:00Z"},
                {"agency": "mds", "reason": "circuit_open"},
                "lixo",
            ]
        }
        assert [s["agency"] for s in dag_module._response_skipped(body)] == ["mec", "mds"]
        assert [s["agency"] for s in dag_module._response_skipped(body, "mec")] == ["mec"]

    def test_skipped_only_body_passes_the_check(self, dag_module):
        """status "skipped" com HTTP 200 não é falha: quem decide o skip é a task."""
        body = {"status": "skipped", "errors": [], "skipped": [{"agency": "mec"}]}
        dag_module._check_scrape_response(200, body, "mec")


class TestAllPermanent:
    def test_empty_is_not_permanent(self, dag_module):
        assert dag_module._all_permanent([]) is False

    def test_all_false_is_permanent(self, dag_module):
        errors = [_error("html_changed", False), _error("url_broken", False, "mds")]
        assert dag_module._all_permanent(errors) is True

    @pytest.mark.parametrize("retryable", [True, None, "false", 0])
    def test_anything_but_false_is_transient(self, dag_module, retryable):
        """Só o booleano False conta: API antiga (sem o campo) repete."""
        errors = [_error("html_changed", False), {"agency": "mds", "retryable": retryable}]
        assert dag_module._all_permanent(errors) is False


class TestCheckScrapeResponse:
    def test_success_does_nothing(self, dag_module):
        dag_module._check_scrape_response(200, {"status": "completed", "errors": []}, None)

    def test_permanent_raises_fail_exception(self, dag_module):
        body = {"status": "failed", "errors": [_error("html_changed", False)]}
        with pytest.raises(AirflowFailException, match="html_changed"):
            dag_module._check_scrape_response(500, body, None)

    def test_partial_with_only_permanent_errors_raises_fail_exception(self, dag_module):
        body = {"status": "partial", "errors": [_error("access_denied", False)]}
        with pytest.raises(AirflowFailException, match="access_denied"):
            dag_module._check_scrape_response(207, body, None)

    @pytest.mark.parametrize(
        ("http", "body"),
        [
            (500, {"status": "failed", "errors": [_error("network_error", True)]}),
            (207, {"status": "partial", "errors": [_error("unknown", True)]}),
            (500, {"status": "failed", "errors": [{"agency": "mec", "error": "x"}]}),
            (500, {"detail": "DB down"}),
            (503, None),
            (200, None),
            (422, {"detail": [{"loc": ["body", "start_date"]}]}),
        ],
    )
    def test_everything_else_raises_retryable_exception(self, dag_module, http, body):
        with pytest.raises(AirflowException) as exc:
            dag_module._check_scrape_response(http, body, None)
        assert not isinstance(exc.value, AirflowFailException)


class TestScrapeResult:
    def test_result_has_agency_and_counts(self, dag_module):
        body = {"articles_new": 2, "articles_saved": 5}
        assert dag_module._scrape_result("mec", body) == {
            "agency": "mec",
            "articles_new": 2,
            "articles_saved": 5,
        }

    def test_old_api_gives_none_articles_new(self, dag_module):
        assert dag_module._scrape_result("mec", {"articles_saved": 5})["articles_new"] is None


# =============================================================================
# Corpo da task scrape_ebc
# =============================================================================


@pytest.fixture
def ebc():
    _drop("dags.scrape_ebc")
    tasks: dict = {}
    mocks = _airflow_mocks(tasks)
    with (
        patch.dict(sys.modules, mocks),
        patch("builtins.open", mock_open(read_data=yaml.dump(SAMPLE_YAML))),
    ):
        module = __import__("dags.scrape_ebc", fromlist=["_check_scrape_response"])
    module._test_tasks = tasks
    module._test_mocks = mocks
    yield module
    _drop("dags.scrape_ebc")


def _run_ebc(module, status_code=200, body=None):
    variable = module._test_mocks["airflow.models"].Variable
    variable.get = MagicMock(return_value="https://scraper.run.app")
    response = MagicMock()
    response.status_code = status_code
    if body is None:
        response.json.side_effect = ValueError("not json")
        response.text = "<html>502</html>"
    else:
        response.json.return_value = body
    with (
        patch.dict(sys.modules, module._test_mocks),
        patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response),
    ):
        return module._test_tasks["scrape_ebc"]._original_fn(logical_date=SIX_AM, params={})


class TestScrapeEbcTask:
    def test_both_sources_skipped_skips_the_task(self, ebc):
        body = {
            "status": "skipped",
            "errors": [],
            "skipped": [
                {"agency": "agencia_brasil", "reason": "circuit_open"},
                {"agency": "tvbrasil", "reason": "circuit_open"},
            ],
        }
        with pytest.raises(AirflowSkipException, match="todas as fontes"):
            _run_ebc(ebc, 200, body)

    def test_one_source_skipped_does_not_fail(self, ebc):
        body = {
            "status": "completed",
            "articles_new": 2,
            "articles_saved": 2,
            "errors": [],
            "skipped": [{"agency": "tvbrasil", "reason": "circuit_open"}],
        }
        assert _run_ebc(ebc, 200, body)["articles_new"] == 2

    def test_one_skipped_and_other_permanent_error_fails_without_retry(self, ebc):
        body = {
            "status": "failed",
            "errors": [_error("html_changed", False, "agencia_brasil")],
            "skipped": [{"agency": "tvbrasil", "reason": "circuit_open"}],
        }
        with pytest.raises(AirflowFailException):
            _run_ebc(ebc, 500, body)

    def test_success_returns_result(self, ebc):
        body = {"status": "completed", "articles_new": 7, "articles_saved": 40, "errors": []}
        assert _run_ebc(ebc, 200, body) == {
            "agency": "ebc",
            "articles_new": 7,
            "articles_saved": 40,
        }

    def test_all_permanent_fails_without_retry(self, ebc):
        body = {
            "status": "failed",
            "errors": [
                _error("unknown_agency", False, "agencia_brasil"),
                _error("html_changed", False, "tvbrasil"),
            ],
        }
        with pytest.raises(AirflowFailException, match="html_changed, unknown_agency"):
            _run_ebc(ebc, 500, body)

    def test_any_transient_error_retries(self, ebc):
        body = {
            "status": "partial",
            "errors": [
                _error("html_changed", False, "agencia_brasil"),
                _error("network_error", True, "tvbrasil"),
            ],
        }
        with pytest.raises(AirflowException, match="EBC") as exc:
            _run_ebc(ebc, 207, body)
        assert not isinstance(exc.value, AirflowFailException)

    def test_non_json_retries(self, ebc):
        with pytest.raises(AirflowException, match="HTTP 502") as exc:
            _run_ebc(ebc, 502, None)
        assert not isinstance(exc.value, AirflowFailException)
