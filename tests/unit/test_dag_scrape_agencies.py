"""Tests for dags/scrape_agencies.py — DAG única com uma task mapeada por agência ativa."""

import ast
import logging
import sys
import types
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

SAMPLE_YAML = {
    "agencies": {
        "mec": {"url": "https://www.gov.br/mec/pt-br/noticias", "active": True},
        "mds": {"url": "https://www.gov.br/mds/pt-br/noticias", "active": True},
        "disabled_agency": {
            "url": "https://www.gov.br/disabled/pt-br/noticias",
            "active": False,
            "disabled_reason": "site em manutenção",
            "disabled_date": "2026-01-15",
        },
        "no_active_field": {"url": "https://www.gov.br/nofield/pt-br/noticias"},
    }
}


class FakeParam:
    def __init__(self, default=None, **schema):
        self.value = default
        self.schema = schema


class FakeTask:
    """Imita uma task TaskFlow: guarda os kwargs do @task, as chamadas de expand() e as
    chamadas diretas (args e kwargs), para conferir a ligação entre as tasks."""

    def __init__(self, fn, task_kwargs):
        self._original_fn = fn
        self.task_kwargs = task_kwargs
        self.expand_calls = []
        self.expand_results = []
        self.calls = []

    def expand(self, **kwargs):
        self.expand_calls.append(kwargs)
        mapped = MagicMock(name=f"{self._original_fn.__name__}_mapped")
        self.expand_results.append(mapped)
        return mapped

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return MagicMock(name="xcom")


class AirflowException(Exception):
    pass


class AirflowFailException(AirflowException):
    pass


def _make_airflow_mocks(dag_calls, tasks, context):
    def fake_dag(**kwargs):
        dag_calls.append(kwargs)

        def wrapper(fn):
            return fn

        return wrapper

    def fake_task(fn=None, **task_kwargs):
        def wrap(f):
            tasks[f.__name__] = FakeTask(f, task_kwargs)
            return tasks[f.__name__]

        return wrap(fn) if callable(fn) else wrap

    decorators = MagicMock()
    decorators.dag = fake_dag
    decorators.task = fake_task

    sdk = types.ModuleType("airflow.sdk")
    sdk.Param = FakeParam
    sdk.get_current_context = lambda: context

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


def _drop_cached_module():
    for key in list(sys.modules):
        if key.startswith("dags.scrape_agencies"):
            del sys.modules[key]


class Loaded:
    """Módulo importado com Airflow mockado (e o YAML, se informado)."""

    def __init__(self, yaml_content=None):
        self.dag_calls: list[dict] = []
        self.tasks: dict[str, FakeTask] = {}
        self.context: dict = {}
        self.mocks = _make_airflow_mocks(self.dag_calls, self.tasks, self.context)
        self._patchers = [patch.dict(sys.modules, self.mocks)]
        if yaml_content is not None:
            self._patchers.append(
                patch("builtins.open", mock_open(read_data=yaml.dump(yaml_content)))
            )
        _drop_cached_module()
        for p in self._patchers:
            p.start()
        import dags.scrape_agencies as mod

        self.mod = mod

    @property
    def dag_kwargs(self) -> dict:
        assert len(self.dag_calls) == 1
        return self.dag_calls[0]

    @property
    def scrape(self) -> FakeTask:
        return self.tasks["scrape"]

    @property
    def summarize(self) -> FakeTask:
        return self.tasks["summarize"]

    def stop(self):
        for p in reversed(self._patchers):
            p.stop()


@pytest.fixture
def load(monkeypatch):
    # O env é lido no import: cada teste começa sem ele (o padrão é 4).
    monkeypatch.delenv("SCRAPE_MAX_CONCURRENT_AGENCIES", raising=False)
    loaded: list[Loaded] = []

    def _load(yaml_content=SAMPLE_YAML):
        result = Loaded(yaml_content)
        loaded.append(result)
        return result

    yield _load
    for item in loaded:
        item.stop()
    _drop_cached_module()


class TestLoadAgenciesConfig:
    def test_filters_inactive_agencies(self, load):
        result = load().mod._load_agencies_config()
        assert "mec" in result
        assert "mds" in result
        assert "disabled_agency" not in result

    def test_default_active_true_when_field_missing(self, load):
        assert "no_active_field" in load().mod._load_agencies_config()

    def test_extracts_url_correctly(self, load):
        result = load().mod._load_agencies_config()
        assert result["mec"] == "https://www.gov.br/mec/pt-br/noticias"
        assert result["mds"] == "https://www.gov.br/mds/pt-br/noticias"

    def test_returns_empty_when_all_inactive(self, load):
        yaml_content = {
            "agencies": {
                "a": {"url": "https://x.com", "active": False},
                "b": {"url": "https://y.com", "active": False},
            }
        }
        assert load(yaml_content).mod._load_agencies_config() == {}

    def test_returns_empty_when_no_agencies(self, load):
        assert load({"agencies": {}}).mod._load_agencies_config() == {}

    def test_active_field_respects_false(self, load):
        yaml_content = {
            "agencies": {
                "active_one": {"url": "https://a.com", "active": True},
                "inactive_one": {"url": "https://b.com", "active": False},
            }
        }
        assert load(yaml_content).mod._load_agencies_config() == {"active_one": "https://a.com"}


class TestSingleDag:
    def test_generates_one_dag_scrape_agencies(self, load):
        loaded = load()
        assert [call["dag_id"] for call in loaded.dag_calls] == ["scrape_agencies"]

    def test_no_per_agency_dags_or_minute_offsets(self, load):
        mod = load().mod
        assert not hasattr(mod, "_minute_offsets")
        assert not hasattr(mod, "create_scraper_dag")
        for key in SAMPLE_YAML["agencies"]:
            assert not hasattr(mod, f"scrape_{key}")

    def test_schedule_every_two_hours_from_6_to_22(self, load):
        """Cron em BRT (core.default_timezone): 06h, 08h, ..., 22h = 9 execuções por dia."""
        kwargs = load().dag_kwargs
        assert kwargs["schedule"] == "0 6-22/2 * * *"
        assert kwargs["catchup"] is False
        assert kwargs["max_active_runs"] == 1
        assert kwargs["start_date"] == datetime(2025, 1, 1)
        assert kwargs["tags"] == ["scraper", "govbr"]

    def test_default_args_keep_retry_and_timeout_policy(self, load):
        loaded = load()
        args = loaded.dag_kwargs["default_args"]
        assert args["retries"] == 2
        assert args["retry_delay"].total_seconds() == 5 * 60
        # Backoff exponencial em task mapeada derruba o scheduler do Airflow 3.1.0
        # (MappedOperator sem max_retry_delay): o intervalo tem de ser fixo.
        assert not args.get("retry_exponential_backoff", False)
        assert "max_retry_delay" not in args
        assert args["execution_timeout"].total_seconds() == 10 * 60
        assert args["on_failure_callback"] is loaded.mod._on_scrape_failure

    def test_optional_catch_up_params(self, load):
        params = load().dag_kwargs["params"]
        assert set(params) == {"start_date", "end_date"}
        for param in params.values():
            assert param.value is None
            assert param.schema["type"] == ["null", "string"]
            assert param.schema["format"] == "date"
            assert "YYYY-MM-DD" in param.schema["description"]


class TestMappedTask:
    def test_expands_over_sorted_active_agencies_only(self, load):
        loaded = load()
        expected = ["mds", "mec", "no_active_field"]
        assert loaded.scrape.expand_calls == [{"agency_key": expected}]
        assert loaded.mod.ACTIVE_AGENCY_KEYS == expected

    def test_limits_concurrency_and_names_map_index(self, load):
        task_kwargs = load().scrape.task_kwargs
        assert task_kwargs["max_active_tis_per_dag"] == 4
        assert task_kwargs["map_index_template"] == "{{ agency_key }}"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("8", 8),
            (" 6 ", 6),
            ("1", 1),
            ("0", 1),  # 0 nunca rodaria nenhuma task mapeada: a DAG pararia
            ("-3", 1),
            ("", 4),
            ("abc", 4),  # ValueError no import faria a DAG sumir
            ("2.5", 4),
        ],
    )
    def test_concurrency_from_env_at_parse_time(self, load, monkeypatch, raw, expected):
        monkeypatch.setenv("SCRAPE_MAX_CONCURRENT_AGENCIES", raw)
        loaded = load()
        assert loaded.mod.MAX_CONCURRENT_AGENCIES == expected
        assert loaded.scrape.task_kwargs["max_active_tis_per_dag"] == expected

    def test_empty_config_expands_empty_list(self, load):
        assert load({"agencies": {}}).scrape.expand_calls == [{"agency_key": []}]

    def test_real_config_maps_exactly_the_active_agencies(self, load):
        """Com o site_urls.yaml real: uma task mapeada por agência ativa, nenhuma inativa."""
        loaded = load(yaml_content=None)
        with open(REPO_ROOT / "dags/config/site_urls.yaml") as f:
            agencies = yaml.safe_load(f)["agencies"]
        active = sorted(k for k, v in agencies.items() if v.get("active", True))
        inactive = {k for k, v in agencies.items() if not v.get("active", True)}

        mapped = loaded.scrape.expand_calls[0]["agency_key"]
        assert mapped == active
        assert len(mapped) > 100
        assert not inactive & set(mapped)


def _response(status_code=200, body=None, text=None):
    """Resposta do httpx: JSON (body) ou não JSON (json() levanta ValueError)."""
    response = MagicMock()
    response.status_code = status_code
    if body is None:
        response.json.side_effect = ValueError("Expecting value")
        response.text = text or "<html>Service Unavailable</html>"
    else:
        response.json.return_value = body
    return response


OK_BODY = {
    "status": "completed",
    "articles_scraped": 5,
    "articles_saved": 3,
    "articles_new": 2,
    "agencies_processed": ["mec"],
    "errors": [],
}


def _error(category, retryable, agency="mec", error="boom"):
    return {"agency": agency, "error": error, "category": category, "retryable": retryable}


def _body(loaded, agency_key="mec", context=None, response=None, api_url="https://scraper.run.app"):
    """Executa o corpo da task scrape com Variable, token e HTTP mockados."""
    loaded.context.clear()
    loaded.context.update(
        context
        if context is not None
        else {"logical_date": datetime(2026, 11, 17, 17, 0, tzinfo=UTC), "params": {}}
    )
    variable = loaded.mocks["airflow.models"].Variable
    variable.get = MagicMock(side_effect=lambda key, default_var=None: api_url or default_var)
    if response is None:
        response = _response(200, dict(OK_BODY))
    with (
        patch("google.oauth2.id_token.fetch_id_token", return_value="id-token") as fetch,
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response) as post,
    ):
        loaded.last_result = loaded.scrape._original_fn(agency_key)
    return post, fetch, response


class TestScrapeTaskBody:
    def test_posts_same_payload_for_one_agency(self, load):
        loaded = load()
        post, fetch, response = _body(loaded)

        fetch.assert_called_once()
        assert fetch.call_args[0][1] == "https://scraper.run.app"
        post.assert_called_once()
        url = post.call_args[0][0]
        kwargs = post.call_args[1]
        assert url == "https://scraper.run.app/scrape/agencies"
        # 14:00 BRT de 17/11: janela de 9 h ainda no mesmo dia
        assert kwargs["json"] == {
            "start_date": "2026-11-17",
            "end_date": "2026-11-17",
            "agencies": ["mec"],
            "allow_update": False,
            "sequential": True,
        }
        assert kwargs["headers"] == {"Authorization": "Bearer id-token"}
        assert kwargs["timeout"] == 900.0
        # O corpo é lido antes do status: raise_for_status descartaria a categoria do erro.
        response.raise_for_status.assert_not_called()

    def test_returns_small_result_for_summarize(self, load):
        loaded = load()
        _body(loaded, agency_key="mec")
        assert loaded.last_result == {"agency": "mec", "articles_new": 2, "articles_saved": 3}

    def test_old_api_without_articles_new_returns_none(self, load):
        loaded = load()
        body = {k: v for k, v in OK_BODY.items() if k != "articles_new"}
        _body(loaded, response=_response(200, body))
        assert loaded.last_result == {"agency": "mec", "articles_new": None, "articles_saved": 3}

    def test_sets_agency_key_in_context_for_map_index(self, load):
        loaded = load()
        _body(loaded, agency_key="mds")
        assert loaded.context["agency_key"] == "mds"

    def test_six_am_run_includes_previous_day(self, load):
        loaded = load()
        post, _, _ = _body(
            loaded,
            context={"logical_date": datetime(2026, 11, 17, 9, 0, tzinfo=UTC), "params": {}},
        )
        payload = post.call_args[1]["json"]
        assert (payload["start_date"], payload["end_date"]) == ("2026-11-16", "2026-11-17")

    def test_params_override_window(self, load):
        loaded = load()
        post, _, _ = _body(
            loaded,
            context={
                "logical_date": datetime(2026, 11, 17, 17, 0, tzinfo=UTC),
                "params": {"start_date": "2026-11-12", "end_date": "2026-11-14"},
            },
        )
        payload = post.call_args[1]["json"]
        assert (payload["start_date"], payload["end_date"]) == ("2026-11-12", "2026-11-14")

    def test_invalid_params_fail_without_retry_and_without_calling_api(self, load):
        loaded = load()
        with pytest.raises(AirflowFailException, match="start_date e end_date"):
            _body(
                loaded,
                context={
                    "logical_date": datetime(2026, 11, 17, 17, 0, tzinfo=UTC),
                    "params": {"start_date": "2026-11-12"},
                },
            )
        # A sigla já está no contexto: a task falha aparece com o map index certo.
        assert loaded.context["agency_key"] == "mec"

    def test_manual_run_without_logical_date_uses_run_after(self, load):
        loaded = load()
        dag_run = MagicMock(run_after=datetime(2026, 11, 17, 9, 30, tzinfo=UTC))
        post, _, _ = _body(loaded, context={"logical_date": None, "dag_run": dag_run, "params": {}})
        payload = post.call_args[1]["json"]
        assert (payload["start_date"], payload["end_date"]) == ("2026-11-16", "2026-11-17")

    @pytest.mark.parametrize(("status", "http"), [("failed", 500), ("partial", 207)])
    def test_raises_on_logical_failure(self, load, status, http):
        loaded = load()
        response = _response(http, {"status": status, "errors": ["boom"]})
        with pytest.raises(AirflowException, match=f"Scraping {status} for mec") as exc:
            _body(loaded, response=response)
        assert not isinstance(exc.value, AirflowFailException)

    @pytest.mark.parametrize(
        "category", ["html_changed", "url_broken", "access_denied", "anti_bot"]
    )
    def test_permanent_error_fails_without_retry(self, load, category):
        loaded = load()
        body = {"status": "failed", "errors": [_error(category, False)]}
        with pytest.raises(AirflowFailException, match=category) as exc:
            _body(loaded, response=_response(500, body))
        assert "mec" in str(exc.value)
        assert "sem retry" in str(exc.value)

    def test_unknown_agency_fails_without_retry(self, load):
        loaded = load()
        body = {
            "status": "failed",
            "errors": [
                _error("unknown_agency", False, error="Agency 'mec' not found in the YAML file.")
            ],
        }
        with pytest.raises(AirflowFailException, match="unknown_agency"):
            _body(loaded, response=_response(500, body))

    @pytest.mark.parametrize("category", ["network_error", "unknown", "empty_content", None])
    def test_transient_error_is_retried(self, load, category):
        loaded = load()
        body = {"status": "failed", "errors": [_error(category, True)]}
        with pytest.raises(AirflowException, match="Scraping failed for mec") as exc:
            _body(loaded, response=_response(500, body))
        assert not isinstance(exc.value, AirflowFailException)

    def test_mixed_permanent_and_transient_is_retried(self, load):
        loaded = load()
        body = {
            "status": "failed",
            "errors": [_error("html_changed", False), _error("network_error", True)],
        }
        with pytest.raises(AirflowException) as exc:
            _body(loaded, response=_response(500, body))
        assert not isinstance(exc.value, AirflowFailException)

    def test_old_api_without_retryable_is_retried(self, load):
        """API anterior ao contrato: errors só com agency e error. Repete, como hoje."""
        loaded = load()
        body = {"status": "failed", "errors": [{"agency": "mec", "error": "HTTP 404"}]}
        with pytest.raises(AirflowException) as exc:
            _body(loaded, response=_response(500, body))
        assert not isinstance(exc.value, AirflowFailException)

    def test_permanent_error_of_another_agency_does_not_count(self, load):
        """Só os erros da agência da task decidem (ex.: erro de config com agency='config')."""
        loaded = load()
        body = {"status": "failed", "errors": [_error("html_changed", False, agency="config")]}
        with pytest.raises(AirflowException) as exc:
            _body(loaded, response=_response(500, body))
        assert not isinstance(exc.value, AirflowFailException)

    @pytest.mark.parametrize("http", [500, 502, 503, 504, 401, 403, 429])
    def test_http_error_without_json_is_retried(self, load, http):
        loaded = load()
        with pytest.raises(AirflowException, match=f"HTTP {http}") as exc:
            _body(loaded, response=_response(http, None))
        assert not isinstance(exc.value, AirflowFailException)

    def test_http_error_with_detail_body_is_retried(self, load):
        """Exceção que escapa do manager: {"detail": ...} sem errors[]."""
        loaded = load()
        with pytest.raises(AirflowException) as exc:
            _body(loaded, response=_response(500, {"detail": "DB connection failed"}))
        assert not isinstance(exc.value, AirflowFailException)
        assert "DB connection failed" in str(exc.value)

    def test_success_status_without_json_is_retried(self, load):
        loaded = load()
        with pytest.raises(AirflowException) as exc:
            _body(loaded, response=_response(200, None))
        assert not isinstance(exc.value, AirflowFailException)

    def test_network_error_propagates_for_retry(self, load):
        """Timeout/conexão do httpx não é capturado: o Airflow repete."""
        loaded = load()
        loaded.context.update({"logical_date": datetime(2026, 11, 17, 17, 0, tzinfo=UTC)})
        variable = loaded.mocks["airflow.models"].Variable
        variable.get = MagicMock(return_value="https://scraper.run.app")
        with (
            patch("google.oauth2.id_token.fetch_id_token", return_value="tok"),
            patch("google.auth.transport.requests.Request"),
            patch("httpx.post", side_effect=TimeoutError("read timeout")),
            pytest.raises(TimeoutError),
        ):
            loaded.scrape._original_fn("mec")

    def test_requires_scraper_api_url(self, load):
        loaded = load()
        with pytest.raises(ValueError, match="scraper_api_url"):
            _body(loaded, api_url="")


# =============================================================================
# summarize: única folha, decide a cor da execução
# =============================================================================


class LazySequenceLike:
    """Imita o LazyXComSequence do Airflow 3.0.1: itera e tem len(), mas fatiar levanta
    TypeError (no 3.1 fatiar funciona; a summarize só pode iterar)."""

    def __init__(self, items):
        self._items = list(items)

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, key):
        if isinstance(key, slice):
            raise TypeError("slicing not supported")
        return self._items[key]


def _ok(agency, new=1, saved=2):
    return {"agency": agency, "articles_new": new, "articles_saved": saved}


def _summarize(loaded, results, expected, ratio_var="0.10"):
    variable = loaded.mocks["airflow.models"].Variable
    variable.get = MagicMock(return_value=ratio_var)
    return loaded.summarize._original_fn(results, expected)


AGENCIES_20 = [f"ag{i:02d}" for i in range(20)]


class TestSummarizeTask:
    def test_is_the_only_leaf_with_all_done_and_no_retry(self, load):
        loaded = load()
        assert set(loaded.tasks) == {"scrape", "summarize"}
        kwargs = loaded.summarize.task_kwargs
        assert kwargs["trigger_rule"] == "all_done"
        assert kwargs["retries"] == 0
        assert "retry_exponential_backoff" not in kwargs
        assert "max_retry_delay" not in kwargs

    def test_consumes_mapped_scrape_output_and_active_list(self, load):
        loaded = load()
        (call,) = loaded.summarize.calls
        args, kwargs = call
        assert args[0] is loaded.scrape.expand_results[0]
        assert list(args[1]) == loaded.mod.ACTIVE_AGENCY_KEYS == ["mds", "mec", "no_active_field"]
        assert kwargs == {}
        # scrape só é expandida, nunca chamada direto (senão viraria outra folha).
        assert loaded.scrape.calls == []

    def test_all_ok_returns_summary(self, load, caplog):
        loaded = load()
        with caplog.at_level(logging.INFO):
            summary = _summarize(loaded, [_ok("mec", 2, 3), _ok("mds", 0, 1)], ["mds", "mec"])
        assert summary == {
            "total": 2,
            "ok": 2,
            "failed": [],
            "failed_ratio": 0.0,
            "articles_new": 2,
            "articles_saved": 4,
            "agencies_without_articles_new": 0,
        }
        assert "2/2 agências OK" in caplog.text

    def test_failures_under_ratio_stay_green_and_are_logged(self, load, caplog):
        """Falhas ficam fora do LazyXComSequence: as que faltam são as que falharam."""
        loaded = load()
        results = [_ok(a) for a in AGENCIES_20 if a not in ("ag07", "ag03")]
        with caplog.at_level(logging.INFO):
            summary = _summarize(loaded, LazySequenceLike(results), AGENCIES_20)
        assert summary["ok"] == 18
        assert summary["failed"] == ["ag03", "ag07"]  # ordenadas
        assert summary["failed_ratio"] == 0.1
        assert "Agências com falha: ag03, ag07" in caplog.text

    def test_failures_above_ratio_fail_the_run(self, load):
        loaded = load()
        results = [_ok(a) for a in AGENCIES_20[3:]]  # 3 de 20 = 15% > 10%
        with pytest.raises(AirflowFailException, match="3 de 20 agências falharam") as exc:
            _summarize(loaded, LazySequenceLike(results), AGENCIES_20)
        assert "ag00, ag01, ag02" in str(exc.value)

    def test_ratio_comes_from_variable(self, load):
        loaded = load()
        results = [_ok(a) for a in AGENCIES_20[3:]]
        summary = _summarize(loaded, results, AGENCIES_20, ratio_var="0.2")
        assert summary["failed"] == ["ag00", "ag01", "ag02"]

    @pytest.mark.parametrize("ratio_var", ["abc", "", "-0.1", "1.5", "nan", None])
    def test_invalid_ratio_falls_back_to_default(self, load, ratio_var):
        loaded = load()
        two_failed = [_ok(a) for a in AGENCIES_20[2:]]  # 10%: no limite padrão, verde
        assert _summarize(loaded, two_failed, AGENCIES_20, ratio_var)["ok"] == 18
        three_failed = [_ok(a) for a in AGENCIES_20[3:]]
        with pytest.raises(AirflowFailException):
            _summarize(loaded, three_failed, AGENCIES_20, ratio_var)

    def test_variable_has_default(self, load):
        loaded = load()
        _summarize(loaded, [_ok("mec")], ["mec"])
        variable = loaded.mocks["airflow.models"].Variable
        variable.get.assert_called_once_with("scraper_max_failed_ratio", default_var="0.10")

    def test_no_agency_ok_fails_even_with_ratio_one(self, load):
        loaded = load()
        with pytest.raises(AirflowFailException, match="Nenhuma agência"):
            _summarize(loaded, LazySequenceLike([]), ["mds", "mec"], ratio_var="1")

    def test_no_active_agency_fails(self, load):
        loaded = load()
        with pytest.raises(AirflowFailException, match="Nenhuma agência"):
            _summarize(loaded, [], [])

    def test_ignores_unexpected_items_and_counts_missing_articles_new(self, load):
        loaded = load()
        results = [
            None,
            "lixo",
            {"articles_new": 5},  # sem agência
            {"agency": "mec", "articles_new": None, "articles_saved": 7},  # API antiga
            _ok("mds", 3, 3),
            {"agency": "fora_da_lista", "articles_new": 1, "articles_saved": 1},
        ]
        summary = _summarize(loaded, results, ["mds", "mec"])
        assert summary["ok"] == 2
        assert summary["failed"] == []
        assert summary["agencies_without_articles_new"] == 1
        assert summary["articles_new"] == 4

    def test_never_slices_the_lazy_sequence(self, load):
        loaded = load()
        seq = LazySequenceLike([_ok("mec")])
        with pytest.raises(TypeError):
            seq[:]  # o 3.0.1 levanta aqui; a summarize não pode fazer isso
        assert _summarize(loaded, seq, ["mec"])["ok"] == 1


class TestNoExponentialBackoffAnywhere:
    """Regressão (18/11/2026): retry_exponential_backoff/max_retry_delay em task mapeada
    derruba o scheduler do Airflow 3.1.0. Nenhum dos dois pode aparecer como argumento
    ou chave em nenhuma DAG de scraping."""

    @pytest.mark.parametrize("dag_file", ["scrape_agencies.py", "scrape_ebc.py"])
    def test_not_used_as_keyword_or_dict_key(self, dag_file):
        tree = ast.parse((REPO_ROOT / "dags" / dag_file).read_text())
        forbidden = {"retry_exponential_backoff", "max_retry_delay"}
        for node in ast.walk(tree):
            if isinstance(node, ast.keyword):
                assert node.arg not in forbidden, f"{dag_file}:{node.lineno}"
            if isinstance(node, ast.Dict):
                for key in node.keys:
                    if isinstance(key, ast.Constant):
                        assert key.value not in forbidden, f"{dag_file}:{key.lineno}"


class TestOnScrapeFailure:
    def test_logs_failure_with_agency_and_map_index(self, load, caplog):
        mod = load().mod
        ti = MagicMock(dag_id="scrape_agencies", task_id="scrape", try_number=3, map_index=42)
        ti.rendered_map_index = "mec"
        context = {"task_instance": ti, "exception": ValueError("connection timeout")}

        with caplog.at_level(logging.ERROR):
            mod._on_scrape_failure(context)

        assert "dag=scrape_agencies" in caplog.text
        assert "task=scrape" in caplog.text
        assert "agency=mec" in caplog.text
        assert "map_index=42" in caplog.text
        assert "connection timeout" in caplog.text

    def test_prefers_agency_key_from_context(self, load, caplog):
        mod = load().mod
        ti = MagicMock(dag_id="scrape_agencies", task_id="scrape", try_number=1, map_index=7)
        ti.rendered_map_index = None
        with caplog.at_level(logging.ERROR):
            mod._on_scrape_failure({"task_instance": ti, "agency_key": "mds", "exception": "x"})
        assert "agency=mds" in caplog.text

    def test_handles_missing_task_instance(self, load, caplog):
        mod = load().mod
        with caplog.at_level(logging.ERROR):
            mod._on_scrape_failure({"task_instance": None, "exception": "some error"})
        assert "dag=unknown" in caplog.text
        assert "agency=unknown" in caplog.text
