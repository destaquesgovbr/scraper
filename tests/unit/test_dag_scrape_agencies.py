"""Tests for dags/scrape_agencies.py — DAG única com uma task mapeada por agência ativa."""

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
    """Imita uma task TaskFlow: guarda os kwargs do @task e as chamadas de expand()."""

    def __init__(self, fn, task_kwargs):
        self._original_fn = fn
        self.task_kwargs = task_kwargs
        self.expand_calls = []

    def expand(self, **kwargs):
        self.expand_calls.append(kwargs)
        return MagicMock(name="mapped")

    def __call__(self, *args, **kwargs):
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

    def stop(self):
        for p in reversed(self._patchers):
            p.stop()


@pytest.fixture
def load():
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
        assert args["retry_exponential_backoff"] is True
        assert args["max_retry_delay"].total_seconds() == 15 * 60
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
        response = MagicMock()
        response.json.return_value = {"status": "completed", "errors": []}
    with (
        patch("google.oauth2.id_token.fetch_id_token", return_value="id-token") as fetch,
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response) as post,
    ):
        loaded.scrape._original_fn(agency_key)
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
        response.raise_for_status.assert_called_once()

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

    @pytest.mark.parametrize("status", ["failed", "partial"])
    def test_raises_on_logical_failure(self, load, status):
        loaded = load()
        response = MagicMock()
        response.json.return_value = {"status": status, "errors": ["boom"]}
        with pytest.raises(AirflowException, match=f"Scraping {status} for mec"):
            _body(loaded, response=response)

    def test_requires_scraper_api_url(self, load):
        loaded = load()
        with pytest.raises(ValueError, match="scraper_api_url"):
            _body(loaded, api_url="")


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
