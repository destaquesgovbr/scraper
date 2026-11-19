"""Tests for dags/scrape_backfill.py — DAG manual que chama POST /dispatch com reason=backfill."""

import re
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.dispatch.tasks_client import DEDUP_KEY_PATTERN


class FakeParam:
    _NOTSET = object()

    def __init__(self, default=_NOTSET, **schema):
        self.required = default is FakeParam._NOTSET
        self.value = None if self.required else default
        self.schema = schema


class FakeTask:
    def __init__(self, fn, task_kwargs):
        self._original_fn = fn
        self.task_kwargs = task_kwargs
        self.calls = []

    def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return MagicMock(name="xcom")


class AirflowException(Exception):
    pass


class AirflowFailException(AirflowException):
    pass


class Loaded:
    def __init__(self):
        self.dag_calls: list[dict] = []
        self.tasks: dict[str, FakeTask] = {}
        self.context: dict = {}

        def fake_dag(**kwargs):
            self.dag_calls.append(kwargs)
            return lambda fn: fn

        def fake_task(fn=None, **task_kwargs):
            def wrap(f):
                self.tasks[f.__name__] = FakeTask(f, task_kwargs)
                return self.tasks[f.__name__]

            return wrap(fn) if callable(fn) else wrap

        decorators = MagicMock()
        decorators.dag = fake_dag
        decorators.task = fake_task
        sdk = types.ModuleType("airflow.sdk")
        sdk.Param = FakeParam
        sdk.get_current_context = lambda: self.context
        exceptions = types.ModuleType("airflow.exceptions")
        exceptions.AirflowException = AirflowException
        exceptions.AirflowFailException = AirflowFailException
        self.mocks = {
            "airflow": MagicMock(),
            "airflow.decorators": decorators,
            "airflow.models": MagicMock(),
            "airflow.exceptions": exceptions,
            "airflow.sdk": sdk,
        }
        self._patcher = patch.dict(sys.modules, self.mocks)
        sys.modules.pop("dags.scrape_backfill", None)
        self._patcher.start()
        import dags.scrape_backfill as mod

        self.mod = mod

    @property
    def dag_kwargs(self) -> dict:
        assert len(self.dag_calls) == 1
        return self.dag_calls[0]

    def stop(self):
        self._patcher.stop()
        sys.modules.pop("dags.scrape_backfill", None)


@pytest.fixture
def loaded():
    result = Loaded()
    yield result
    result.stop()


PARAMS = {"agencies": [], "start_date": "2026-11-01", "end_date": "2026-11-03"}


def _response(status_code=200, body=None):
    response = MagicMock()
    response.status_code = status_code
    if body is None:
        response.json.side_effect = ValueError("Expecting value")
        response.text = "<html>Service Unavailable</html>"
    else:
        response.json.return_value = body
    return response


def _live_body(**overrides):
    body = {
        "mode": "live",
        "reason": "backfill",
        "due": 2,
        "dispatched": 2,
        "duplicates": 0,
        "failed": 0,
        "agencies": [
            {"agency": "mec", "task": "backfill-mec-20261101-20261103-1442", "result": "created"},
            {"agency": "mds", "task": "backfill-mds-20261101-20261103-1442", "result": "created"},
        ],
        "skipped_circuit": [],
        "unknown_agencies": [],
    }
    body.update(overrides)
    return body


def _run(loaded, params=None, response=None, api_url="https://scraper.run.app", run_id=None):
    loaded.context.clear()
    loaded.context["params"] = dict(PARAMS if params is None else params)
    if run_id is not None:
        loaded.context["run_id"] = run_id
    variable = loaded.mocks["airflow.models"].Variable
    variable.get = MagicMock(side_effect=lambda key, default_var=None: api_url or default_var)
    with (
        patch("google.oauth2.id_token.fetch_id_token", return_value="id-token") as fetch,
        patch("google.auth.transport.requests.Request"),
        patch("httpx.post", return_value=response or _response(200, _live_body())) as post,
    ):
        result = loaded.tasks["dispatch_backfill"]._original_fn()
    return result, post, fetch


class TestDagDefinition:
    def test_manual_dag_with_one_task(self, loaded):
        kwargs = loaded.dag_kwargs
        assert kwargs["dag_id"] == "scrape_backfill"
        assert kwargs["schedule"] is None
        assert kwargs["catchup"] is False
        assert kwargs["max_active_runs"] == 1
        assert set(loaded.tasks) == {"dispatch_backfill"}
        assert len(loaded.tasks["dispatch_backfill"].calls) == 1

    def test_retry_once_without_exponential_backoff(self, loaded):
        args = loaded.dag_kwargs["default_args"]
        assert args["retries"] == 1
        assert args["retry_delay"].total_seconds() == 300
        assert "retry_exponential_backoff" not in args
        assert "max_retry_delay" not in args
        assert loaded.tasks["dispatch_backfill"].task_kwargs == {}

    def test_params(self, loaded):
        params = loaded.dag_kwargs["params"]
        assert set(params) == {"agencies", "start_date", "end_date", "force", "dry_run"}
        assert params["agencies"].value == []
        assert params["agencies"].schema["type"] == "array"
        for name in ("start_date", "end_date"):
            assert params[name].required, name  # sem default: obrigatório no disparo
            assert params[name].schema == {
                "type": "string",
                "format": "date",
                "title": params[name].schema["title"],
                "description": params[name].schema["description"],
            }
        assert params["force"].value is False
        assert params["dry_run"].value is False


class TestPayload:
    def test_minimal(self, loaded):
        assert loaded.mod._backfill_payload(PARAMS) == {
            "reason": "backfill",
            "agencies": [],
            "start_date": "2026-11-01",
            "end_date": "2026-11-03",
            "force": False,
            "dry_run": False,
        }

    def test_dedup_key_is_stable_per_run(self, loaded):
        run_id = "manual__2026-11-19T12:34:56.789012+00:00"
        first = loaded.mod._backfill_payload(PARAMS, run_id)
        again = loaded.mod._backfill_payload(PARAMS, run_id)  # repetição da task
        other = loaded.mod._backfill_payload(PARAMS, "manual__2026-11-19T13:00:00+00:00")
        assert first["dedup_key"] == again["dedup_key"]
        assert first["dedup_key"] != other["dedup_key"]
        assert re.fullmatch(r"[0-9a-f]{12}", first["dedup_key"])
        assert re.fullmatch(DEDUP_KEY_PATTERN, first["dedup_key"])

    @pytest.mark.parametrize(
        ("agencies", "expected"),
        [
            (["mec", " mds ", "mec", ""], ["mec", "mds"]),
            ("mec, mds\nagencia_brasil", ["mec", "mds", "agencia_brasil"]),
            (None, []),
        ],
    )
    def test_agencies_list_or_text(self, loaded, agencies, expected):
        payload = loaded.mod._backfill_payload({**PARAMS, "agencies": agencies})
        assert payload["agencies"] == expected

    def test_force_and_dry_run_must_be_true(self, loaded):
        payload = loaded.mod._backfill_payload({**PARAMS, "force": True, "dry_run": "true"})
        assert (payload["force"], payload["dry_run"]) == (True, False)

    @pytest.mark.parametrize(
        "params",
        [
            {"start_date": None, "end_date": "2026-11-03"},
            {"start_date": "2026-11-01"},
            {"start_date": "01/11/2026", "end_date": "2026-11-03"},
            {"start_date": "2026-02-30", "end_date": "2026-11-03"},
            {"start_date": "2026-11-05", "end_date": "2026-11-03"},
            {"start_date": "2026-11-01", "end_date": "2026-11-03", "agencies": ["mec;drop"]},
            {"start_date": "2026-11-01", "end_date": "2026-11-03", "agencies": 3},
        ],
    )
    def test_invalid_params(self, loaded, params):
        with pytest.raises(ValueError):
            loaded.mod._backfill_payload(params)


class TestTaskBody:
    def test_posts_backfill_to_dispatch_with_id_token(self, loaded):
        result, post, fetch = _run(loaded, params={**PARAMS, "agencies": ["mec", "mds"]})
        fetch.assert_called_once()
        assert fetch.call_args.args[1] == "https://scraper.run.app"
        assert post.call_args.args[0] == "https://scraper.run.app/dispatch"
        assert post.call_args.kwargs["json"]["reason"] == "backfill"
        assert post.call_args.kwargs["json"]["agencies"] == ["mec", "mds"]
        assert post.call_args.kwargs["headers"] == {"Authorization": "Bearer id-token"}
        assert post.call_args.kwargs["timeout"] == 300.0
        assert result["dispatched"] == 2
        assert result["failed"] == []

    def test_sends_the_dedup_key_of_the_run(self, loaded):
        _, post, _ = _run(loaded, run_id="manual__2026-11-19T12:34:56+00:00")
        _, again, _ = _run(loaded, run_id="manual__2026-11-19T12:34:56+00:00")
        key = post.call_args.kwargs["json"]["dedup_key"]
        assert key == again.call_args.kwargs["json"]["dedup_key"]

    def test_invalid_params_fail_without_calling_api(self, loaded):
        with pytest.raises(AirflowFailException):
            _run(loaded, params={"start_date": "2026-11-05", "end_date": "2026-11-01"})

    def test_requires_scraper_api_url(self, loaded):
        with pytest.raises(ValueError, match="scraper_api_url"):
            _run(loaded, api_url="")

    def test_off_mode_fails_without_retry(self, loaded):
        body = _live_body(mode="off", due=0, dispatched=0, agencies=[])
        with pytest.raises(AirflowFailException, match="off"):
            _run(loaded, response=_response(200, body))

    def test_dry_run_server_without_dry_run_param_fails(self, loaded):
        body = _live_body(mode="dry_run", dispatched=0)
        with pytest.raises(AirflowFailException, match="dry_run"):
            _run(loaded, response=_response(200, body))

    def test_dry_run_param_accepts_dry_run_mode(self, loaded):
        body = _live_body(mode="dry_run", dispatched=0)
        result, post, _ = _run(
            loaded, params={**PARAMS, "dry_run": True}, response=_response(200, body)
        )
        assert post.call_args.kwargs["json"]["dry_run"] is True
        assert result["mode"] == "dry_run"

    def test_rejected_tasks_fail_listing_the_agencies(self, loaded):
        agencies = [
            {"agency": "mec", "task": "t1", "result": "created"},
            {"agency": "mds", "task": "t2", "result": "failed"},
        ]
        body = _live_body(agencies=agencies, dispatched=1, failed=1)
        with pytest.raises(AirflowFailException, match="mds"):
            _run(loaded, response=_response(200, body))

    def test_422_fails_without_retry(self, loaded):
        with pytest.raises(AirflowFailException):
            _run(loaded, response=_response(422, {"detail": "backfill exige start_date"}))

    @pytest.mark.parametrize("status", [500, 503])
    def test_5xx_is_retried(self, loaded, status):
        with pytest.raises(AirflowException) as exc:
            _run(loaded, response=_response(status, {"detail": "x"}))
        assert not isinstance(exc.value, AirflowFailException)

    def test_body_without_json_is_retried(self, loaded):
        with pytest.raises(AirflowException) as exc:
            _run(loaded, response=_response(200, None))
        assert not isinstance(exc.value, AirflowFailException)

    def test_skipped_and_unknown_are_reported_not_failures(self, loaded):
        body = _live_body(skipped_circuit=["antaq"], unknown_agencies=["sri"])
        result, _, _ = _run(loaded, response=_response(200, body))
        assert result["skipped_circuit"] == ["antaq"]
        assert result["unknown_agencies"] == ["sri"]
