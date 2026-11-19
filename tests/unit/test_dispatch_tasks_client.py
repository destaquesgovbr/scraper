"""Cliente REST do Cloud Tasks e configuração do despacho (modo e envs)."""

import base64
import json
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.dispatch.config import (
    DispatchConfigError,
    DispatchMode,
    TasksConfig,
    dispatch_mode_from_env,
    effective_mode,
)
from govbr_scraper.dispatch.tasks_client import (
    CREATED,
    DUPLICATE,
    FAILED,
    CloudTasksClient,
    backfill_task_id,
    tick_task_id,
)

QUEUE = "projects/inspire-7-finep/locations/southamerica-east1/queues/scrape-govbr"
TARGET = "https://destaquesgovbr-scraper-api-990583792367.southamerica-east1.run.app"
SA = "scrape-tasks-invoker@inspire-7-finep.iam.gserviceaccount.com"


def _response(status, text=""):
    response = MagicMock()
    response.status_code = status
    response.text = text
    return response


def _client(*responses, error=None):
    session = MagicMock()
    if error is not None:
        session.post.side_effect = error
    else:
        session.post.side_effect = list(responses)
    return CloudTasksClient(QUEUE, TARGET + "/", SA, session=session), session


class TestTaskIds:
    @pytest.mark.parametrize(
        ("utc", "expected"),
        [
            (datetime(2026, 11, 19, 18, 0, 3, tzinfo=UTC), "scrape-mec-202611191500"),
            (datetime(2026, 11, 19, 18, 14, 59, tzinfo=UTC), "scrape-mec-202611191500"),
            (datetime(2026, 11, 19, 18, 15, 1, tzinfo=UTC), "scrape-mec-202611191515"),
            (datetime(2026, 11, 20, 2, 59, tzinfo=UTC), "scrape-mec-202611192345"),
        ],
    )
    def test_tick_id_is_brt_rounded_down_to_15_min(self, utc, expected):
        assert tick_task_id("mec", utc) == expected

    def test_invalid_characters_become_hyphen(self):
        assert tick_task_id("a.b c", datetime(2026, 11, 19, 18, tzinfo=UTC)).startswith(
            "scrape-a-b-c-"
        )

    def test_backfill_id(self):
        now = datetime(2026, 11, 19, 17, 42, 30, tzinfo=UTC)  # 14:42 BRT
        assert (
            backfill_task_id("agencia_brasil", date(2026, 11, 1), date(2026, 11, 3), now)
            == "backfill-agencia_brasil-20261101-20261103-1442"
        )

    def test_backfill_suffix_replaces_the_clock(self):
        # O dedup_key da DAG: a repetição da task, minutos depois, gera o mesmo nome (409).
        start, end = date(2026, 11, 1), date(2026, 11, 3)
        first = datetime(2026, 11, 19, 17, 42, tzinfo=UTC)
        retry = datetime(2026, 11, 19, 17, 47, tzinfo=UTC)
        names = {
            backfill_task_id("mec", start, end, at, suffix="execucao-01") for at in (first, retry)
        }
        assert names == {"backfill-mec-20261101-20261103-execucao-01"}


class TestBuildTask:
    def test_http_request_with_oidc_and_deadline(self):
        client, _ = _client()
        task = client.build_task("scrape-mec-202611191500", {"agency": "mec", "trigger": "task"})
        assert task["name"] == f"{QUEUE}/tasks/scrape-mec-202611191500"
        assert task["dispatchDeadline"] == "900s"
        http = task["httpRequest"]
        assert http["url"] == f"{TARGET}/scrape/agency"
        assert http["httpMethod"] == "POST"
        assert http["headers"] == {"Content-Type": "application/json"}
        assert json.loads(base64.b64decode(http["body"])) == {"agency": "mec", "trigger": "task"}
        assert http["oidcToken"] == {"serviceAccountEmail": SA, "audience": TARGET}


class TestCreateTask:
    def test_created(self):
        client, session = _client(_response(200, "{}"))
        result = client.create_task("t1", {"agency": "mec"})
        assert (result.status, result.ok, result.http_status) == (CREATED, True, 200)
        url = session.post.call_args.args[0]
        assert url == f"https://cloudtasks.googleapis.com/v2/{QUEUE}/tasks"
        kwargs = session.post.call_args.kwargs
        assert kwargs["json"]["responseView"] == "BASIC"
        assert kwargs["json"]["task"]["name"].endswith("/tasks/t1")
        assert kwargs["timeout"] == 10

    def test_409_is_duplicate(self):
        client, _ = _client(_response(409, '{"error": {"status": "ALREADY_EXISTS"}}'))
        result = client.create_task("t1", {"agency": "mec"})
        assert (result.status, result.ok) == (DUPLICATE, True)

    @pytest.mark.parametrize("status", [400, 403, 404, 429, 500, 503])
    def test_other_http_errors_fail_without_raising(self, status):
        client, _ = _client(_response(status, "PERMISSION_DENIED " + "x" * 1000))
        result = client.create_task("t1", {"agency": "mec"})
        assert (result.status, result.ok, result.http_status) == (FAILED, False, status)
        assert len(result.error) == 300

    def test_network_error_fails_without_raising(self):
        client, _ = _client(error=ConnectionError("reset by peer"))
        result = client.create_task("t1", {"agency": "mec"})
        assert result.status == FAILED
        assert result.error == "ConnectionError: reset by peer"

    def test_credentials_error_fails_without_raising(self):
        client = CloudTasksClient(QUEUE, TARGET, SA)
        with patch("google.auth.default", side_effect=RuntimeError("no ADC")):
            result = client.create_task("t1", {"agency": "mec"})
        assert result.status == FAILED
        assert "no ADC" in result.error

    def test_session_from_default_credentials_is_reused_and_closed(self):
        client = CloudTasksClient(QUEUE, TARGET, SA)
        session = MagicMock()
        session.post.return_value = _response(200)
        with (
            patch("google.auth.default", return_value=("creds", "proj")) as default,
            patch(
                "google.auth.transport.requests.AuthorizedSession", return_value=session
            ) as authorized,
        ):
            client.create_task("t1", {})
            client.create_task("t2", {})
        default.assert_called_once_with(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        authorized.assert_called_once_with("creds")
        client.close()
        session.close.assert_called_once()

    def test_injected_session_is_not_closed(self):
        client, session = _client()
        client.close()
        session.close.assert_not_called()

    def test_from_config(self):
        client = CloudTasksClient.from_config(TasksConfig(QUEUE, SA, TARGET), session=MagicMock())
        assert (client.queue, client.invoker_sa, client.target_url) == (QUEUE, SA, TARGET)


class TestDispatchConfig:
    @pytest.mark.parametrize(
        ("raw", "mode"),
        [
            ("", DispatchMode.OFF),
            ("off", DispatchMode.OFF),
            ("dry_run", DispatchMode.DRY_RUN),
            (" LIVE ", DispatchMode.LIVE),
            ("ligado", DispatchMode.OFF),  # inválido: pausa
        ],
    )
    def test_mode_from_env(self, monkeypatch, raw, mode):
        monkeypatch.setenv("SCRAPE_DISPATCH_MODE", raw)
        assert dispatch_mode_from_env() == mode

    def test_mode_defaults_to_off(self, monkeypatch):
        monkeypatch.delenv("SCRAPE_DISPATCH_MODE", raising=False)
        assert dispatch_mode_from_env() == DispatchMode.OFF

    @pytest.mark.parametrize(
        ("env_mode", "dry_run", "expected"),
        [
            (DispatchMode.LIVE, True, DispatchMode.DRY_RUN),
            (DispatchMode.LIVE, False, DispatchMode.LIVE),
            (DispatchMode.DRY_RUN, False, DispatchMode.DRY_RUN),
            (DispatchMode.OFF, True, DispatchMode.OFF),
        ],
    )
    def test_dry_run_in_body_forces_dry(self, env_mode, dry_run, expected):
        assert effective_mode(env_mode, dry_run) == expected

    def test_tasks_config_from_env(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_TASKS_QUEUE", QUEUE + "/")
        monkeypatch.setenv("SCRAPE_TASKS_INVOKER_SA", SA)
        monkeypatch.setenv("SCRAPE_TASKS_TARGET_URL", TARGET + "/")
        assert TasksConfig.from_env() == TasksConfig(QUEUE, SA, TARGET)

    def test_tasks_config_lists_missing_envs(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_TASKS_QUEUE", QUEUE)
        monkeypatch.delenv("SCRAPE_TASKS_INVOKER_SA", raising=False)
        monkeypatch.setenv("SCRAPE_TASKS_TARGET_URL", " ")
        with pytest.raises(DispatchConfigError) as exc:
            TasksConfig.from_env()
        assert "SCRAPE_TASKS_INVOKER_SA" in str(exc.value)
        assert "SCRAPE_TASKS_TARGET_URL" in str(exc.value)
        assert "SCRAPE_TASKS_QUEUE" not in str(exc.value)
