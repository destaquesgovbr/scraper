"""POST /dispatch: modos (off, dry_run, live), dry_run no corpo, erros e o dispatch_run."""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from govbr_scraper.api import app
from govbr_scraper.dispatch.config import DispatchMode
from govbr_scraper.dispatch.service import DispatchFailed, DispatchOutcome
from govbr_scraper.state.repository import StateUnavailable

client = TestClient(app)

TASK_ENVS = {
    "SCRAPE_TASKS_QUEUE": "projects/inspire-7-finep/locations/southamerica-east1/queues/scrape-govbr",
    "SCRAPE_TASKS_INVOKER_SA": "scrape-tasks-invoker@inspire-7-finep.iam.gserviceaccount.com",
    "SCRAPE_TASKS_TARGET_URL": "https://destaquesgovbr-scraper-api-990583792367.southamerica-east1.run.app",
}


class Api:
    def __init__(self, monkeypatch, mode="dry_run", task_envs=True, error=None):
        if mode is None:
            monkeypatch.delenv("SCRAPE_DISPATCH_MODE", raising=False)
        else:
            monkeypatch.setenv("SCRAPE_DISPATCH_MODE", mode)
        for env, value in TASK_ENVS.items():
            if task_envs:
                monkeypatch.setenv(env, value)
            else:
                monkeypatch.delenv(env, raising=False)
        self.pg = MagicMock(name="PostgresManager")
        self.conn = self.pg.get_connection.return_value
        self.dispatcher_cls = MagicMock(name="Dispatcher")
        self.dispatcher = self.dispatcher_cls.return_value
        if error is not None:
            self.dispatcher.tick.side_effect = error
            self.dispatcher.backfill.side_effect = error
        else:
            self.dispatcher.tick.side_effect = lambda: DispatchOutcome(
                mode=self.mode_used(), reason="tick", due=2, dispatched=2
            )
            self.dispatcher.backfill.side_effect = lambda **kw: DispatchOutcome(
                mode=self.mode_used(),
                reason="backfill",
                due=1,
                start_date=kw["start_date"].isoformat(),
                end_date=kw["end_date"].isoformat(),
            )
        self.tasks_cls = MagicMock(name="CloudTasksClient")
        self.log = MagicMock(name="log_dispatch_run")
        self.log_failed = MagicMock(name="log_dispatch_failed")

    def mode_used(self) -> str:
        return self.dispatcher_cls.call_args.kwargs["mode"].value

    def post(self, payload=None):
        with (
            patch(
                "govbr_scraper.storage.postgres_manager.PostgresManager", return_value=self.pg
            ) as self.pg_cls,
            patch("govbr_scraper.dispatch.service.Dispatcher", self.dispatcher_cls),
            patch("govbr_scraper.dispatch.tasks_client.CloudTasksClient", self.tasks_cls),
            patch("govbr_scraper.api.log_dispatch_run", self.log),
            patch("govbr_scraper.api.log_dispatch_failed", self.log_failed),
        ):
            if payload is None:
                return client.post("/dispatch")
            return client.post("/dispatch", json=payload)


class TestOff:
    @pytest.mark.parametrize("mode", [None, "off", "ligado"])
    def test_off_only_logs_dispatch_run(self, monkeypatch, mode):
        api = Api(monkeypatch, mode=mode)
        response = api.post({"reason": "tick"})
        assert response.status_code == 200
        body = response.json()
        assert body["mode"] == "off"
        assert (body["due"], body["dispatched"]) == (0, 0)
        api.pg_cls.assert_not_called()
        api.dispatcher_cls.assert_not_called()
        api.log.assert_called_once()
        assert api.log.call_args.kwargs["mode"] == "off"
        assert api.log.call_args.kwargs["reason"] == "tick"

    def test_off_with_dry_run_body_stays_off(self, monkeypatch):
        api = Api(monkeypatch, mode="off")
        assert api.post({"reason": "tick", "dry_run": True}).json()["mode"] == "off"

    def test_off_backfill_reports_the_window(self, monkeypatch):
        api = Api(monkeypatch, mode="off")
        body = api.post(
            {"reason": "backfill", "start_date": "2026-11-01", "end_date": "2026-11-02"}
        ).json()
        assert (body["mode"], body["reason"]) == ("off", "backfill")
        assert (body["start_date"], body["end_date"]) == ("2026-11-01", "2026-11-02")


class TestModes:
    def test_dry_run_tick(self, monkeypatch):
        api = Api(monkeypatch, mode="dry_run")
        response = api.post({"reason": "tick"})
        assert response.status_code == 200
        assert response.json()["mode"] == "dry_run"
        kwargs = api.dispatcher_cls.call_args.kwargs
        assert kwargs["mode"] == DispatchMode.DRY_RUN
        assert kwargs["tasks"] is None
        assert "mec" in kwargs["active_keys"] and "agencia_brasil" in kwargs["active_keys"]
        assert "memoria-ebc" not in kwargs["active_keys"]
        assert api.dispatcher_cls.call_args.args[0] is api.conn
        api.pg_cls.assert_called_once_with(min_connections=1, max_connections=1)
        api.pg.put_connection.assert_called_once_with(api.conn)
        api.pg.close_all.assert_called_once()
        api.log.assert_called_once()
        assert api.log.call_args.kwargs["due"] == 2

    def test_empty_body_is_a_tick(self, monkeypatch):
        api = Api(monkeypatch, mode="dry_run")
        assert api.post().status_code == 200
        api.dispatcher.tick.assert_called_once()

    def test_live_creates_the_tasks_client_and_closes_it(self, monkeypatch):
        api = Api(monkeypatch, mode="live")
        response = api.post({"reason": "tick"})
        assert response.json()["mode"] == "live"
        config = api.tasks_cls.from_config.call_args.args[0]
        assert config.queue == TASK_ENVS["SCRAPE_TASKS_QUEUE"]
        assert (
            api.dispatcher_cls.call_args.kwargs["tasks"] is api.tasks_cls.from_config.return_value
        )
        api.tasks_cls.from_config.return_value.close.assert_called_once()

    def test_dry_run_in_body_forces_dry_in_live(self, monkeypatch):
        api = Api(monkeypatch, mode="live")
        response = api.post({"reason": "tick", "dry_run": True})
        assert response.json()["mode"] == "dry_run"
        assert api.dispatcher_cls.call_args.kwargs["tasks"] is None

    def test_live_without_task_envs_is_500_without_heartbeat(self, monkeypatch):
        api = Api(monkeypatch, mode="live", task_envs=False)
        response = api.post({"reason": "tick"})
        assert response.status_code == 500
        assert "SCRAPE_TASKS_QUEUE" in response.json()["detail"]
        api.dispatcher_cls.assert_not_called()
        api.log.assert_not_called()


class TestErrors:
    def test_missing_table_is_503(self, monkeypatch):
        api = Api(monkeypatch, error=StateUnavailable("relation does not exist"))
        response = api.post({"reason": "tick"})
        assert response.status_code == 503
        assert response.json() == {"detail": "scrape_agency_state indisponível"}
        api.pg.close_all.assert_called_once()
        api.log.assert_not_called()

    def test_live_tick_without_any_task_created_is_500_without_heartbeat(self, monkeypatch):
        # Cloud Tasks recusou todas (403): o dispatch_run esconderia a ingestão parada.
        outcome = DispatchOutcome(mode="live", reason="tick", due=3, failed=3)
        outcome.agencies = [
            {"agency": k, "result": "failed", "error": "HTTP 403: PERMISSION_DENIED"}
            for k in ("mec", "mds", "mme")
        ]
        api = Api(monkeypatch, mode="live", error=DispatchFailed(outcome))
        response = api.post({"reason": "tick"})
        assert response.status_code == 500
        body = response.json()
        assert "recusou 3 de 3" in body["detail"]
        assert "HTTP 403" in body["detail"]
        assert (body["due"], body["dispatched"], body["failed"]) == (3, 0, 3)
        api.log.assert_not_called()
        api.log_failed.assert_called_once()
        kwargs = api.log_failed.call_args.kwargs
        assert (kwargs["mode"], kwargs["failed"]) == ("live", 3)
        assert "recusou 3 de 3" in kwargs["error"]
        api.pg.close_all.assert_called_once()
        api.tasks_cls.from_config.return_value.close.assert_called_once()

    def test_other_error_is_500(self, monkeypatch):
        api = Api(monkeypatch, mode="live", error=RuntimeError("connection refused"))
        response = api.post({"reason": "tick"})
        assert response.status_code == 500
        api.pg.close_all.assert_called_once()
        api.tasks_cls.from_config.return_value.close.assert_called_once()
        api.log.assert_not_called()


class TestBackfill:
    def test_backfill_is_routed_with_the_window(self, monkeypatch):
        api = Api(monkeypatch, mode="live")
        response = api.post(
            {
                "reason": "backfill",
                "agencies": ["mec"],
                "start_date": "2026-11-01",
                "end_date": "2026-11-03",
                "force": True,
            }
        )
        assert response.status_code == 200
        api.dispatcher.backfill.assert_called_once_with(
            agencies=["mec"],
            start_date=date(2026, 11, 1),
            end_date=date(2026, 11, 3),
            force=True,
            dedup_key=None,
        )
        api.dispatcher.tick.assert_not_called()
        body = response.json()
        assert (body["reason"], body["start_date"]) == ("backfill", "2026-11-01")
        assert api.log.call_args.kwargs["reason"] == "backfill"

    def test_dedup_key_is_passed_to_the_backfill(self, monkeypatch):
        api = Api(monkeypatch, mode="live")
        response = api.post(
            {
                "reason": "backfill",
                "start_date": "2026-11-01",
                "end_date": "2026-11-01",
                "dedup_key": "execucao-01",
            }
        )
        assert response.status_code == 200
        assert api.dispatcher.backfill.call_args.kwargs["dedup_key"] == "execucao-01"

    @pytest.mark.parametrize(
        "payload",
        [
            {"reason": "backfill"},
            {"reason": "backfill", "start_date": "2026-11-01"},
            {"reason": "backfill", "start_date": "2026-11-05", "end_date": "2026-11-01"},
            {"reason": "backfill", "start_date": "ontem", "end_date": "2026-11-01"},
            {"reason": "outra"},
            {
                "reason": "backfill",
                "start_date": "2026-11-01",
                "end_date": "2026-11-01",
                "dedup_key": "manual__2026-11-19T12:00:00+00:00",
            },
        ],
    )
    def test_invalid_backfill_is_422(self, monkeypatch, payload):
        api = Api(monkeypatch, mode="live")
        assert api.post(payload).status_code == 422
        api.dispatcher_cls.assert_not_called()
