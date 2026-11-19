"""POST /scrape/agency (alvo das tarefas do Cloud Tasks): status HTTP, roteamento, disjuntor,
payload inválido sem 422 e os cabeçalhos do Cloud Tasks no scrape_attempt."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from fastapi.testclient import TestClient

from govbr_scraper.api import app
from govbr_scraper.state.model import AgencyState, AttemptContext, CircuitState
from govbr_scraper.state.policy import BRT
from govbr_scraper.storage.storage_adapter import StorageAdapter

client = TestClient(app)

NOW = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)  # 10:00 BRT
TASK_HEADERS = {
    "X-CloudTasks-TaskName": "scrape-mec-202611191000",
    "X-CloudTasks-TaskRetryCount": "2",
    "X-CloudTasks-QueueName": "scrape-govbr",
}


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


def _state(key="mec", **overrides) -> AgencyState:
    return replace(AgencyState.new(key, NOW - timedelta(days=3)), **overrides)


def _ok(agency, new=0):
    return {
        "articles_scraped": new,
        "articles_saved": new,
        "articles_new": new,
        "articles_skipped": 0,
        "agencies_processed": [agency],
        "errors": [],
    }


def _error(agency, category, retryable):
    return {
        "articles_scraped": 0,
        "articles_saved": 0,
        "articles_new": 0,
        "agencies_processed": [],
        "errors": [
            {"agency": agency, "error": "boom", "category": category, "retryable": retryable}
        ],
    }


class Api:
    def __init__(self, states=None, result=None, raise_exc=None):
        self.storage_cls = create_autospec(StorageAdapter)
        self.storage = self.storage_cls.return_value
        self.storage.load_agency_states.return_value = states if states is not None else {}
        self.gov = MagicMock(name="ScrapeManager")
        self.ebc = MagicMock(name="EBCScrapeManager")
        self.log = MagicMock()
        self.result = result
        self.raise_exc = raise_exc

        def run_scraper(*, agencies, min_date, max_date, **kwargs):
            if self.raise_exc:
                raise self.raise_exc
            return self.result or _ok(agencies[0])

        self.gov.run_scraper.side_effect = run_scraper
        self.ebc.run_scraper.side_effect = run_scraper

    def post(self, payload=None, headers=None, raw=None):
        with (
            patch("govbr_scraper.storage.StorageAdapter", self.storage_cls),
            patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", return_value=self.gov),
            patch(
                "govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", return_value=self.ebc
            ),
            patch("govbr_scraper.api.datetime", _FrozenDatetime),
            patch("govbr_scraper.api.log_scrape_attempt", self.log),
        ):
            if raw is not None:
                return client.post("/scrape/agency", content=raw, headers=headers or {})
            return client.post("/scrape/agency", json=payload, headers=headers or {})

    def attempt(self) -> dict:
        (call,) = self.log.call_args_list
        return call.kwargs


class TestSuccessAndRouting:
    def test_govbr_agency_uses_scrape_manager_and_watermark(self):
        api = Api(
            states={"mec": _state(last_success_at=datetime(2026, 11, 17, 15, tzinfo=BRT))},
            result=_ok("mec", new=2),
        )
        response = api.post({"agency": "mec"}, headers=TASK_HEADERS)
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "completed"
        assert (body["agency"], body["trigger"], body["retryable"]) == ("mec", "task", None)
        assert (body["start_date"], body["end_date"]) == ("2026-11-17", "2026-11-19")
        assert body["articles_new"] == 2
        kwargs = api.gov.run_scraper.call_args.kwargs
        assert kwargs["agencies"] == ["mec"]
        assert (kwargs["min_date"], kwargs["max_date"]) == ("2026-11-17", "2026-11-19")
        assert kwargs["sequential"] is True
        assert kwargs["allow_update"] is False
        api.ebc.run_scraper.assert_not_called()
        api.storage_cls.assert_called_once_with(attempt_context=AttemptContext())
        api.storage.close.assert_called_once()

    def test_ebc_agency_uses_ebc_manager(self):
        api = Api(result=_ok("agencia_brasil"))
        response = api.post({"agency": "agencia_brasil"})
        assert response.status_code == 200
        api.ebc.run_scraper.assert_called_once()
        api.gov.run_scraper.assert_not_called()

    def test_task_headers_go_to_scrape_attempt(self):
        api = Api()
        api.post({"agency": "mec"}, headers=TASK_HEADERS)
        attempt = api.attempt()
        assert attempt["task_name"] == "scrape-mec-202611191000"
        assert attempt["task_retry"] == 2
        assert attempt["trigger"] == "task"
        assert attempt["outcome"] == "success"

    def test_without_headers_there_are_no_task_fields(self):
        api = Api()
        api.post({"agency": "mec"})
        assert "task_name" not in api.attempt()
        assert "task_retry" not in api.attempt()

    def test_explicit_window_is_backfill_and_does_not_touch_state(self):
        api = Api()
        response = api.post({"agency": "mec", "start_date": "2026-11-01", "end_date": "2026-11-03"})
        assert response.status_code == 200
        assert response.json()["trigger"] == "backfill"
        kwargs = api.gov.run_scraper.call_args.kwargs
        assert (kwargs["min_date"], kwargs["max_date"]) == ("2026-11-01", "2026-11-03")
        api.storage_cls.assert_called_once_with(
            attempt_context=AttemptContext(explicit_window=True, forced=False)
        )
        assert api.attempt()["trigger"] == "backfill"
        assert api.attempt()["explicit_window"] is True

    def test_explicit_trigger_wins(self):
        api = Api()
        response = api.post({"agency": "mec", "start_date": "2026-11-01", "trigger": "task"})
        assert response.json()["trigger"] == "task"
        assert response.json()["end_date"] == "2026-11-01"

    def test_force_and_allow_update_are_passed(self):
        api = Api(
            states={
                "mec": _state(circuit_state=CircuitState.OPEN, next_due_at=NOW + timedelta(hours=5))
            }
        )
        response = api.post({"agency": "mec", "force": True, "allow_update": True})
        assert response.status_code == 200
        assert response.json()["status"] == "completed"
        assert api.gov.run_scraper.call_args.kwargs["allow_update"] is True
        api.storage_cls.assert_called_once_with(
            attempt_context=AttemptContext(explicit_window=False, forced=True)
        )


class TestErrors:
    @pytest.mark.parametrize(
        "category", ["html_changed", "url_broken", "access_denied", "anti_bot"]
    )
    def test_permanent_error_is_200_without_retry(self, category):
        api = Api(result=_error("mec", category, False))
        response = api.post({"agency": "mec"})
        assert response.status_code == 200
        body = response.json()
        assert (body["status"], body["retryable"]) == ("failed", False)
        assert body["errors"][0]["category"] == category
        assert "Erro permanente" in body["message"]

    @pytest.mark.parametrize("category", ["network_error", "unknown", None])
    def test_transient_error_is_503(self, category):
        api = Api(result=_error("mec", category, True))
        response = api.post({"agency": "mec"}, headers=TASK_HEADERS)
        assert response.status_code == 503
        body = response.json()
        assert (body["status"], body["retryable"]) == ("failed", True)
        assert api.attempt()["outcome"] == "failed"
        api.storage.close.assert_called_once()

    def test_unexpected_exception_is_503_and_closes_resources(self):
        api = Api(raise_exc=RuntimeError("bug no scraper"))
        response = api.post({"agency": "mec"})
        assert response.status_code == 503
        assert response.json()["retryable"] is True
        assert "bug no scraper" in response.json()["message"]
        api.storage.close.assert_called_once()

    def test_storage_init_failure_is_503(self):
        api = Api()
        api.storage_cls.side_effect = RuntimeError("connection refused")
        response = api.post({"agency": "mec"})
        assert response.status_code == 503


class TestCircuit:
    def test_open_circuit_not_due_is_skipped_with_200(self):
        probe_at = NOW + timedelta(hours=4)
        api = Api(states={"mec": _state(circuit_state=CircuitState.OPEN, next_due_at=probe_at)})
        response = api.post({"agency": "mec"}, headers=TASK_HEADERS)
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "skipped"
        assert body["skipped"] == [
            {"agency": "mec", "reason": "circuit_open", "next_probe_at": "2026-11-19T17:00:00Z"}
        ]
        api.gov.run_scraper.assert_not_called()
        attempt = api.attempt()
        assert (attempt["outcome"], attempt["task_name"]) == ("skipped", "scrape-mec-202611191000")

    def test_open_circuit_due_is_probed(self):
        api = Api(states={"mec": _state(circuit_state=CircuitState.OPEN, next_due_at=NOW)})
        response = api.post({"agency": "mec"})
        assert response.status_code == 200
        api.storage.mark_agency_probe.assert_called_once_with("mec", NOW)
        assert api.attempt()["probe"] is True


class TestUnknownAgency:
    @pytest.mark.parametrize("agency", ["nao_existe", "memoria-ebc"])  # fora do YAML / inativa
    def test_unknown_or_inactive_is_200(self, agency):
        api = Api()
        response = api.post({"agency": agency})
        assert response.status_code == 200
        body = response.json()
        assert (body["status"], body["retryable"]) == ("unknown_agency", False)
        assert body["errors"][0]["category"] == "unknown_agency"
        api.storage_cls.assert_not_called()
        attempt = api.attempt()
        assert (attempt["outcome"], attempt["category"], attempt["retryable"]) == (
            "failed",
            "unknown_agency",
            False,
        )


class TestInvalidPayload:
    @pytest.mark.parametrize(
        "raw",
        [
            b"",
            b"{not json",
            b"[]",
            b'"mec"',
            b"{}",
            b'{"agency": ""}',
            b'{"agency": 123}',
            b'{"agency": "mec", "start_date": "2026-13-01"}',
            b'{"agency": "mec", "end_date": "2026-11-01"}',
            b'{"agency": "mec", "start_date": "2026-11-05", "end_date": "2026-11-01"}',
            b'{"agency": "mec", "trigger": "dag"}',
            b"\xff\xfe",
        ],
    )
    def test_never_422(self, raw):
        api = Api()
        response = api.post(raw=raw, headers={"Content-Type": "application/json", **TASK_HEADERS})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "invalid_payload"
        assert body["retryable"] is False
        api.storage_cls.assert_not_called()
        api.log.assert_not_called()

    def test_unknown_fields_are_ignored(self):
        api = Api()
        response = api.post({"agency": "mec", "extra": 1})
        assert response.json()["status"] == "completed"
