"""API com o estado por agência: janela pela marca d'água, disjuntor (skip/sonda/force),
eventos scrape_attempt e GET /agencies/state."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from fastapi.testclient import TestClient

from govbr_scraper.api import app
from govbr_scraper.state.model import AgencyState, AttemptContext, CircuitState, Tier
from govbr_scraper.state.policy import BRT
from govbr_scraper.state.repository import StateUnavailable
from govbr_scraper.storage.storage_adapter import StorageAdapter

client = TestClient(app)

NOW = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)  # 10:00 BRT


class _FrozenDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is not None else NOW.replace(tzinfo=None)


def _state(key="mec", **overrides) -> AgencyState:
    return replace(AgencyState.new(key, NOW - timedelta(days=3)), **overrides)


def _metrics(agency, *, new=0, error=None):
    if error is not None:
        return {
            "articles_scraped": 0,
            "articles_saved": 0,
            "articles_new": 0,
            "agencies_processed": [],
            "errors": [error],
        }
    return {
        "articles_scraped": new,
        "articles_saved": new,
        "articles_new": new,
        "articles_skipped": 0,
        "agencies_processed": [agency],
        "errors": [],
    }


class Api:
    """StorageAdapter e managers mockados, relógio fixo e o log de scrape_attempt espionado."""

    def __init__(self, states=None, results=None, raise_for=None):
        self.storage_cls = create_autospec(StorageAdapter)
        self.storage = self.storage_cls.return_value
        self.storage.load_agency_states.return_value = states
        self.manager = MagicMock()
        self.results = results or {}

        def run_scraper(*, agencies, min_date, max_date, **kwargs):
            (agency,) = agencies
            if raise_for == agency:
                raise RuntimeError("bug no scraper")
            return self.results.get(agency) or _metrics(agency)

        self.manager.run_scraper.side_effect = run_scraper
        self.log = MagicMock()

    def post(self, path, payload):
        manager_target = (
            "govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager"
            if path.endswith("ebc")
            else "govbr_scraper.scrapers.scrape_manager.ScrapeManager"
        )
        with (
            patch("govbr_scraper.storage.StorageAdapter", self.storage_cls),
            patch(manager_target, return_value=self.manager),
            patch("govbr_scraper.api.datetime", _FrozenDatetime),
            patch("govbr_scraper.api.log_scrape_attempt", self.log),
        ):
            return client.post(path, json=payload)

    def windows(self) -> dict[str, tuple[str, str]]:
        return {
            c.kwargs["agencies"][0]: (c.kwargs["min_date"], c.kwargs["max_date"])
            for c in self.manager.run_scraper.call_args_list
        }

    def attempts(self) -> dict[str, dict]:
        return {c.kwargs["agency"]: c.kwargs for c in self.log.call_args_list}


class TestWatermarkWindow:
    def test_each_agency_gets_its_own_window(self):
        states = {
            "mec": _state("mec", last_success_at=datetime(2026, 11, 17, 15, tzinfo=BRT)),
            "mds": _state("mds", last_success_at=datetime(2026, 11, 19, 0, 30, tzinfo=BRT)),
            "antaq": _state("antaq", last_success_at=datetime(2026, 10, 28, tzinfo=BRT)),
        }
        api = Api(states=states)
        response = api.post("/scrape/agencies", {"agencies": ["mec", "mds", "antaq", "ansn"]})

        assert response.status_code == 200
        assert api.windows() == {
            "mec": ("2026-11-17", "2026-11-19"),
            "mds": ("2026-11-18", "2026-11-19"),  # 00:30 − 1 h volta ao dia anterior
            "antaq": ("2026-11-12", "2026-11-19"),  # limitada a 7 dias
            "ansn": ("2026-11-18", "2026-11-19"),  # sem linha: ontem→hoje
        }
        body = response.json()
        assert (body["start_date"], body["end_date"]) == ("2026-11-12", "2026-11-19")
        api.storage_cls.assert_called_once_with(attempt_context=AttemptContext())

    def test_high_volume_agency_window_is_limited_and_logged(self):
        """agencia_brasil (~40/dia) sem sucesso há 3 dias: a janela automática cobre ontem e
        hoje (~80 notícias), e o scrape_attempt marca window_truncated."""
        states = {
            "agencia_brasil": _state(
                "agencia_brasil",
                rate_ewma_per_day=40.0,
                last_success_at=datetime(2026, 11, 16, 9, tzinfo=BRT),
            ),
            "tvbrasil": _state(
                "tvbrasil",
                rate_ewma_per_day=15.0,
                last_success_at=datetime(2026, 11, 16, 9, tzinfo=BRT),
            ),
        }
        api = Api(states=states)
        response = api.post("/scrape/ebc", {"agencies": ["agencia_brasil", "tvbrasil"]})
        assert response.status_code == 200
        assert api.windows() == {
            "agencia_brasil": ("2026-11-18", "2026-11-19"),
            "tvbrasil": ("2026-11-16", "2026-11-19"),  # 4 dias × 15 cabem: sem corte
        }
        attempts = api.attempts()
        assert attempts["agencia_brasil"]["window_truncated"] is True
        assert attempts["tvbrasil"]["window_truncated"] is False

    def test_state_unavailable_uses_yesterday_to_today(self):
        api = Api(states=None)
        response = api.post("/scrape/agencies", {"agencies": ["mec"]})
        assert response.status_code == 200
        assert api.windows() == {"mec": ("2026-11-18", "2026-11-19")}
        assert api.attempts()["mec"]["tier"] is None

    def test_explicit_window_is_used_as_is_and_marked(self):
        api = Api(states={"mec": _state(last_success_at=NOW)})
        response = api.post(
            "/scrape/agencies",
            {"agencies": ["mec"], "start_date": "2026-11-01", "end_date": "2026-11-03"},
        )
        assert response.status_code == 200
        assert api.windows() == {"mec": ("2026-11-01", "2026-11-03")}
        api.storage_cls.assert_called_once_with(
            attempt_context=AttemptContext(explicit_window=True, forced=False)
        )
        assert api.attempts()["mec"]["explicit_window"] is True

    def test_duplicated_agencies_are_scraped_once(self):
        api = Api(states={})
        api.post("/scrape/agencies", {"agencies": ["mec", "mec"]})
        assert api.manager.run_scraper.call_count == 1

    @pytest.mark.parametrize("path", ["/scrape/agencies", "/scrape/ebc"])
    def test_always_sequential_even_if_the_request_says_otherwise(self, path):
        """No modo em lote o scrape_run de sucesso (marca d'água) sai antes do INSERT: um
        erro no INSERT perderia a janela. Uma agência por chamada não ganha nada com o lote."""
        api = Api(states={})
        agency = "agencia_brasil" if path.endswith("ebc") else "mec"
        response = api.post(path, {"agencies": [agency], "sequential": False})
        assert response.status_code == 200
        assert api.manager.run_scraper.call_args.kwargs["sequential"] is True

    def test_ebc_without_agencies_scrapes_both_sources_with_own_window(self):
        states = {
            "agencia_brasil": _state(
                "agencia_brasil", last_success_at=datetime(2026, 11, 19, 8, tzinfo=BRT)
            ),
            "tvbrasil": _state("tvbrasil", last_success_at=datetime(2026, 11, 16, 8, tzinfo=BRT)),
        }
        api = Api(states=states)
        response = api.post("/scrape/ebc", {})
        assert response.status_code == 200
        assert api.windows() == {
            "agencia_brasil": ("2026-11-19", "2026-11-19"),
            "tvbrasil": ("2026-11-16", "2026-11-19"),
        }

    def test_pool_is_closed_at_the_end(self):
        api = Api(states={})
        api.post("/scrape/agencies", {"agencies": ["mec"]})
        api.storage.close.assert_called_once()


OPEN_NOT_DUE = {
    "circuit_state": CircuitState.OPEN,
    "next_due_at": NOW + timedelta(hours=3),
    "last_error_category": "html_changed",
}


class TestCircuitBreaker:
    def test_open_circuit_not_due_is_skipped_without_scraping(self):
        api = Api(states={"mec": _state(**OPEN_NOT_DUE)})
        response = api.post("/scrape/agencies", {"agencies": ["mec"]})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "skipped"
        assert body["skipped"] == [
            {"agency": "mec", "reason": "circuit_open", "next_probe_at": "2026-11-19T16:00:00Z"}
        ]
        assert body["agencies_processed"] == []
        assert body["errors"] == []
        assert (body["start_date"], body["end_date"]) == (None, None)
        api.manager.run_scraper.assert_not_called()
        api.storage.mark_agency_probe.assert_not_called()
        attempt = api.attempts()["mec"]
        assert attempt["outcome"] == "skipped"
        assert attempt["tier"] == "h2"
        assert attempt["trigger"] == "dag"

    def test_half_open_not_due_is_also_skipped(self):
        api = Api(
            states={"mec": _state(**{**OPEN_NOT_DUE, "circuit_state": CircuitState.HALF_OPEN})}
        )
        assert api.post("/scrape/agencies", {"agencies": ["mec"]}).json()["status"] == "skipped"

    def test_due_open_circuit_is_probed(self):
        state = _state(circuit_state=CircuitState.OPEN, next_due_at=NOW - timedelta(minutes=1))
        api = Api(states={"mec": state})
        response = api.post("/scrape/agencies", {"agencies": ["mec"]})

        assert response.status_code == 200
        assert response.json()["status"] == "completed"
        api.storage.mark_agency_probe.assert_called_once_with("mec", NOW)
        assert api.manager.run_scraper.call_count == 1
        assert api.attempts()["mec"]["probe"] is True

    def test_probe_with_explicit_window_does_not_mark_half_open(self):
        state = _state(circuit_state=CircuitState.OPEN, next_due_at=NOW)
        api = Api(states={"mec": state})
        api.post("/scrape/agencies", {"agencies": ["mec"], "start_date": "2026-11-10"})
        api.storage.mark_agency_probe.assert_not_called()
        assert api.windows() == {"mec": ("2026-11-10", "2026-11-10")}

    def test_force_ignores_the_circuit(self):
        api = Api(states={"mec": _state(**OPEN_NOT_DUE)})
        response = api.post("/scrape/agencies", {"agencies": ["mec"], "force": True})

        assert response.json()["status"] == "completed"
        api.storage.mark_agency_probe.assert_not_called()
        api.storage_cls.assert_called_once_with(
            attempt_context=AttemptContext(explicit_window=False, forced=True)
        )

    def test_skipped_and_processed_is_completed(self):
        api = Api(states={"mec": _state(**OPEN_NOT_DUE), "mds": _state("mds")})
        response = api.post("/scrape/agencies", {"agencies": ["mec", "mds"]})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "completed"
        assert body["agencies_processed"] == ["mds"]
        assert [s["agency"] for s in body["skipped"]] == ["mec"]

    def test_skipped_and_failed_keeps_500(self):
        error = {"agency": "mds", "error": "boom", "category": "network_error", "retryable": True}
        api = Api(
            states={"mec": _state(**OPEN_NOT_DUE)},
            results={"mds": _metrics("mds", error=error)},
        )
        response = api.post("/scrape/agencies", {"agencies": ["mec", "mds"]})
        assert response.status_code == 500
        assert response.json()["status"] == "failed"

    def test_ebc_one_source_skipped_does_not_fail(self):
        api = Api(states={"tvbrasil": _state("tvbrasil", **OPEN_NOT_DUE)})
        response = api.post("/scrape/ebc", {"agencies": ["agencia_brasil", "tvbrasil"]})
        assert response.status_code == 200
        body = response.json()
        assert body["agencies_processed"] == ["agencia_brasil"]
        assert [s["agency"] for s in body["skipped"]] == ["tvbrasil"]


class TestScrapeAttemptEvents:
    def test_success_and_failure_fields(self):
        error = {
            "agency": "antaq",
            "error": "No articles found",
            "category": "html_changed",
            "retryable": False,
        }
        api = Api(
            states={"mec": _state(tier=Tier.H6, poll_interval_minutes=360)},
            results={"mec": _metrics("mec", new=3), "antaq": _metrics("antaq", error=error)},
        )
        response = api.post("/scrape/agencies", {"agencies": ["mec", "antaq"]})
        assert response.status_code == 207

        attempts = api.attempts()
        mec = attempts["mec"]
        assert mec["outcome"] == "success"
        assert mec["category"] is None
        assert mec["retryable"] is None
        assert mec["articles_new"] == 3
        assert mec["tier"] == "h6"
        assert mec["explicit_window"] is False
        assert mec["trigger"] == "dag"
        assert mec["duration_s"] >= 0
        assert (mec["window_start"], mec["window_end"]) == ("2026-11-18", "2026-11-19")
        antaq = attempts["antaq"]
        assert antaq["outcome"] == "failed"
        assert antaq["category"] == "html_changed"
        assert antaq["retryable"] is False
        assert antaq["articles_new"] is None
        assert antaq["tier"] == "h2"  # sem linha: a padrão que o record_scrape_run cria
        assert api.log.call_count == 2

    def test_unknown_agency_is_a_failed_attempt(self):
        api = Api(states={})
        with (
            patch("govbr_scraper.storage.StorageAdapter", api.storage_cls),
            patch("govbr_scraper.api.log_scrape_attempt", api.log),
        ):
            response = client.post("/scrape/agencies", json={"agencies": ["nao_existe_xyz"]})
        assert response.status_code == 500
        attempt = api.attempts()["nao_existe_xyz"]
        assert (attempt["outcome"], attempt["category"]) == ("failed", "unknown_agency")
        assert attempt["retryable"] is False

    def test_exception_logs_attempt_returns_500_and_closes_pool(self):
        api = Api(states={}, raise_for="mds")
        response = api.post("/scrape/agencies", {"agencies": ["mec", "mds"]})
        assert response.status_code == 500
        assert "bug no scraper" in response.json()["detail"]
        assert api.attempts()["mds"]["outcome"] == "failed"
        assert api.attempts()["mds"]["category"] == "unknown"
        api.storage.close.assert_called_once()


class TestAgenciesStateEndpoint:
    def _get(self, list_result=None, error=None):
        pg = MagicMock()
        if error is not None:
            pg.list_agency_states.side_effect = error
        else:
            pg.list_agency_states.return_value = list_result or []
        with (
            patch("govbr_scraper.storage.postgres_manager.PostgresManager", return_value=pg) as cls,
            patch("govbr_scraper.api.datetime", _FrozenDatetime),
        ):
            response = client.get("/agencies/state")
        return response, pg, cls

    def test_returns_agencies_and_summary(self, monkeypatch):
        monkeypatch.setenv("SCRAPE_DISPATCH_MODE", "dry_run")
        states = [
            _state("mec", **OPEN_NOT_DUE),
            _state("mme", last_success_at=NOW - timedelta(minutes=35)),
            _state("tvbrasil", tier=Tier.M30, poll_interval_minutes=30, last_success_at=NOW),
            _state("sri", last_success_at=NOW - timedelta(days=10)),  # inativa no YAML
        ]
        response, pg, cls = self._get(states)

        assert response.status_code == 200
        body = response.json()
        assert body["generated_at"] == "2026-11-19T13:00:00Z"
        assert body["dispatch_mode"] == "dry_run"
        by_key = {a["agency_key"]: a for a in body["agencies"]}
        assert by_key["mme"]["lag_minutes"] == 35.0
        assert by_key["mme"]["within_slo"] is True
        assert by_key["mec"]["within_slo"] is None
        assert by_key["tvbrasil"]["active"] is True
        assert by_key["sri"]["active"] is False
        assert body["summary"] == {
            "total": 4,
            "active": 3,
            "by_tier": {"m30": 1, "h2": 2, "h6": 0, "d1": 0},
            "circuit_open": 1,
            "overdue": 0,
            "slo_ok_ratio": 1.0,
        }
        cls.assert_called_once_with(min_connections=1, max_connections=1)
        pg.close_all.assert_called_once()

    def test_dispatch_mode_defaults_to_off(self, monkeypatch):
        monkeypatch.delenv("SCRAPE_DISPATCH_MODE", raising=False)
        response, _, _ = self._get([])
        assert response.json()["dispatch_mode"] == "off"

    def test_missing_table_is_503(self):
        response, pg, _ = self._get(error=StateUnavailable("relation does not exist"))
        assert response.status_code == 503
        assert response.json() == {"detail": "scrape_agency_state indisponível"}
        pg.close_all.assert_called_once()

    def test_other_error_is_500(self):
        response, _, _ = self._get(error=RuntimeError("connection refused"))
        assert response.status_code == 500


def test_watermark_dates_are_brt_dates():
    """Sanidade do relógio fixo: 13:00 UTC = 10:00 BRT de 19/11."""
    assert NOW.astimezone(BRT).date() == date(2026, 11, 19)


@pytest.mark.parametrize("path", ["/scrape/agencies", "/scrape/ebc"])
def test_force_must_be_boolean(path):
    response = client.post(path, json={"force": {"x": 1}})
    assert response.status_code == 422
