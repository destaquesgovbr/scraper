"""Tests for dags/scraper_coverage_report.py — alerta de cobertura baixa e canais."""

import sys
from unittest.mock import MagicMock, patch

import pytest

TOPIC = "projects/inspire-7-finep/topics/dgb.ops.alerts"


@pytest.fixture
def loaded():
    registry: dict[str, MagicMock] = {}

    def fake_dag(**kwargs):
        def wrapper(fn):
            return fn

        return wrapper

    def fake_task(fn):
        mock_callable = MagicMock(name=f"task_{fn.__name__}")
        mock_callable._original_fn = fn
        registry[fn.__name__] = mock_callable
        return mock_callable

    decorators = MagicMock()
    decorators.dag = fake_dag
    decorators.task = fake_task
    notify = MagicMock()
    notify.default_alert_topic = MagicMock(return_value=TOPIC)
    notify.send_alert = MagicMock(return_value=True)
    scraper_pkg = MagicMock()
    scraper_pkg.notify = notify
    mocks = {
        "airflow": MagicMock(),
        "airflow.decorators": decorators,
        "airflow.models": MagicMock(),
        "scraper": scraper_pkg,
        "scraper.notify": notify,
    }
    for key in list(sys.modules):
        if key.startswith("dags.scraper_coverage_report"):
            del sys.modules[key]
    with patch.dict(sys.modules, mocks):
        import dags.scraper_coverage_report  # noqa: F401

        yield registry, mocks
    for key in list(sys.modules):
        if key.startswith("dags.scraper_coverage_report"):
            del sys.modules[key]


REPORT = {
    "total_active": 150,
    "agencies_scraped": 90,
    "agencies_with_errors": 20,
    "total_articles": 300,
    "coverage_ratio": 0.6,
    "top_errors": [{"error_category": "url_broken", "count": 12}],
}


def _variables(values):
    def _get(key, default_var=None):
        return values.get(key, default_var)

    return _get


def test_low_coverage_alert_goes_to_ops_alerts_topic(loaded):
    registry, mocks = loaded
    mocks["airflow.models"].Variable.get = MagicMock(side_effect=_variables({}))
    registry["alert_on_low_coverage"]._original_fn(REPORT)

    send_alert = mocks["scraper.notify"].send_alert
    send_alert.assert_called_once()
    kwargs = send_alert.call_args.kwargs
    assert kwargs["pubsub_topic"] == TOPIC
    assert kwargs["title"] == "Scraper: cobertura baixa"
    assert kwargs["severity"] == "warning"
    assert kwargs["source"] == "scraper_coverage_report (Airflow)"
    assert "Cobertura 24h: <b>60%</b>" in kwargs["message"]


def test_topic_variable_overrides_default(loaded):
    registry, mocks = loaded
    mocks["airflow.models"].Variable.get = MagicMock(
        side_effect=_variables({"scraper_alert_pubsub_topic": "projects/outro-proj/topics/t1"})
    )
    registry["alert_on_low_coverage"]._original_fn(REPORT)
    kwargs = mocks["scraper.notify"].send_alert.call_args.kwargs
    assert kwargs["pubsub_topic"] == "projects/outro-proj/topics/t1"


def test_no_alert_when_coverage_ok(loaded):
    registry, mocks = loaded
    mocks["airflow.models"].Variable.get = MagicMock(side_effect=_variables({}))
    registry["alert_on_low_coverage"]._original_fn({**REPORT, "coverage_ratio": 0.95})
    mocks["scraper.notify"].send_alert.assert_not_called()
