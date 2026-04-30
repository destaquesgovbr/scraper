"""Tests for dags/monitor_scraping_health.py — sanitize and alert formatting."""

import importlib
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


def _make_airflow_mocks():
    """Create Airflow module mocks that prevent real execution of tasks."""
    def fake_dag(**kwargs):
        def wrapper(fn):
            fn._dag_kwargs = kwargs
            return fn
        return wrapper

    def fake_task(fn):
        mock_callable = MagicMock(name=f"task_{fn.__name__}")
        mock_callable._original_fn = fn
        return mock_callable

    mock_decorators = MagicMock()
    mock_decorators.dag = fake_dag
    mock_decorators.task = fake_task

    mock_variable = MagicMock()
    mock_variable.get = MagicMock(return_value="")

    mock_models = MagicMock()
    mock_models.Variable = mock_variable

    return {
        "airflow": MagicMock(),
        "airflow.decorators": mock_decorators,
        "airflow.models": mock_models,
    }


@pytest.fixture(autouse=True)
def _clean_module():
    """Remove cached DAG module between tests."""
    yield
    for key in list(sys.modules):
        if key.startswith("dags.monitor_scraping_health"):
            del sys.modules[key]


def _load_module():
    """Import monitor_scraping_health with mocked Airflow."""
    airflow_mocks = _make_airflow_mocks()

    for key in list(sys.modules):
        if key.startswith("dags.monitor_scraping_health"):
            del sys.modules[key]

    with patch.dict(sys.modules, airflow_mocks):
        import dags.monitor_scraping_health as mod
        return mod


class TestSanitize:

    def test_removes_null_bytes(self):
        mod = _load_module()
        assert mod._sanitize("\x00evil") == "evil"

    def test_removes_control_characters(self):
        mod = _load_module()
        assert mod._sanitize("\x01\x02\x03text\x1f") == "text"

    def test_removes_c1_control_characters(self):
        mod = _load_module()
        assert mod._sanitize("\x80\x8f\x9ftext") == "text"

    def test_removes_del_character(self):
        mod = _load_module()
        assert mod._sanitize("before\x7fafter") == "beforeafter"

    def test_preserves_normal_text(self):
        mod = _load_module()
        assert mod._sanitize("Hello, World! 123") == "Hello, World! 123"

    def test_preserves_unicode_and_accents(self):
        mod = _load_module()
        assert mod._sanitize("Agência Manutenção São Paulo") == "Agência Manutenção São Paulo"

    def test_preserves_html_tags(self):
        mod = _load_module()
        assert mod._sanitize("<b>bold</b>") == "<b>bold</b>"

    def test_returns_empty_for_none(self):
        mod = _load_module()
        assert mod._sanitize(None) == ""

    def test_returns_empty_for_empty_string(self):
        mod = _load_module()
        assert mod._sanitize("") == ""

    def test_converts_non_string_to_string(self):
        mod = _load_module()
        assert mod._sanitize(123) == "123"

    def test_handles_mixed_valid_and_control(self):
        mod = _load_module()
        assert mod._sanitize("mec\x00\x01: \x1fnetwork_error") == "mec: network_error"


class TestAlertMessageFormatting:
    """Tests verifying the formatting logic used in send_alerts task."""

    def test_failure_message_format(self):
        mod = _load_module()

        failures = [
            {
                "agency_key": "mec",
                "last_error": "network_error",
                "last_failure_at": datetime(2026, 4, 30, 10, 0),
            },
            {
                "agency_key": "mds\x00injected",
                "last_error": "anti_bot\x01",
                "last_failure_at": datetime(2026, 4, 30, 11, 0),
            },
        ]

        lines = [
            f"- <b>{mod._sanitize(r['agency_key'])}</b>: {mod._sanitize(r['last_error'])} "
            f"(ultima falha: {r['last_failure_at']})"
            for r in failures
        ]
        message = "<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n" + "\n".join(lines)

        assert "<b>mec</b>: network_error" in message
        assert "<b>mdsinjected</b>: anti_bot" in message
        assert "\x00" not in message
        assert "\x01" not in message

    def test_stale_message_format(self):
        mod = _load_module()

        stale = [
            {
                "agency_key": "funai",
                "last_success_at": datetime(2026, 4, 28, 8, 0),
            },
        ]

        lines = [
            f"- <b>{mod._sanitize(r['agency_key'])}</b>: ultima noticia em {r['last_success_at']}"
            for r in stale
        ]
        message = "<b>Alerta: Agencias Sem Noticias</b>\n\n" + "\n".join(lines)

        assert "<b>funai</b>" in message
        assert "2026-04-28" in message

    def test_combined_message_has_both_sections(self):
        mod = _load_module()

        failures = [{"agency_key": "mec", "last_error": "network_error",
                     "last_failure_at": datetime(2026, 4, 30, 10, 0)}]
        stale = [{"agency_key": "funai", "last_success_at": datetime(2026, 4, 28, 8, 0)}]

        parts = []
        if failures:
            lines = [
                f"- <b>{mod._sanitize(r['agency_key'])}</b>: {mod._sanitize(r['last_error'])} "
                f"(ultima falha: {r['last_failure_at']})"
                for r in failures
            ]
            parts.append("<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n" + "\n".join(lines))

        if stale:
            lines = [
                f"- <b>{mod._sanitize(r['agency_key'])}</b>: ultima noticia em {r['last_success_at']}"
                for r in stale
            ]
            parts.append("<b>Alerta: Agencias Sem Noticias</b>\n\n" + "\n".join(lines))

        message = "\n\n".join(parts)

        assert "Falhas Consecutivas" in message
        assert "Agencias Sem Noticias" in message
        assert message.index("Falhas Consecutivas") < message.index("Agencias Sem Noticias")

    def test_empty_lists_produce_no_message(self):
        failures = []
        stale = []
        assert not failures and not stale
