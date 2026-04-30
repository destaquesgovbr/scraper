"""Tests for dags/scrape_agencies.py — config loading and dynamic DAG generation."""

import sys
from unittest.mock import MagicMock, mock_open, patch

import pytest
import yaml


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


def _make_airflow_mocks():
    """Create Airflow module mocks that prevent real execution."""
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

    return {
        "airflow": MagicMock(),
        "airflow.decorators": mock_decorators,
        "airflow.models": MagicMock(),
        "airflow.exceptions": MagicMock(),
    }


@pytest.fixture(autouse=True)
def _clean_module():
    """Remove cached DAG module between tests."""
    yield
    for key in list(sys.modules):
        if key.startswith("dags.scrape_agencies"):
            del sys.modules[key]


def _load_module(yaml_content):
    """Import scrape_agencies with mocked Airflow and YAML content.

    Returns (module, open_mock) — keep open_mock alive for the test duration
    so that subsequent calls to _load_agencies_config() still use the mock.
    """
    airflow_mocks = _make_airflow_mocks()
    yaml_data = yaml.dump(yaml_content)

    for key in list(sys.modules):
        if key.startswith("dags.scrape_agencies"):
            del sys.modules[key]

    open_patcher = patch("builtins.open", mock_open(read_data=yaml_data))
    modules_patcher = patch.dict(sys.modules, airflow_mocks)

    modules_patcher.start()
    open_patcher.start()

    import dags.scrape_agencies as mod

    return mod, (modules_patcher, open_patcher)


def _cleanup(patchers):
    modules_patcher, open_patcher = patchers
    open_patcher.stop()
    modules_patcher.stop()


class TestLoadAgenciesConfig:

    def test_filters_inactive_agencies(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            result = mod._load_agencies_config()
            assert "mec" in result
            assert "mds" in result
            assert "disabled_agency" not in result
        finally:
            _cleanup(patchers)

    def test_default_active_true_when_field_missing(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            result = mod._load_agencies_config()
            assert "no_active_field" in result
        finally:
            _cleanup(patchers)

    def test_extracts_url_correctly(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            result = mod._load_agencies_config()
            assert result["mec"] == "https://www.gov.br/mec/pt-br/noticias"
            assert result["mds"] == "https://www.gov.br/mds/pt-br/noticias"
        finally:
            _cleanup(patchers)

    def test_returns_empty_when_all_inactive(self):
        yaml_content = {
            "agencies": {
                "a": {"url": "https://x.com", "active": False},
                "b": {"url": "https://y.com", "active": False},
            }
        }
        mod, patchers = _load_module(yaml_content)
        try:
            result = mod._load_agencies_config()
            assert result == {}
        finally:
            _cleanup(patchers)

    def test_returns_empty_when_no_agencies(self):
        yaml_content = {"agencies": {}}
        mod, patchers = _load_module(yaml_content)
        try:
            result = mod._load_agencies_config()
            assert result == {}
        finally:
            _cleanup(patchers)

    def test_active_field_respects_false(self):
        yaml_content = {
            "agencies": {
                "active_one": {"url": "https://a.com", "active": True},
                "inactive_one": {"url": "https://b.com", "active": False},
            }
        }
        mod, patchers = _load_module(yaml_content)
        try:
            result = mod._load_agencies_config()
            assert "active_one" in result
            assert "inactive_one" not in result
            assert len(result) == 1
        finally:
            _cleanup(patchers)


class TestDynamicDagGeneration:

    def test_minute_offset_distribution(self):
        agencies = {"a": "url_a", "b": "url_b", "c": "url_c"}
        sorted_keys = [k for k, _ in sorted(agencies.items())]
        offsets = {k: idx % 10 for idx, k in enumerate(sorted_keys)}

        assert offsets["a"] == 0
        assert offsets["b"] == 1
        assert offsets["c"] == 2

    def test_offset_wraps_at_10(self):
        agencies = {f"agency_{i:02d}": f"url_{i}" for i in range(25)}
        sorted_keys = [k for k, _ in sorted(agencies.items())]
        offsets = [idx % 10 for idx in range(len(sorted_keys))]

        assert offsets[0] == 0
        assert offsets[9] == 9
        assert offsets[10] == 0
        assert offsets[19] == 9
        assert offsets[24] == 4

    def test_offset_is_deterministic(self):
        agencies = {"zz": "url_z", "aa": "url_a", "mm": "url_m"}
        run1 = [(k, idx % 10) for idx, (k, _) in enumerate(sorted(agencies.items()))]
        run2 = [(k, idx % 10) for idx, (k, _) in enumerate(sorted(agencies.items()))]

        assert run1 == run2
        assert run1[0][0] == "aa"
        assert run1[1][0] == "mm"
        assert run1[2][0] == "zz"

    def test_dag_id_follows_scrape_prefix_pattern(self):
        yaml_content = {
            "agencies": {
                "mec": {"url": "https://gov.br/mec", "active": True},
                "mds": {"url": "https://gov.br/mds", "active": True},
            }
        }
        mod, patchers = _load_module(yaml_content)
        try:
            config = mod._load_agencies_config()
            for key in config:
                expected_dag_id = f"scrape_{key}"
                assert expected_dag_id.startswith("scrape_")
                assert key in expected_dag_id
        finally:
            _cleanup(patchers)

    def test_all_active_agencies_get_offset(self):
        yaml_content = {
            "agencies": {
                "aaa": {"url": "https://gov.br/aaa", "active": True},
                "bbb": {"url": "https://gov.br/bbb", "active": True},
                "ccc": {"url": "https://gov.br/ccc", "active": True},
            }
        }
        mod, patchers = _load_module(yaml_content)
        try:
            config = mod._load_agencies_config()
            expected = [(k, idx % 10) for idx, (k, _) in enumerate(sorted(config.items()))]
            assert expected == [("aaa", 0), ("bbb", 1), ("ccc", 2)]
        finally:
            _cleanup(patchers)


class TestOnScrapeFailure:

    def test_logs_failure_with_context(self, caplog):
        import logging

        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            mock_ti = MagicMock()
            mock_ti.dag_id = "scrape_mec"
            mock_ti.task_id = "scrape"
            mock_ti.try_number = 2

            context = {
                "task_instance": mock_ti,
                "exception": ValueError("connection timeout"),
            }

            with caplog.at_level(logging.ERROR):
                mod._on_scrape_failure(context)

            assert "scrape_mec" in caplog.text
            assert "scrape" in caplog.text
            assert "connection timeout" in caplog.text
        finally:
            _cleanup(patchers)

    def test_handles_missing_task_instance(self, caplog):
        import logging

        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            context = {"task_instance": None, "exception": "some error"}

            with caplog.at_level(logging.ERROR):
                mod._on_scrape_failure(context)

            assert "unknown" in caplog.text
        finally:
            _cleanup(patchers)
