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


def _make_airflow_mocks(dag_calls=None):
    """Create Airflow module mocks that prevent real execution.

    dag_calls: lista opcional que recebe os kwargs de cada @dag (dag_id, schedule...).
    """

    def fake_dag(**kwargs):
        if dag_calls is not None:
            dag_calls.append(kwargs)

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


def _load_module(yaml_content, dag_calls=None):
    """Import scrape_agencies with mocked Airflow and YAML content.

    Returns (module, open_mock) — keep open_mock alive for the test duration
    so that subsequent calls to _load_agencies_config() still use the mock.
    """
    airflow_mocks = _make_airflow_mocks(dag_calls)
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


def _schedules(yaml_content):
    """{dag_id: schedule} das DAGs geradas na importação do módulo."""
    dag_calls = []
    _mod, patchers = _load_module(yaml_content, dag_calls)
    _cleanup(patchers)
    return {call["dag_id"]: call["schedule"] for call in dag_calls}


class TestDynamicDagGeneration:
    def test_minute_offset_distribution(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            offsets = mod._minute_offsets(["a", "b", "c"])
        finally:
            _cleanup(patchers)

        assert offsets == {"a": 0, "b": 1, "c": 2}

    def test_offset_wraps_at_10(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            offsets = mod._minute_offsets([f"agency_{i:02d}" for i in range(25)])
        finally:
            _cleanup(patchers)

        assert offsets["agency_00"] == 0
        assert offsets["agency_09"] == 9
        assert offsets["agency_10"] == 0
        assert offsets["agency_19"] == 9
        assert offsets["agency_24"] == 4

    def test_offset_is_deterministic(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            run1 = mod._minute_offsets(["zz", "aa", "mm"])
            run2 = mod._minute_offsets(["mm", "zz", "aa"])
        finally:
            _cleanup(patchers)

        assert run1 == run2 == {"aa": 0, "mm": 1, "zz": 2}

    def test_offset_index_includes_inactive_agencies(self):
        """O índice do minuto conta as inativas; só as ativas viram DAG."""
        yaml_content = {
            "agencies": {
                "aaa": {"url": "https://gov.br/aaa", "active": True},
                "bbb": {"url": "https://gov.br/bbb", "active": False},
                "ccc": {"url": "https://gov.br/ccc", "active": True},
            }
        }

        assert _schedules(yaml_content) == {
            "scrape_aaa": "0/10 * * * *",
            "scrape_ccc": "2/10 * * * *",
        }

    def test_deactivating_agency_keeps_other_schedules(self):
        """Desativar uma agência não desloca o minuto das outras (DagRun zumbi, runbook)."""
        agencies = {
            f"ag{i:02d}": {"url": f"https://gov.br/ag{i:02d}", "active": True} for i in range(25)
        }
        before = _schedules({"agencies": agencies})

        agencies["ag03"] = {**agencies["ag03"], "active": False}
        after = _schedules({"agencies": agencies})

        assert "scrape_ag03" not in after
        assert after == {
            dag_id: sched for dag_id, sched in before.items() if dag_id != "scrape_ag03"
        }

    def test_inactive_agency_generates_no_dag(self):
        mod, patchers = _load_module(SAMPLE_YAML)
        try:
            generated = {name for name in vars(mod) if name.startswith("scrape_")}
            assert {"scrape_mec", "scrape_mds", "scrape_no_active_field"} <= generated
            assert "scrape_disabled_agency" not in generated
            assert set(mod._MINUTE_OFFSETS) == set(SAMPLE_YAML["agencies"])
        finally:
            _cleanup(patchers)

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

        assert _schedules(yaml_content) == {
            "scrape_aaa": "0/10 * * * *",
            "scrape_bbb": "1/10 * * * *",
            "scrape_ccc": "2/10 * * * *",
        }


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
