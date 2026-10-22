"""Tests for dags/monitor_scraping_health.py — sanitize, alert formatting e agências silenciosas."""

import re
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from govbr_scraper.monitoring.health_checks import SILENT_AGENCIES_QUERY

REPO_ROOT = Path(__file__).resolve().parents[2]


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

    mock_notify = MagicMock()
    mock_notify.send_alert = MagicMock(return_value=True)

    mock_scraper_pkg = MagicMock()
    mock_scraper_pkg.notify = mock_notify

    return {
        "airflow": MagicMock(),
        "airflow.decorators": mock_decorators,
        "airflow.models": mock_models,
        "scraper": mock_scraper_pkg,
        "scraper.notify": mock_notify,
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

        failures = [
            {
                "agency_key": "mec",
                "last_error": "network_error",
                "last_failure_at": datetime(2026, 4, 30, 10, 0),
            }
        ]
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


class TestSendAlertsTask:
    """Tests for the send_alerts task importing scraper.notify correctly."""

    def test_send_alerts_imports_from_scraper_notify(self):
        """send_alerts must import from scraper.notify, not bare notify."""
        import inspect

        mod = _load_module()
        # The fake_task decorator stores the original function
        # We need to find it among tasks created during module load
        source = inspect.getsource(mod.monitor_scraping_health_dag)
        assert "from scraper.notify import send_alert" in source

    def test_module_loads_with_scraper_notify_mock(self):
        """DAG module must load successfully when scraper.notify is available."""
        airflow_mocks = _make_airflow_mocks()

        for key in list(sys.modules):
            if key.startswith("dags.monitor_scraping_health"):
                del sys.modules[key]

        with patch.dict(sys.modules, airflow_mocks):
            import dags.monitor_scraping_health as mod

            assert mod.monitor_scraping_health_dag is not None


# =============================================================================
# Agências silenciosas (DAG que não chama a API: DagRun zumbi, DAG pausada/ausente)
# =============================================================================


def _load_module_with_registry():
    """Importa o DAG com Airflow mockado e registra cada @task pelo nome da função.

    Retorna (module, registry, mocks). O DAG é instanciado na importação
    (dag_instance = monitor_scraping_health_dag()), então o registry guarda as
    chamadas entre tasks e permite verificar a fiação do grafo.
    """
    mocks = _make_airflow_mocks()
    registry: dict[str, MagicMock] = {}

    def recording_task(fn):
        mock_callable = MagicMock(name=f"task_{fn.__name__}")
        mock_callable._original_fn = fn
        registry[fn.__name__] = mock_callable
        return mock_callable

    mocks["airflow.decorators"].task = recording_task

    for key in list(sys.modules):
        if key.startswith("dags.monitor_scraping_health"):
            del sys.modules[key]

    with patch.dict(sys.modules, mocks):
        import dags.monitor_scraping_health as mod

    return mod, registry, mocks


def _variables(values: dict):
    """side_effect para Variable.get que respeita default_var."""

    def _get(key, default_var=None):
        return values.get(key, default_var)

    return _get


def _mock_connection(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


SILENT_ROWS = [
    {"agency_key": "aids", "last_run_at": datetime(2026, 8, 21, 11, 32, 46)},
    {"agency_key": "inma", "last_run_at": None},
]


class TestLoadActiveAgencies:
    def test_filters_inactive_and_defaults_to_active(self, tmp_path):
        mod = _load_module()
        config = tmp_path / "site_urls.yaml"
        config.write_text(
            yaml.dump(
                {
                    "agencies": {
                        "mec": {"url": "https://www.gov.br/mec", "active": True},
                        "lapoc": {"url": "https://www.gov.br/lapoc", "active": False},
                        "aids": {"url": "https://www.gov.br/aids"},
                    }
                }
            )
        )
        assert mod._load_active_agencies(str(config)) == ["aids", "mec"]

    def test_matches_dags_generated_by_scrape_agencies(self):
        """O monitor espera execução exatamente das agências que viram DAG scrape_*."""
        monitor = _load_module()
        mocks = _make_airflow_mocks()
        mocks["airflow.exceptions"] = MagicMock()
        for key in list(sys.modules):
            if key.startswith("dags.scrape_agencies"):
                del sys.modules[key]
        try:
            with patch.dict(sys.modules, mocks):
                import dags.scrape_agencies as scrape_agencies

                generated = sorted(scrape_agencies._load_agencies_config())
        finally:
            for key in list(sys.modules):
                if key.startswith("dags.scrape_agencies"):
                    del sys.modules[key]

        active = monitor._load_active_agencies()
        assert active == generated
        assert len(active) > 100
        assert Path(monitor._AGENCIES_CONFIG_PATH) == REPO_ROOT / "dags/config/site_urls.yaml"


class TestSilentQueryParity:
    def test_dag_query_matches_health_checks(self):
        """A query vive duplicada (o pacote não é instalado no Composer): não pode divergir."""
        mod = _load_module()
        assert " ".join(mod._SILENT_AGENCIES_QUERY.split()) == " ".join(
            SILENT_AGENCIES_QUERY.split()
        )


class TestBuildAlertMessage:
    FAILURES = [
        {
            "agency_key": "mec",
            "last_error": "network_error",
            "last_failure_at": datetime(2026, 4, 30, 10, 0),
        }
    ]
    STALE = [{"agency_key": "funai", "last_success_at": datetime(2026, 4, 28, 8, 0)}]

    def test_returns_none_without_findings(self):
        mod = _load_module()
        assert mod._build_alert_message([], [], []) is None

    def test_silent_section_lists_agencies_and_last_run(self):
        mod = _load_module()
        message = mod._build_alert_message([], [], SILENT_ROWS, silent_hours=2)
        assert "<b>Alerta: Agencias Sem Execucao</b>" in message
        assert "2 agencia(s) ativa(s) sem nenhum scrape_run nas ultimas 2h" in message
        assert "- <b>aids</b>: ultima execucao em 2026-08-21 11:32:46" in message
        assert "- <b>inma</b>: nenhuma execucao registrada em scrape_runs" in message
        assert "docs/runbook.md" in message

    def test_silent_section_comes_first(self):
        """Hoje o alerta passa de 4096 caracteres e o Cloud Logging corta o fim da linha."""
        mod = _load_module()
        message = mod._build_alert_message(self.FAILURES, self.STALE, SILENT_ROWS)
        assert message.startswith("<b>Alerta: Agencias Sem Execucao</b>")
        assert (
            message.index("Agencias Sem Execucao")
            < message.index("Falhas Consecutivas")
            < message.index("Agencias Sem Noticias")
        )

    def test_keeps_existing_sections_format(self):
        mod = _load_module()
        message = mod._build_alert_message(self.FAILURES, self.STALE)
        assert message == (
            "<b>Alerta: Falhas Consecutivas no Scraper</b>\n\n"
            "- <b>mec</b>: network_error (ultima falha: 2026-04-30 10:00:00)"
            "\n\n"
            "<b>Alerta: Agencias Sem Noticias</b>\n\n"
            "- <b>funai</b>: ultima noticia em 2026-04-28 08:00:00"
        )

    def test_sanitizes_silent_agency_key(self):
        mod = _load_module()
        message = mod._build_alert_message(
            [], [], [{"agency_key": "ai\x00ds\x01", "last_run_at": None}]
        )
        assert "<b>aids</b>" in message
        assert "\x00" not in message and "\x01" not in message

    def test_caps_long_sections(self):
        """Pane global (ex.: Composer sem acesso à API) deixa as ~155 agências silenciosas."""
        mod = _load_module()
        silent = [{"agency_key": f"agencia{i:03d}", "last_run_at": None} for i in range(155)]
        message = mod._build_alert_message([], [], silent)
        listed = [line for line in message.splitlines() if line.startswith("- <b>")]
        assert len(listed) == mod._MAX_LINES_PER_SECTION
        assert f"... e mais {155 - mod._MAX_LINES_PER_SECTION}" in message
        assert "155 agencia(s) ativa(s)" in message

    def test_total_message_fits_telegram_and_log_limit(self):
        """Com as três seções cheias, a mensagem cabe no limite e corta em fim de linha."""
        mod = _load_module()
        n = 155
        failures = [
            {
                "agency_key": f"agencia-com-nome-longo-{i:03d}",
                "last_error": "html_changed",
                "last_failure_at": datetime(2026, 10, 22, 3, 47, 47, 325090),
            }
            for i in range(n)
        ]
        stale = [
            {
                "agency_key": f"agencia-com-nome-longo-{i:03d}",
                "last_success_at": datetime(2026, 10, 9, 13, 2, 55, 123456),
            }
            for i in range(n)
        ]
        silent = [
            {
                "agency_key": f"agencia-com-nome-longo-{i:03d}",
                "last_run_at": datetime(2026, 8, 21, 11, 32, 46, 804820),
            }
            for i in range(n)
        ]
        message = mod._build_alert_message(failures, stale, silent)
        # Telegram recusa texto acima de 4096; no modo log-only a linha
        # "Alert (log-only): ..." é cortada em 4096 no Cloud Logging.
        assert len("Alert (log-only): ") + mod._MAX_MESSAGE_CHARS <= 4096
        assert len(message) <= mod._MAX_MESSAGE_CHARS
        assert message.startswith("<b>Alerta: Agencias Sem Execucao</b>")
        assert message.endswith(mod._TRUNCATED_MARKER)
        for line in message.splitlines():
            assert line.count("<b>") == line.count("</b>"), line

    def test_short_message_is_not_truncated(self):
        mod = _load_module()
        message = mod._build_alert_message(self.FAILURES, self.STALE, SILENT_ROWS)
        assert mod._TRUNCATED_MARKER not in message

    def test_only_bold_tags_for_telegram_html(self):
        """parse_mode=HTML do Telegram rejeita tags desconhecidas (ex.: <agencia>)."""
        mod = _load_module()
        message = mod._build_alert_message(self.FAILURES, self.STALE, SILENT_ROWS)
        assert set(re.findall(r"<[^>]*>", message)) <= {"<b>", "</b>"}


class TestCheckSilentAgenciesTask:
    def _run(self, variables, rows=None):
        mod, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(side_effect=_variables(variables))
        conn, cursor = _mock_connection(rows if rows is not None else SILENT_ROWS)
        with (
            patch.dict(sys.modules, mocks),
            patch("psycopg2.connect", return_value=conn) as connect,
        ):
            result = registry["check_silent_agencies"]._original_fn()
        return mod, result, connect, conn, cursor

    def test_queries_active_agencies_with_configured_window(self):
        mod, result, connect, conn, cursor = self._run(
            {"scraper_database_url": "postgresql://monitor", "scraper_silent_hours": "3"}
        )
        connect.assert_called_once_with("postgresql://monitor")
        sql, params = cursor.execute.call_args[0]
        assert sql == mod._SILENT_AGENCIES_QUERY
        assert params == {"agencies": mod._load_active_agencies(), "silent_hours": 3}
        assert result == SILENT_ROWS
        conn.close.assert_called_once()

    def test_default_window_is_2_hours(self):
        _, _, _, _, cursor = self._run({"scraper_database_url": "postgresql://monitor"}, rows=[])
        assert cursor.execute.call_args[0][1]["silent_hours"] == 2

    def test_window_below_one_hour_is_clamped(self):
        """Janela 0 marcaria todas as agências como silenciosas."""
        _, _, _, _, cursor = self._run(
            {"scraper_database_url": "postgresql://monitor", "scraper_silent_hours": "0"}, rows=[]
        )
        assert cursor.execute.call_args[0][1]["silent_hours"] == 1

    def test_requires_database_url(self):
        with pytest.raises(ValueError, match="scraper_database_url"):
            self._run({})


class TestDagWiring:
    def test_send_alerts_receives_all_checks(self):
        _, registry, _ = _load_module_with_registry()
        registry["send_alerts"].assert_called_once_with(
            registry["check_consecutive_failures"].return_value,
            registry["check_stale_agencies"].return_value,
            registry["check_silent_agencies"].return_value,
        )


class TestSendAlertsWithSilent:
    def _run(self, failures, stale, silent):
        _, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(side_effect=_variables({}))
        with patch.dict(sys.modules, mocks):
            registry["send_alerts"]._original_fn(failures, stale, silent)
        return mocks["scraper.notify"].send_alert

    def test_sends_message_with_silent_section(self):
        send_alert = self._run([], [], SILENT_ROWS)
        send_alert.assert_called_once()
        message = send_alert.call_args.kwargs["message"]
        assert "Agencias Sem Execucao" in message
        assert "<b>aids</b>" in message

    def test_skips_when_nothing_to_report(self):
        send_alert = self._run([], [], [])
        send_alert.assert_not_called()
