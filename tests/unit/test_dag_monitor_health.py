"""Tests for dags/monitor_scraping_health.py — sanitize, alert formatting e agências silenciosas."""

import inspect
import json
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from psycopg2 import errors

from govbr_scraper.models.monitoring import PERMANENT_ERROR_CATEGORIES
from govbr_scraper.monitoring.health_checks import (
    CONSECUTIVE_FAILURES_QUERY,
    OPEN_CIRCUIT_AGENCIES_QUERY,
    SILENT_AGENCIES_QUERY,
    STALE_AGENCIES_QUERY,
    find_consecutive_failures,
    find_silent_agencies,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TOPIC = "projects/inspire-7-finep/topics/dgb.ops.alerts"


def _make_airflow_mocks():
    """Create Airflow module mocks that prevent real execution of tasks."""

    def fake_dag(**kwargs):
        def wrapper(fn):
            fn._dag_kwargs = kwargs
            return fn

        return wrapper

    def fake_task(fn=None, **task_kwargs):
        # Aceita @task e @task(...) (scrape_agencies usa map_index_template etc.).
        def wrap(f):
            mock_callable = MagicMock(name=f"task_{f.__name__}")
            mock_callable._original_fn = f
            return mock_callable

        return wrap(fn) if callable(fn) else wrap

    mock_decorators = MagicMock()
    mock_decorators.dag = fake_dag
    mock_decorators.task = fake_task

    mock_variable = MagicMock()
    mock_variable.get = MagicMock(return_value="")

    mock_models = MagicMock()
    mock_models.Variable = mock_variable

    mock_notify = MagicMock()
    mock_notify.send_alert = MagicMock(return_value=True)
    mock_notify.default_alert_topic = MagicMock(return_value=DEFAULT_TOPIC)

    mock_scraper_pkg = MagicMock()
    mock_scraper_pkg.notify = mock_notify

    return {
        "airflow": MagicMock(),
        "airflow.decorators": mock_decorators,
        "airflow.models": mock_models,
        "airflow.sdk": MagicMock(),
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
        assert "from scraper.notify import default_alert_topic, send_alert" in source
        assert "from notify import" not in source

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

    def recording_task(fn=None, **task_kwargs):
        def wrap(f):
            mock_callable = MagicMock(name=f"task_{f.__name__}")
            mock_callable._original_fn = f
            registry[f.__name__] = mock_callable
            return mock_callable

        return wrap(fn) if callable(fn) else wrap

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


def _mock_connection(rows, open_circuits=(), state_error=None):
    """Conexão mockada: a query de scrape_agency_state devolve as agências com o circuito
    aberto (ou levanta state_error); as outras devolvem ``rows``."""
    cursor = MagicMock()
    last = {"sql": ""}

    def execute(sql, params=None):
        last["sql"] = sql
        if "scrape_agency_state" in sql and state_error is not None:
            raise state_error

    def fetchall():
        if "scrape_agency_state" in last["sql"]:
            return [{"agency_key": k} for k in open_circuits]
        return rows

    cursor.execute.side_effect = execute
    cursor.fetchall.side_effect = fetchall
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

    def test_matches_tasks_mapped_by_scrape_agencies(self):
        """O monitor espera execução exatamente das agências mapeadas na scrape_agencies."""
        monitor = _load_module()
        mocks = _make_airflow_mocks()
        mocks["airflow.exceptions"] = MagicMock()
        for key in list(sys.modules):
            if key.startswith("dags.scrape_agencies"):
                del sys.modules[key]
        try:
            with patch.dict(sys.modules, mocks):
                import dags.scrape_agencies as scrape_agencies

                generated = list(scrape_agencies.ACTIVE_AGENCY_KEYS)
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


class TestConsecutiveFailuresQueryParity:
    def test_dag_query_matches_health_checks(self):
        """A query vive duplicada (o pacote não é instalado no Composer): não pode divergir."""
        mod = _load_module()
        assert " ".join(mod._CONSECUTIVE_FAILURES_QUERY.split()) == " ".join(
            CONSECUTIVE_FAILURES_QUERY.split()
        )

    def test_permanent_categories_match_models(self):
        """Erro permanente na última execução basta: a lista é a de PERMANENT_ERROR_CATEGORIES."""
        match = re.search(r"error_category IN \(([^)]*)\)", CONSECUTIVE_FAILURES_QUERY)
        assert match
        listed = {item.strip().strip("'") for item in match.group(1).split(",")}
        assert listed == {str(c) for c in PERMANENT_ERROR_CATEGORIES}


class TestOpenCircuitQueryParity:
    def test_dag_query_matches_health_checks(self):
        """A query vive duplicada (o pacote não é instalado na VM): não pode divergir."""
        mod = _load_module()
        assert " ".join(mod._OPEN_CIRCUIT_AGENCIES_QUERY.split()) == " ".join(
            OPEN_CIRCUIT_AGENCIES_QUERY.split()
        )


class TestOpenCircuitExclusionInTasks:
    """Circuito aberto: fora de "Agências Sem Execução" e das falhas (o alerta do disjuntor
    cobre). Sem scrape_agency_state (038 não aplicada), nada muda."""

    FAILURES = [
        {"agency_key": "mec", "last_error": "html_changed", "last_failure_at": None},
        {"agency_key": "mds", "last_error": "network_error", "last_failure_at": None},
    ]

    def _run(self, task, rows, open_circuits=(), state_error=None):
        _, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(
            side_effect=_variables({"scraper_database_url": "postgresql://monitor"})
        )
        conn, cursor = _mock_connection(rows, open_circuits, state_error)
        with patch.dict(sys.modules, mocks), patch("psycopg2.connect", return_value=conn):
            result = registry[task]._original_fn()
        return result, conn, cursor

    def test_silent_excludes_open_circuits(self):
        result, conn, _ = self._run("check_silent_agencies", SILENT_ROWS, open_circuits=["inma"])
        assert [r["agency_key"] for r in result] == ["aids"]
        conn.close.assert_called_once()

    def test_failures_exclude_open_circuits(self):
        result, _, _ = self._run(
            "check_consecutive_failures", self.FAILURES, open_circuits=["mec", "antaq"]
        )
        assert [r["agency_key"] for r in result] == ["mds"]

    @pytest.mark.parametrize("task", ["check_silent_agencies", "check_consecutive_failures"])
    def test_missing_table_changes_nothing_and_rolls_back(self, task):
        rows = SILENT_ROWS if task == "check_silent_agencies" else self.FAILURES
        result, conn, _ = self._run(
            task, rows, state_error=errors.UndefinedTable('relation "scrape_agency_state"')
        )
        assert result == rows
        conn.rollback.assert_called_once()

    def test_stale_is_not_filtered(self):
        rows = [{"agency_key": "mec", "last_success_at": None}]
        result, _, cursor = self._run("check_stale_agencies", rows, open_circuits=["mec"])
        assert result == rows
        assert cursor.execute.call_count == 1


class TestStaleQueryParity:
    def test_dag_query_matches_health_checks(self):
        """A query vive duplicada (o pacote não é instalado no Composer): não pode divergir."""
        mod = _load_module()
        assert " ".join(mod._STALE_AGENCIES_QUERY.split()) == " ".join(STALE_AGENCIES_QUERY.split())

    def test_counts_new_articles_with_fallback_to_saved(self):
        """articles_new (migration 037); linhas anteriores a ela (NULL) usam articles_saved."""
        normalized = " ".join(STALE_AGENCIES_QUERY.split())
        assert normalized.count("COALESCE(articles_new, articles_saved) > 0") == 3
        assert "AND articles_saved > 0" not in normalized


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
        message = mod._build_alert_message([], [], SILENT_ROWS, silent_hours=3)
        assert "<b>Alerta: Agencias Sem Execucao</b>" in message
        assert "2 agencia(s) ativa(s) sem nenhum scrape_run nas ultimas 3h" in message
        assert "- <b>aids</b>: ultima execucao em 2026-08-21 11:32:46" in message
        assert "- <b>inma</b>: nenhuma execucao registrada em scrape_runs" in message

    def test_silent_header_points_to_single_dag_and_runbook(self):
        """Não existe mais DAG scrape_<agencia>: o problema é a task mapeada da scrape_agencies."""
        mod = _load_module()
        message = mod._build_alert_message([], [], SILENT_ROWS)
        assert "task mapeada da agencia na DAG scrape_agencies" in message
        assert "scrape_*" not in message
        assert "Ver docs/runbook.md, secao 'Agencias Sem Execucao (scrape_agencies)'" in message
        assert "ultimas 3h" in message

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
        sql, params = cursor.execute.call_args_list[0][0]
        assert sql == mod._SILENT_AGENCIES_QUERY
        assert params == {"agencies": mod._load_active_agencies(), "silent_hours": 3}
        assert result == SILENT_ROWS
        conn.close.assert_called_once()

    def test_default_window_is_3_hours(self):
        """Scraping de 2 em 2 h + 1 h de folga."""
        _, _, _, _, cursor = self._run({"scraper_database_url": "postgresql://monitor"}, rows=[])
        assert cursor.execute.call_args_list[0][0][1]["silent_hours"] == 3

    def test_window_below_one_hour_is_clamped(self):
        """Janela 0 marcaria todas as agências como silenciosas."""
        _, _, _, _, cursor = self._run(
            {"scraper_database_url": "postgresql://monitor", "scraper_silent_hours": "0"}, rows=[]
        )
        assert cursor.execute.call_args_list[0][0][1]["silent_hours"] == 1

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
            registry["check_open_circuits"].return_value,
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


# =============================================================================
# Agenda e limiares recalibrados para o scraping de 2 em 2 horas
# =============================================================================


class TestScheduleAndDefaults:
    def test_runs_30_minutes_after_each_scrape_run(self):
        """Scraping às 06h, 08h, ..., 22h BRT; o monitor às 06h30, ..., 22h30, nunca de madrugada."""
        mod = _load_module()
        assert mod.monitor_scraping_health_dag._dag_kwargs["schedule"] == "30 6-22/2 * * *"

    def test_silent_hours_default_is_3(self):
        assert _load_module()._DEFAULT_SILENT_HOURS == 3

    def test_failure_window_default_is_6_hours_with_threshold_3(self):
        mod = _load_module()
        assert mod._DEFAULT_FAILURE_WINDOW_HOURS == 6
        assert mod._DEFAULT_FAILURE_THRESHOLD == 3

    def test_defaults_match_health_checks(self):
        """health_checks.py duplica as queries e os defaults: não podem divergir."""
        mod = _load_module()
        failures = inspect.signature(find_consecutive_failures).parameters
        silent = inspect.signature(find_silent_agencies).parameters
        assert failures["window_hours"].default == mod._DEFAULT_FAILURE_WINDOW_HOURS
        assert failures["threshold"].default == mod._DEFAULT_FAILURE_THRESHOLD
        assert silent["silent_hours"].default == mod._DEFAULT_SILENT_HOURS


class TestCheckConsecutiveFailuresTask:
    def _run(self, variables):
        _, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(side_effect=_variables(variables))
        conn, cursor = _mock_connection([])
        with patch.dict(sys.modules, mocks), patch("psycopg2.connect", return_value=conn):
            registry["check_consecutive_failures"]._original_fn()
        return cursor.execute.call_args_list[0][0][1]

    def test_default_window_6h_and_threshold_3(self):
        params = self._run({"scraper_database_url": "postgresql://monitor"})
        assert params == {"threshold": 3, "window_hours": 6}

    def test_variables_override_defaults(self):
        params = self._run(
            {
                "scraper_database_url": "postgresql://monitor",
                "scraper_failure_window_hours": "12",
                "scraper_consecutive_failure_threshold": "5",
            }
        )
        assert params == {"threshold": 5, "window_hours": 12}


# =============================================================================
# Dedup do alerta (não repetir o mesmo alerta a cada execução)
# =============================================================================

FAILURE_ROWS = [
    {
        "agency_key": "mec",
        "last_error": "network_error",
        "last_failure_at": datetime(2026, 11, 17, 10, 0),
    }
]
STALE_ROWS = [{"agency_key": "funai", "last_success_at": datetime(2026, 11, 10, 8, 0)}]
OPEN_CIRCUIT_ROWS = [
    {
        "agency_key": "mec",
        "circuit_state": "open",
        "circuit_opened_at": datetime(2026, 11, 19, 10, 5),
        "next_due_at": datetime(2026, 11, 20, 10, 5),
        "last_error_category": "html_changed",
    },
    {
        "agency_key": "antaq",
        "circuit_state": "half_open",
        "circuit_opened_at": None,
        "next_due_at": None,
        "last_error_category": None,
    },
]


class TestAlertDigest:
    def test_lists_sorted_keys_of_silent_and_failures_only(self):
        mod = _load_module()
        digest = mod._alert_digest(FAILURE_ROWS, list(reversed(SILENT_ROWS)))
        assert digest == {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}

    def test_ignores_timestamps(self):
        mod = _load_module()
        later = [{**SILENT_ROWS[0], "last_run_at": datetime(2026, 11, 17, 12, 0)}, SILENT_ROWS[1]]
        assert mod._alert_digest([], SILENT_ROWS) == mod._alert_digest([], later)

    def test_empty(self):
        assert _load_module()._alert_digest([], []) == {
            "silent": [],
            "failures": [],
            "open_circuits": [],
        }

    def test_open_circuits_enter_the_digest(self):
        digest = _load_module()._alert_digest([], [], OPEN_CIRCUIT_ROWS)
        assert digest["open_circuits"] == ["antaq", "mec"]


class TestShouldSend:
    NOW = datetime(2026, 11, 17, 12, 30, tzinfo=UTC)
    DIGEST = {"silent": ["aids"], "failures": []}

    def _state(self, hours_ago, digest=None):
        return {
            "digest": digest or self.DIGEST,
            "sent_at": (self.NOW - timedelta(hours=hours_ago)).isoformat(),
        }

    def test_sends_without_state(self):
        mod = _load_module()
        assert mod._should_send(self.DIGEST, None, self.NOW) is True
        assert mod._should_send(self.DIGEST, {}, self.NOW) is True

    def test_same_digest_within_repeat_window_is_suppressed(self):
        mod = _load_module()
        assert mod._should_send(self.DIGEST, self._state(2), self.NOW, 24) is False
        assert mod._should_send(self.DIGEST, self._state(23.9), self.NOW, 24) is False

    def test_same_digest_after_repeat_window_is_sent(self):
        mod = _load_module()
        assert mod._should_send(self.DIGEST, self._state(24), self.NOW, 24) is True

    def test_changed_digest_is_sent(self):
        mod = _load_module()
        state = self._state(1, digest={"silent": [], "failures": []})
        assert mod._should_send(self.DIGEST, state, self.NOW, 24) is True

    def test_unparseable_sent_at_is_sent(self):
        mod = _load_module()
        state = {"digest": self.DIGEST, "sent_at": "ontem"}
        assert mod._should_send(self.DIGEST, state, self.NOW, 24) is True

    def test_repeat_zero_disables_dedup(self):
        mod = _load_module()
        assert mod._should_send(self.DIGEST, self._state(0), self.NOW, 0) is True


class _VariableStore:
    """Variable.get/set em memória, com default_var e erros opcionais."""

    def __init__(self, values=None, get_error=None, set_error=None):
        self.values = dict(values or {})
        self.get_error = get_error
        self.set_error = set_error
        self.set_calls = []

    def get(self, key, default_var=None):
        if self.get_error and key == "scraper_monitor_last_alert":
            raise self.get_error
        return self.values.get(key, default_var)

    def set(self, key, value, *args, **kwargs):
        self.set_calls.append((key, value))
        if self.set_error:
            raise self.set_error
        self.values[key] = value

    def state(self):
        raw = self.values.get("scraper_monitor_last_alert")
        return json.loads(raw) if raw else None


def _state_json(digest, hours_ago):
    sent_at = datetime.now(UTC) - timedelta(hours=hours_ago)
    return json.dumps({"digest": digest, "sent_at": sent_at.isoformat()})


class TestSendAlertsDedup:
    def _run(self, failures, stale, silent, store=None, delivered=True):
        store = store or _VariableStore()
        _, registry, mocks = _load_module_with_registry()
        variable = mocks["airflow.models"].Variable
        variable.get = MagicMock(side_effect=store.get)
        variable.set = MagicMock(side_effect=store.set)
        send_alert = mocks["scraper.notify"].send_alert
        send_alert.return_value = delivered
        with patch.dict(sys.modules, mocks):
            registry["send_alerts"]._original_fn(failures, stale, silent)
        return send_alert, store

    def test_first_alert_is_sent_and_state_saved(self):
        send_alert, store = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS)
        send_alert.assert_called_once()
        state = store.state()
        assert state["digest"] == {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}
        assert abs(datetime.now(UTC) - datetime.fromisoformat(state["sent_at"])) < timedelta(
            minutes=1
        )

    def test_same_findings_within_24h_are_not_resent(self):
        digest = {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 2)})
        send_alert, store = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_not_called()
        assert store.set_calls == []

    def test_stale_only_change_does_not_trigger(self):
        digest = {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 2)})
        more_stale = STALE_ROWS + [{"agency_key": "ibama", "last_success_at": None}]
        send_alert, _ = self._run(FAILURE_ROWS, more_stale, SILENT_ROWS, store)
        send_alert.assert_not_called()

    def test_new_silent_agency_triggers(self):
        digest = {"silent": ["aids"], "failures": ["mec"], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 1)})
        send_alert, store = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()
        assert store.state()["digest"]["silent"] == ["aids", "inma"]

    def test_failures_change_triggers(self):
        digest = {"silent": ["aids", "inma"], "failures": [], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 1)})
        send_alert, _ = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()

    def test_same_findings_are_resent_after_24h(self):
        digest = {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 25)})
        send_alert, _ = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()

    def test_repeat_hours_variable(self):
        digest = {"silent": ["aids", "inma"], "failures": ["mec"], "open_circuits": []}
        store = _VariableStore(
            {
                "scraper_monitor_last_alert": _state_json(digest, 7),
                "scraper_alert_repeat_hours": "6",
            }
        )
        send_alert, _ = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()

    def test_stale_only_findings_are_sent_once_then_daily(self):
        send_alert, store = self._run([], STALE_ROWS, [])
        send_alert.assert_called_once()
        assert "Agencias Sem Noticias" in send_alert.call_args.kwargs["message"]
        assert store.state()["digest"] == {"silent": [], "failures": [], "open_circuits": []}

        send_alert, _ = self._run([], STALE_ROWS + STALE_ROWS, [], store)
        send_alert.assert_not_called()

    def test_state_read_error_still_sends(self):
        store = _VariableStore(get_error=RuntimeError("secret manager fora"))
        send_alert, _ = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()

    def test_corrupt_state_still_sends(self):
        store = _VariableStore({"scraper_monitor_last_alert": "{nao e json"})
        send_alert, _ = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()

    def test_state_write_error_does_not_fail_task(self):
        store = _VariableStore(set_error=RuntimeError("api server fora"))
        send_alert, store = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, store)
        send_alert.assert_called_once()
        assert len(store.set_calls) == 1

    def test_state_not_saved_when_not_delivered(self):
        """No modo log-only o alerta volta ao log na execução seguinte."""
        send_alert, store = self._run(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS, delivered=False)
        send_alert.assert_called_once()
        assert store.set_calls == []

    def test_no_findings_resets_state(self):
        digest = {"silent": ["aids"], "failures": [], "open_circuits": []}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 1)})
        send_alert, store = self._run([], [], [], store)
        send_alert.assert_not_called()
        assert store.set_calls == [("scraper_monitor_last_alert", "{}")]

    def test_no_findings_without_state_writes_nothing(self):
        send_alert, store = self._run([], [], [])
        send_alert.assert_not_called()
        assert store.set_calls == []


class TestOpenCircuitSection:
    """Circuito aberto: seção própria no alerta e no dedup. O alerta 🔴 do scraper-api é
    publicado uma vez só, sem retry; se ele se perde, esta seção é o único aviso de que a
    agência está fora da raspagem regular."""

    def _send(self, open_circuits, store=None, failures=None, silent=None):
        store = store or _VariableStore()
        _, registry, mocks = _load_module_with_registry()
        variable = mocks["airflow.models"].Variable
        variable.get = MagicMock(side_effect=store.get)
        variable.set = MagicMock(side_effect=store.set)
        send_alert = mocks["scraper.notify"].send_alert
        with patch.dict(sys.modules, mocks):
            registry["send_alerts"]._original_fn(failures or [], [], silent or [], open_circuits)
        return send_alert, store

    def test_message_lists_open_circuits(self):
        message = _load_module()._build_alert_message([], [], [], 3, OPEN_CIRCUIT_ROWS)
        assert "Circuito Aberto (disjuntor)" in message
        assert "2 agencia(s) fora da raspagem regular" in message
        assert "<b>mec</b>: html_changed, aberto em 2026-11-19 10:05:00" in message
        assert "proxima sonda em 2026-11-20 10:05:00" in message
        assert "<b>antaq</b>: sem categoria" in message

    def test_only_open_circuits_is_sent_and_saved(self):
        send_alert, store = self._send(OPEN_CIRCUIT_ROWS)
        send_alert.assert_called_once()
        assert store.state()["digest"]["open_circuits"] == ["antaq", "mec"]

    def test_same_open_circuits_are_reminded_after_24h(self):
        digest = {"silent": [], "failures": [], "open_circuits": ["antaq", "mec"]}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 2)})
        send_alert, _ = self._send(OPEN_CIRCUIT_ROWS, store)
        send_alert.assert_not_called()

        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 25)})
        send_alert, _ = self._send(OPEN_CIRCUIT_ROWS, store)
        send_alert.assert_called_once()

    def test_new_open_circuit_triggers(self):
        digest = {"silent": [], "failures": [], "open_circuits": ["mec"]}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 1)})
        send_alert, _ = self._send(OPEN_CIRCUIT_ROWS, store)
        send_alert.assert_called_once()

    def test_state_from_before_the_section_resends_once(self):
        """Estado gravado antes da seção (sem a chave open_circuits): reenvia uma vez."""
        digest = {"silent": [], "failures": ["mec"]}
        store = _VariableStore({"scraper_monitor_last_alert": _state_json(digest, 1)})
        send_alert, _ = self._send([], store, failures=FAILURE_ROWS)
        send_alert.assert_called_once()


class TestCheckOpenCircuitsTask:
    def _run(self, open_circuits=(), state_error=None, active=("mec", "nova")):
        mod, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(
            side_effect=_variables({"scraper_database_url": "postgresql://monitor"})
        )
        conn, _ = _mock_connection([], open_circuits, state_error)
        with (
            patch.dict(sys.modules, mocks),
            patch("psycopg2.connect", return_value=conn),
            patch.object(mod, "_load_active_agencies", return_value=list(active)),
        ):
            result = registry["check_open_circuits"]._original_fn()
        return result, conn

    def test_only_active_gov_br_and_ebc_sources(self):
        """Agência desativada no YAML com o circuito aberto não é mais raspada: fica de fora."""
        result, conn = self._run(open_circuits=["mec", "nova", "sri", "tvbrasil"])
        assert [r["agency_key"] for r in result] == ["mec", "nova", "tvbrasil"]
        conn.close.assert_called_once()

    def test_missing_table_is_empty_and_rolls_back(self):
        result, conn = self._run(state_error=errors.UndefinedTable("scrape_agency_state"))
        assert result == []
        conn.rollback.assert_called_once()

    def test_ebc_sources_match_scrape_ebc(self):
        monitor = _load_module()
        mocks = _make_airflow_mocks()
        mocks["airflow.exceptions"] = MagicMock()
        for key in list(sys.modules):
            if key.startswith("dags.scrape_ebc"):
                del sys.modules[key]
        try:
            with patch.dict(sys.modules, mocks):
                import dags.scrape_ebc as scrape_ebc

                sources = list(scrape_ebc.EBC_SOURCES)
        finally:
            for key in list(sys.modules):
                if key.startswith("dags.scrape_ebc"):
                    del sys.modules[key]
        assert list(monitor._EBC_SOURCES) == sources


class TestSendAlertsChannels:
    def _run(self, variables=None):
        store = _VariableStore(variables)
        _, registry, mocks = _load_module_with_registry()
        mocks["airflow.models"].Variable.get = MagicMock(side_effect=store.get)
        mocks["airflow.models"].Variable.set = MagicMock(side_effect=store.set)
        with patch.dict(sys.modules, mocks):
            registry["send_alerts"]._original_fn(FAILURE_ROWS, STALE_ROWS, SILENT_ROWS)
        return mocks["scraper.notify"].send_alert.call_args.kwargs

    def test_publishes_to_default_ops_alerts_topic(self):
        kwargs = self._run()
        assert kwargs["pubsub_topic"] == DEFAULT_TOPIC
        assert kwargs["title"] == "Scraper: saúde das agências"
        assert kwargs["severity"] == "warning"
        assert kwargs["source"] == "monitor_scraping_health (Airflow)"

    def test_topic_variable_overrides_default(self):
        kwargs = self._run({"scraper_alert_pubsub_topic": "projects/outro-projeto/topics/x.y"})
        assert kwargs["pubsub_topic"] == "projects/outro-projeto/topics/x.y"

    def test_keeps_telegram_and_webhook_variables(self):
        kwargs = self._run(
            {
                "scraper_telegram_bot_token": "123:ABC",
                "scraper_telegram_monitor_chat_id": "-100",
                "scraper_alert_webhook_url": "https://hooks.example.com",
            }
        )
        assert kwargs["telegram_token"] == "123:ABC"
        assert kwargs["telegram_chat_id"] == "-100"
        assert kwargs["webhook_url"] == "https://hooks.example.com"
