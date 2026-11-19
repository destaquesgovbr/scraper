"""Logs em JSON (Cloud Logging) e eventos scrape_attempt/circuit_transition."""

import json
import logging
from datetime import UTC, datetime

import pytest
from loguru import logger

from govbr_scraper.logging_setup import configure_logging
from govbr_scraper.monitoring.structured_log import log_circuit_transition, log_scrape_attempt
from govbr_scraper.state.model import CircuitState, Transition


@pytest.fixture
def json_logs(capsys):
    """Liga o modo JSON e devolve uma função que lê as linhas JSON do stderr."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    assert configure_logging("json", "INFO") == "json"
    capsys.readouterr()

    def read() -> list[dict]:
        err = capsys.readouterr().err
        return [json.loads(line) for line in err.splitlines() if line.startswith("{")]

    yield read
    configure_logging("text")
    root.handlers, root.level = handlers, level


class TestConfigureLogging:
    def test_json_line_has_severity_message_time_and_extra_at_root(self, json_logs):
        logger.bind(agency="mec", event="teste").warning("mensagem {x}")
        (line,) = json_logs()
        assert line["severity"] == "WARNING"
        assert line["message"] == "mensagem {x}"
        assert line["agency"] == "mec"
        assert line["event"] == "teste"
        assert datetime.fromisoformat(line["time"]).tzinfo is not None
        assert line["logging.googleapis.com/sourceLocation"]["function"]

    def test_format_kwargs_also_go_to_root(self, json_logs):
        logger.info("Scrape completed for {agency}", agency="mds")
        (line,) = json_logs()
        assert line["message"] == "Scrape completed for mds"
        assert line["agency"] == "mds"
        assert line["severity"] == "INFO"

    def test_extra_cannot_override_base_fields(self, json_logs):
        logger.bind(severity="LIXO", message="outra", time="x").error("original")
        (line,) = json_logs()
        assert (line["severity"], line["message"]) == ("ERROR", "original")

    def test_level_filter(self, capsys):
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        try:
            configure_logging("json", "WARNING")
            capsys.readouterr()
            logger.info("não sai")
            logger.warning("sai")
            lines = [json.loads(x) for x in capsys.readouterr().err.splitlines()]
            assert [x["message"] for x in lines] == ["sai"]
        finally:
            configure_logging("text")
            root.handlers, root.level = handlers, level

    def test_non_serializable_extra_becomes_string(self, json_logs):
        logger.bind(at=datetime(2026, 11, 19, tzinfo=UTC)).info("x")
        (line,) = json_logs()
        assert line["at"].startswith("2026-11-19")

    def test_exception_traceback_in_message(self, json_logs):
        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("falhou")
        (line,) = json_logs()
        assert line["severity"] == "ERROR"
        assert "ValueError: boom" in line["message"]

    def test_stdlib_logging_is_intercepted(self, json_logs):
        logging.getLogger("govbr_scraper.teste").warning("do logging %s", "stdlib")
        lines = [x for x in json_logs() if x["message"] == "do logging stdlib"]
        assert lines and lines[0]["severity"] == "WARNING"
        assert lines[0]["stdlib_logger"] == "govbr_scraper.teste"

    def test_text_mode_is_not_json(self, capsys):
        configure_logging("text")
        capsys.readouterr()
        logger.info("texto puro")
        err = capsys.readouterr().err
        assert "texto puro" in err
        assert not err.lstrip().startswith("{")

    def test_env_selects_format(self, monkeypatch, capsys):
        root = logging.getLogger()
        handlers, level = root.handlers[:], root.level
        try:
            monkeypatch.setenv("LOG_FORMAT", "TEXT")
            assert configure_logging() == "text"
            monkeypatch.delenv("LOG_FORMAT")
            assert configure_logging() == "json"
        finally:
            configure_logging("text")
            root.handlers, root.level = handlers, level


class TestEvents:
    def test_scrape_attempt_fields(self, json_logs):
        log_scrape_attempt(
            agency="mec",
            outcome="failed",
            category="html_changed",
            retryable=False,
            articles_new=None,
            duration_s=1.23456,
            tier="h2",
            explicit_window=False,
            trigger="dag",
            window_start="2026-11-18",
        )
        (line,) = json_logs()
        assert line["event"] == "scrape_attempt"
        assert line["severity"] == "WARNING"
        assert {
            k: line[k]
            for k in (
                "agency",
                "outcome",
                "category",
                "retryable",
                "articles_new",
                "duration_s",
                "tier",
                "explicit_window",
                "trigger",
                "window_start",
            )
        } == {
            "agency": "mec",
            "outcome": "failed",
            "category": "html_changed",
            "retryable": False,
            "articles_new": None,
            "duration_s": 1.235,
            "tier": "h2",
            "explicit_window": False,
            "trigger": "dag",
            "window_start": "2026-11-18",
        }

    @pytest.mark.parametrize("outcome", ["success", "skipped"])
    def test_scrape_attempt_without_category_is_none_string(self, json_logs, outcome):
        log_scrape_attempt(agency="mds", outcome=outcome, articles_new=2, tier=None)
        (line,) = json_logs()
        assert line["category"] == "none"
        assert line["severity"] == "INFO"
        assert line["outcome"] == outcome

    def test_circuit_transition_fields(self, json_logs):
        t = Transition(
            "mec",
            CircuitState.CLOSED,
            CircuitState.OPEN,
            "permanent_errors",
            "html_changed",
            datetime(2026, 11, 19, tzinfo=UTC),
        )
        log_circuit_transition(t)
        (line,) = json_logs()
        assert line["event"] == "circuit_transition"
        assert line["severity"] == "WARNING"
        assert {k: line[k] for k in ("agency", "from_state", "to_state", "reason", "category")} == {
            "agency": "mec",
            "from_state": "closed",
            "to_state": "open",
            "reason": "permanent_errors",
            "category": "html_changed",
        }
