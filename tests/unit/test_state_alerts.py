"""Alertas do disjuntor no tópico dgb.ops.alerts (publish_ops_alert, alert_for_transition)."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from govbr_scraper.state.alerts import alert_for_transition, announce_transition, publish_ops_alert
from govbr_scraper.state.model import CircuitState, Transition

T0 = datetime(2026, 11, 19, 13, 0, tzinfo=UTC)

TRANSITION_OPEN = Transition(
    "mec", CircuitState.CLOSED, CircuitState.OPEN, "permanent_errors", "html_changed", T0
)
TRANSITION_CLOSE = Transition(
    "mec", CircuitState.HALF_OPEN, CircuitState.CLOSED, "probe_success", "html_changed", T0
)


class TestAlertForTransition:
    def test_open(self):
        alert = alert_for_transition(TRANSITION_OPEN)
        assert alert["text"].startswith("🔴 Disjuntor aberto: mec")
        assert "permanent_errors" in alert["text"]
        assert "html_changed" in alert["text"]
        assert "2 h, 6 h, 24 h" in alert["text"]
        assert alert["severity"] == "warning"
        assert "mec" in alert["title"]

    def test_close(self):
        alert = alert_for_transition(TRANSITION_CLOSE)
        assert alert["text"].startswith("🟢 Disjuntor fechado: mec")
        assert "probe_success" in alert["text"]
        assert alert["severity"] == "info"

    def test_without_category(self):
        alert = alert_for_transition(replace(TRANSITION_OPEN, category=None))
        assert "sem categoria" in alert["text"]


class TestPublishOpsAlert:
    TOPIC = "projects/inspire-7-finep/topics/dgb.ops.alerts"

    def test_without_env_only_warns(self, monkeypatch):
        monkeypatch.delenv("PUBSUB_TOPIC_OPS_ALERTS", raising=False)
        with patch("google.cloud.pubsub_v1.PublisherClient") as client:
            assert publish_ops_alert("texto", "título", "warning") is False
        client.assert_not_called()

    def test_publishes_payload_and_waits(self, monkeypatch):
        monkeypatch.setenv("PUBSUB_TOPIC_OPS_ALERTS", self.TOPIC)
        with patch("google.cloud.pubsub_v1.PublisherClient") as client:
            future = client.return_value.publish.return_value
            assert publish_ops_alert("🔴 texto", "título", "critical", test=True) is True
        topic, data = client.return_value.publish.call_args[0]
        assert topic == self.TOPIC
        assert json.loads(data.decode("utf-8")) == {
            "text": "🔴 texto",
            "title": "título",
            "severity": "critical",
            "source": "scraper-api",
            "test": True,
        }
        future.result.assert_called_once_with(timeout=10)

    def test_invalid_severity_becomes_warning(self, monkeypatch):
        monkeypatch.setenv("PUBSUB_TOPIC_OPS_ALERTS", self.TOPIC)
        with patch("google.cloud.pubsub_v1.PublisherClient") as client:
            publish_ops_alert("t", "x", "grave")
        payload = json.loads(client.return_value.publish.call_args[0][1])
        assert payload["severity"] == "warning"
        assert payload["test"] is False

    @pytest.mark.parametrize("where", ["client", "publish", "result"])
    def test_never_raises(self, monkeypatch, where):
        monkeypatch.setenv("PUBSUB_TOPIC_OPS_ALERTS", self.TOPIC)
        with patch("google.cloud.pubsub_v1.PublisherClient") as client:
            if where == "client":
                client.side_effect = RuntimeError("sem credencial")
            elif where == "publish":
                client.return_value.publish.side_effect = RuntimeError("403")
            else:
                client.return_value.publish.return_value.result.side_effect = TimeoutError()
            assert publish_ops_alert("t", "x", "info") is False

    def test_long_text_is_cut(self, monkeypatch):
        monkeypatch.setenv("PUBSUB_TOPIC_OPS_ALERTS", self.TOPIC)
        text = "\n".join(["linha " + "x" * 90] * 100)
        with patch("google.cloud.pubsub_v1.PublisherClient") as client:
            publish_ops_alert(text, "x", "info")
        payload = json.loads(client.return_value.publish.call_args[0][1])
        assert len(payload["text"]) <= 3400
        assert payload["text"].endswith("x")

    def test_announce_logs_event_and_publishes(self, monkeypatch):
        monkeypatch.setenv("PUBSUB_TOPIC_OPS_ALERTS", self.TOPIC)
        with (
            patch("google.cloud.pubsub_v1.PublisherClient") as client,
            patch("govbr_scraper.monitoring.structured_log.log_circuit_transition") as log,
        ):
            assert announce_transition(TRANSITION_OPEN) is True
        log.assert_called_once_with(TRANSITION_OPEN)
        client.return_value.publish.assert_called_once()
