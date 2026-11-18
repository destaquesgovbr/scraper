"""Tests for notification helpers — Telegram, Pub/Sub (dgb.ops.alerts), webhook and fallback."""

import base64
import json
import logging
from unittest.mock import MagicMock, patch

import httpx
import pytest


class TestSendTelegramAlert:
    @patch("dags.notify.httpx")
    def test_posts_to_telegram_api(self, mock_httpx):
        # Import inside method so @patch("dags.notify.httpx") is active before import
        from dags.notify import send_telegram_alert

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.post.return_value = mock_response

        result = send_telegram_alert("123456:ABC-DEF", "-100123", "<b>Alert</b>")

        assert result is True
        mock_httpx.post.assert_called_once()
        call_args = mock_httpx.post.call_args
        assert "123456:ABC-DEF" in call_args[0][0]
        assert call_args[1]["json"]["chat_id"] == "-100123"
        assert call_args[1]["json"]["parse_mode"] == "HTML"

    def test_rejects_invalid_token_format(self):
        from dags.notify import send_telegram_alert

        assert send_telegram_alert("BADTOKEN", "-100", "msg") is False
        assert send_telegram_alert("", "-100", "msg") is False

    @patch("dags.notify.httpx")
    def test_returns_false_on_401_unauthorized(self, mock_httpx):
        """HTTP 401 from Telegram indicates invalid token."""
        from dags.notify import send_telegram_alert

        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_httpx.post.return_value = mock_response

        result = send_telegram_alert("123:ABC", "-100", "msg")

        assert result is False

    @patch("dags.notify.httpx")
    def test_returns_false_on_non_200_status(self, mock_httpx):
        """Non-200 status codes should return False."""
        from dags.notify import send_telegram_alert

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_httpx.post.return_value = mock_response

        result = send_telegram_alert("123:ABC", "-100", "msg")

        assert result is False

    @patch("dags.notify.httpx")
    def test_timeout_handling(self, mock_httpx):
        """Timeout should be handled gracefully."""
        from dags.notify import send_telegram_alert

        mock_httpx.post.side_effect = httpx.TimeoutException("Request timed out")

        result = send_telegram_alert("123:ABC", "-100", "msg")

        assert result is False

    @patch("dags.notify.httpx")
    def test_includes_timeout_parameter(self, mock_httpx):
        """POST request should include 5s timeout."""
        from dags.notify import send_telegram_alert

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.post.return_value = mock_response

        send_telegram_alert("123:ABC", "-100", "msg")

        call_kwargs = mock_httpx.post.call_args[1]
        assert call_kwargs["timeout"] == 5.0

    @patch("dags.notify.httpx")
    def test_handles_long_message(self, mock_httpx):
        """Long messages (approaching Telegram 4096 char limit) should work."""
        from dags.notify import send_telegram_alert

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.post.return_value = mock_response

        long_message = "X" * 4000

        result = send_telegram_alert("123:ABC", "-100", long_message)

        assert result is True
        call_args = mock_httpx.post.call_args
        assert call_args[1]["json"]["text"] == long_message


class TestSendAlert:
    @patch("dags.notify.send_telegram_alert")
    def test_logs_when_telegram_not_configured(self, mock_tg, caplog):
        from dags.notify import send_alert

        with caplog.at_level(logging.INFO):
            send_alert("Test alert", telegram_token=None, telegram_chat_id=None)

        mock_tg.assert_not_called()
        assert "Test alert" in caplog.text

    @patch("dags.notify.httpx")
    def test_does_not_raise_on_http_error(self, mock_httpx):
        from dags.notify import send_telegram_alert

        mock_httpx.post.side_effect = Exception("Connection refused")

        result = send_telegram_alert("123:ABC", "-100", "msg")

        assert result is False

    @patch("dags.notify.send_telegram_alert")
    def test_returns_true_when_telegram_succeeds(self, mock_tg):
        """When Telegram succeeds, should return True."""
        from dags.notify import send_alert

        mock_tg.return_value = True

        result = send_alert("Alert", telegram_token="123:ABC", telegram_chat_id="-100")

        assert result is True
        mock_tg.assert_called_once_with("123:ABC", "-100", "Alert")

    @patch("dags.notify.send_telegram_alert")
    @patch("dags.notify.httpx")
    def test_falls_back_to_webhook_when_telegram_fails(self, mock_httpx, mock_tg):
        """When Telegram fails, should try webhook."""
        from dags.notify import send_alert

        mock_tg.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.post.return_value = mock_response

        result = send_alert(
            "Alert",
            telegram_token="123:ABC",
            telegram_chat_id="-100",
            webhook_url="https://hooks.example.com/alert",
        )

        assert result is True
        mock_httpx.post.assert_called_once()
        call_args = mock_httpx.post.call_args
        assert call_args[0][0] == "https://hooks.example.com/alert"
        assert call_args[1]["json"]["text"] == "Alert"

    @patch("dags.notify.send_telegram_alert")
    @patch("dags.notify.httpx")
    def test_returns_false_when_all_channels_fail(self, mock_httpx, mock_tg, caplog):
        """When all channels fail, should log and return False."""
        from dags.notify import send_alert

        mock_tg.return_value = False
        mock_httpx.post.side_effect = Exception("Network error")

        with caplog.at_level(logging.INFO):
            result = send_alert(
                "Alert",
                telegram_token="123:ABC",
                telegram_chat_id="-100",
                webhook_url="https://hooks.example.com/alert",
            )

        assert result is False
        # Should log the alert as fallback
        assert "Alert (log-only)" in caplog.text or "Alert" in caplog.text

    @patch("dags.notify.send_telegram_alert")
    @patch("dags.notify.httpx")
    def test_webhook_with_non_2xx_status_returns_false(self, mock_httpx, mock_tg):
        """Webhook with non-2xx status should be considered failure."""
        from dags.notify import send_alert

        mock_tg.return_value = False
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_httpx.post.return_value = mock_response

        result = send_alert(
            "Alert",
            telegram_token="123:ABC",
            telegram_chat_id="-100",
            webhook_url="https://hooks.example.com/alert",
        )

        # Should fall through to log-only
        assert result is False

    @patch("dags.notify.send_telegram_alert")
    def test_skips_telegram_when_token_missing(self, mock_tg, caplog):
        """Missing telegram_token should skip Telegram channel."""
        from dags.notify import send_alert

        with caplog.at_level(logging.INFO):
            send_alert("Alert", telegram_token=None, telegram_chat_id="-100")

        mock_tg.assert_not_called()

    @patch("dags.notify.send_telegram_alert")
    def test_skips_telegram_when_chat_id_missing(self, mock_tg, caplog):
        """Missing telegram_chat_id should skip Telegram channel."""
        from dags.notify import send_alert

        with caplog.at_level(logging.INFO):
            send_alert("Alert", telegram_token="123:ABC", telegram_chat_id=None)

        mock_tg.assert_not_called()

    @patch("dags.notify.httpx")
    def test_webhook_includes_timeout(self, mock_httpx):
        """Webhook POST should include 5s timeout."""
        from dags.notify import send_alert

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_httpx.post.return_value = mock_response

        send_alert("Alert", webhook_url="https://hooks.example.com/alert")

        call_kwargs = mock_httpx.post.call_args[1]
        assert call_kwargs["timeout"] == 5.0


# =============================================================================
# Pub/Sub dgb.ops.alerts (repassado ao Telegram da equipe pelo alert-relay)
# =============================================================================

TOPIC = "projects/inspire-7-finep/topics/dgb.ops.alerts"
MONITOR_MESSAGE = (
    "<b>Alerta: Agencias Sem Execucao</b>\n"
    "1 agencia(s) ativa(s) sem nenhum scrape_run nas ultimas 3h\n\n"
    "- <b>esg</b>: ultima execucao em 2026-11-11 10:00:00"
)


def _decode(post_mock):
    body = post_mock.call_args.kwargs["json"]
    (msg,) = body["messages"]
    return json.loads(base64.b64decode(msg["data"]).decode("utf-8"))


@pytest.fixture
def pubsub():
    """google.auth.default + AuthorizedSession mockados; devolve (default, session_cls, session)."""
    credentials = MagicMock(name="credentials", token="ya29.segredo")
    session = MagicMock(name="session")
    session.post.return_value = MagicMock(status_code=200, text='{"messageIds": ["1"]}')
    with (
        patch("google.auth.default", return_value=(credentials, "inspire-7-finep")) as default,
        patch("google.auth.transport.requests.AuthorizedSession") as session_cls,
    ):
        session_cls.return_value.__enter__.return_value = session
        yield default, session_cls, session


class TestPublishPubsubAlert:
    def test_posts_to_publish_endpoint_with_cloud_platform_scope(self, pubsub):
        from dags.notify import publish_pubsub_alert

        default, session_cls, session = pubsub
        assert publish_pubsub_alert(TOPIC, MONITOR_MESSAGE, title="T", source="S") is True

        default.assert_called_once_with(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        session_cls.assert_called_once_with(default.return_value[0])
        url = session.post.call_args.args[0]
        assert url == f"https://pubsub.googleapis.com/v1/{TOPIC}:publish"
        assert session.post.call_args.kwargs["timeout"] == 10.0

    def test_payload_follows_alert_relay_contract(self, pubsub):
        from dags.notify import publish_pubsub_alert

        _, _, session = pubsub
        publish_pubsub_alert(
            TOPIC,
            MONITOR_MESSAGE,
            title="Scraper: saúde das agências",
            severity="warning",
            source="monitor_scraping_health (Airflow)",
        )
        assert _decode(session.post) == {
            "text": (
                "Alerta: Agencias Sem Execucao\n"
                "1 agencia(s) ativa(s) sem nenhum scrape_run nas ultimas 3h\n\n"
                "- esg: ultima execucao em 2026-11-11 10:00:00"
            ),
            "title": "Scraper: saúde das agências",
            "severity": "warning",
            "source": "monitor_scraping_health (Airflow)",
        }

    def test_optional_fields_are_omitted(self, pubsub):
        from dags.notify import publish_pubsub_alert

        _, _, session = pubsub
        publish_pubsub_alert(TOPIC, "texto")
        assert _decode(session.post) == {"text": "texto", "severity": "warning"}

    def test_unknown_severity_falls_back_to_warning(self, pubsub):
        from dags.notify import publish_pubsub_alert

        _, _, session = pubsub
        publish_pubsub_alert(TOPIC, "texto", severity="urgent")
        assert _decode(session.post)["severity"] == "warning"

    def test_long_text_is_truncated_at_line_boundary(self, pubsub):
        from dags.notify import PUBSUB_MAX_TEXT_CHARS, publish_pubsub_alert

        _, _, session = pubsub
        lines = [
            f"- <b>agencia-{i:03d}</b>: ultima execucao em 2026-11-11 10:00:00" for i in range(150)
        ]
        message = "<b>Alerta</b>\n" + "\n".join(lines)
        publish_pubsub_alert(TOPIC, message)

        text = _decode(session.post)["text"]
        assert len(text) <= PUBSUB_MAX_TEXT_CHARS <= 3500
        assert text.endswith("... (alerta truncado; lista completa no log da task)")
        plain = {line.replace("<b>", "").replace("</b>", "") for line in ["<b>Alerta</b>", *lines]}
        for line in text.split("\n")[:-1]:
            assert line in plain, line

    def test_single_huge_line_is_cut(self, pubsub):
        from dags.notify import PUBSUB_MAX_TEXT_CHARS, publish_pubsub_alert

        _, _, session = pubsub
        publish_pubsub_alert(TOPIC, "X" * 10_000)
        assert len(_decode(session.post)["text"]) <= PUBSUB_MAX_TEXT_CHARS

    @pytest.mark.parametrize(
        "topic",
        [
            "",
            None,
            "dgb.ops.alerts",
            "projects/inspire-7-finep/topics/",
            "projects/inspire-7-finep/topics/x/../../subscriptions/y",
            "https://evil.example.com/projects/p/topics/t",
        ],
    )
    def test_rejects_invalid_topic_without_calling_api(self, pubsub, topic):
        from dags.notify import publish_pubsub_alert

        default, _, session = pubsub
        assert publish_pubsub_alert(topic, "texto") is False
        default.assert_not_called()
        session.post.assert_not_called()

    def test_http_error_returns_false_without_logging_credentials(self, pubsub, caplog):
        from dags.notify import publish_pubsub_alert

        _, _, session = pubsub
        session.post.return_value = MagicMock(
            status_code=403, text='{"error": {"status": "PERMISSION_DENIED"}}'
        )
        with caplog.at_level(logging.INFO):
            assert publish_pubsub_alert(TOPIC, "texto") is False
        assert "403" in caplog.text
        assert "PERMISSION_DENIED" in caplog.text
        assert "ya29.segredo" not in caplog.text

    def test_auth_error_returns_false(self, pubsub, caplog):
        from dags.notify import publish_pubsub_alert

        default, _, _ = pubsub
        default.side_effect = RuntimeError("no default credentials")
        with caplog.at_level(logging.ERROR):
            assert publish_pubsub_alert(TOPIC, "texto") is False
        assert "RuntimeError" in caplog.text

    def test_network_error_returns_false(self, pubsub):
        from dags.notify import publish_pubsub_alert

        _, _, session = pubsub
        session.post.side_effect = ConnectionError("reset")
        assert publish_pubsub_alert(TOPIC, "texto") is False


class TestToPlainText:
    def test_strips_bold_tags_only(self):
        from dags.notify import to_plain_text

        assert to_plain_text("<b>Alerta</b>: <b>mec</b> a < b") == "Alerta: mec a < b"


class TestDefaultAlertTopic:
    def test_uses_gcp_project_id(self, monkeypatch):
        from dags.notify import default_alert_topic

        monkeypatch.setenv("GCP_PROJECT_ID", "inspire-7-finep")
        assert default_alert_topic() == TOPIC

    def test_none_without_project(self, monkeypatch):
        from dags.notify import default_alert_topic

        monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
        assert default_alert_topic() is None


class TestSendAlertChannelPriority:
    @patch("dags.notify.publish_pubsub_alert")
    @patch("dags.notify.send_telegram_alert")
    def test_telegram_first(self, mock_tg, mock_pubsub):
        from dags.notify import send_alert

        mock_tg.return_value = True
        assert send_alert("A", telegram_token="1:X", telegram_chat_id="-1", pubsub_topic=TOPIC)
        mock_pubsub.assert_not_called()

    @patch("dags.notify.httpx")
    @patch("dags.notify.publish_pubsub_alert")
    @patch("dags.notify.send_telegram_alert")
    def test_pubsub_when_telegram_fails(self, mock_tg, mock_pubsub, mock_httpx):
        from dags.notify import send_alert

        mock_tg.return_value = False
        mock_pubsub.return_value = True
        result = send_alert(
            "A",
            telegram_token="1:X",
            telegram_chat_id="-1",
            pubsub_topic=TOPIC,
            webhook_url="https://hooks.example.com",
            title="T",
            severity="critical",
            source="S",
        )
        assert result is True
        mock_pubsub.assert_called_once_with(TOPIC, "A", title="T", severity="critical", source="S")
        mock_httpx.post.assert_not_called()

    @patch("dags.notify.httpx")
    @patch("dags.notify.publish_pubsub_alert")
    def test_pubsub_without_telegram(self, mock_pubsub, mock_httpx):
        """Cenário de produção: as Variables scraper_telegram_* não existem."""
        from dags.notify import send_alert

        mock_pubsub.return_value = True
        assert send_alert("A", pubsub_topic=TOPIC, webhook_url="https://hooks.example.com")
        mock_httpx.post.assert_not_called()

    @patch("dags.notify.httpx")
    @patch("dags.notify.publish_pubsub_alert")
    def test_webhook_when_pubsub_fails(self, mock_pubsub, mock_httpx):
        from dags.notify import send_alert

        mock_pubsub.return_value = False
        mock_httpx.post.return_value = MagicMock(status_code=200)
        assert send_alert("A", pubsub_topic=TOPIC, webhook_url="https://hooks.example.com")
        mock_httpx.post.assert_called_once()

    @patch("dags.notify.publish_pubsub_alert")
    def test_log_only_when_pubsub_fails_and_nothing_else(self, mock_pubsub, caplog):
        from dags.notify import send_alert

        mock_pubsub.return_value = False
        with caplog.at_level(logging.INFO):
            assert send_alert("A", pubsub_topic=TOPIC) is False
        assert "Alert (log-only): A" in caplog.text

    @patch("dags.notify.publish_pubsub_alert")
    def test_skips_pubsub_without_topic(self, mock_pubsub):
        from dags.notify import send_alert

        send_alert("A", pubsub_topic=None)
        send_alert("A", pubsub_topic="")
        mock_pubsub.assert_not_called()

    def test_end_to_end_publish_without_telegram(self, pubsub):
        from dags.notify import send_alert

        _, _, session = pubsub
        assert send_alert(MONITOR_MESSAGE, pubsub_topic=TOPIC, title="T") is True
        assert "<b>" not in _decode(session.post)["text"]
