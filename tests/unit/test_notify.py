"""Tests for notification helpers — Telegram alerts and fallback."""

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
            webhook_url="https://hooks.example.com/alert"
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
                webhook_url="https://hooks.example.com/alert"
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
            webhook_url="https://hooks.example.com/alert"
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
