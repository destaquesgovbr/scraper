"""Tests for notification helpers — Telegram alerts and fallback."""

import logging
from unittest.mock import MagicMock, patch

import pytest


class TestSendTelegramAlert:

    @patch("dags.notify.httpx")
    def test_posts_to_telegram_api(self, mock_httpx):
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
