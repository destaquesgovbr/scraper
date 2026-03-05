"""Testes unitários para verificação de integridade de notícias."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from govbr_scraper.integrity.checker import check_content, check_image
from govbr_scraper.integrity.service import verify_batch


# --- check_image ---


class TestCheckImage:
    def test_no_image_url(self):
        result = check_image(None)
        assert result["image_status"] == "no_image"
        assert result["image_http_code"] is None

    def test_empty_image_url(self):
        result = check_image("")
        assert result["image_status"] == "no_image"

    @patch("govbr_scraper.integrity.checker.requests.head")
    def test_image_ok(self, mock_head):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"content-type": "image/jpeg"}
        mock_head.return_value = mock_resp

        result = check_image("https://www.gov.br/img.jpg")
        assert result["image_status"] == "ok"
        assert result["image_http_code"] == 200
        assert result["image_content_type"] == "image/jpeg"
        assert result["image_checked_at"] is not None

    @patch("govbr_scraper.integrity.checker.requests.head")
    def test_image_404(self, mock_head):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.headers = {"content-type": "text/html"}
        mock_head.return_value = mock_resp

        result = check_image("https://www.gov.br/img.jpg")
        assert result["image_status"] == "broken"
        assert result["image_http_code"] == 404

    @patch("govbr_scraper.integrity.checker.requests.head")
    def test_image_timeout(self, mock_head):
        mock_head.side_effect = requests.exceptions.Timeout()

        result = check_image("https://www.gov.br/img.jpg")
        assert result["image_status"] == "timeout"
        assert result["image_http_code"] is None

    @patch("govbr_scraper.integrity.checker.requests.head")
    def test_image_connection_error(self, mock_head):
        mock_head.side_effect = requests.exceptions.ConnectionError()

        result = check_image("https://www.gov.br/img.jpg")
        assert result["image_status"] == "broken"


# --- check_content ---


class TestCheckContent:
    def test_no_source_url(self):
        result = check_content(None)
        assert result["content_status"] == "error"

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_304_not_modified(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 304
        mock_get.return_value = mock_resp

        result = check_content(
            "https://www.gov.br/noticia",
            stored_hash="sha256:abc",
            stored_etag='"etag123"',
        )
        assert result["content_status"] == "unchanged"
        assert result["content_hash"] == "sha256:abc"

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_404_removed(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_get.return_value = mock_resp

        result = check_content("https://www.gov.br/noticia")
        assert result["content_status"] == "removed"

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_unchanged_same_hash(self, mock_get):
        html = b'<div id="content-core"><p>Texto da noticia</p></div>'
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = html
        mock_resp.text = html.decode()
        mock_resp.headers = {}
        mock_get.return_value = mock_resp

        # Primeiro call para obter o hash
        result1 = check_content("https://www.gov.br/noticia")
        assert result1["content_status"] == "unchanged"  # sem stored_hash, sempre unchanged
        stored_hash = result1["content_hash"]

        # Segundo call com mesmo conteúdo
        result2 = check_content("https://www.gov.br/noticia", stored_hash=stored_hash)
        assert result2["content_status"] == "unchanged"

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_changed_different_hash(self, mock_get):
        html = b'<div id="content-core"><p>Texto modificado</p></div>'
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = html
        mock_resp.text = html.decode()
        mock_resp.headers = {"etag": '"new_etag"'}
        mock_get.return_value = mock_resp

        result = check_content(
            "https://www.gov.br/noticia",
            stored_hash="sha256:hash_antigo",
        )
        assert result["content_status"] == "changed"
        assert result["content_hash"].startswith("sha256:")
        assert result["source_etag"] == '"new_etag"'

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_extracts_new_image_url(self, mock_get):
        html = b'<div id="content-core"><img src="https://gov.br/nova.jpg"/><p>Texto</p></div>'
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = html
        mock_resp.text = html.decode()
        mock_resp.headers = {}
        mock_get.return_value = mock_resp

        result = check_content("https://www.gov.br/noticia", stored_hash="sha256:old")
        assert result["new_image_url"] == "https://gov.br/nova.jpg"

    @patch("govbr_scraper.integrity.checker.requests.get")
    def test_content_timeout(self, mock_get):
        mock_get.side_effect = requests.exceptions.Timeout()

        result = check_content("https://www.gov.br/noticia")
        assert result["content_status"] == "error"


# --- verify_batch ---


class TestVerifyBatch:
    def test_empty_batch(self):
        result = verify_batch([])
        assert result["results"] == []
        assert result["summary"]["total"] == 0

    @patch("govbr_scraper.integrity.service.check_image")
    def test_batch_image_only(self, mock_check_image):
        mock_check_image.return_value = {
            "image_status": "ok",
            "image_http_code": 200,
            "image_checked_at": "2026-01-01T00:00:00Z",
            "image_content_type": "image/jpeg",
        }

        articles = [
            {"unique_id": "abc123", "image_url": "https://gov.br/img.jpg"},
            {"unique_id": "def456", "image_url": "https://gov.br/img2.jpg"},
        ]
        result = verify_batch(articles)

        assert result["summary"]["total"] == 2
        assert result["summary"]["images_ok"] == 2
        assert result["summary"]["content_unchecked"] == 2
        assert len(result["results"]) == 2

    @patch("govbr_scraper.integrity.service.check_content")
    @patch("govbr_scraper.integrity.service.check_image")
    def test_batch_with_content_check(self, mock_check_image, mock_check_content):
        mock_check_image.return_value = {
            "image_status": "broken",
            "image_http_code": 404,
            "image_checked_at": "2026-01-01T00:00:00Z",
            "image_content_type": None,
        }
        mock_check_content.return_value = {
            "content_status": "changed",
            "content_hash": "sha256:new",
            "content_checked_at": "2026-01-01T00:00:00Z",
            "source_etag": None,
            "new_image_url": "https://gov.br/nova.jpg",
        }

        articles = [
            {
                "unique_id": "abc123",
                "url": "https://gov.br/noticia",
                "image_url": "https://gov.br/img.jpg",
                "check_content": True,
            },
        ]
        result = verify_batch(articles)

        assert result["summary"]["total"] == 1
        assert result["summary"]["images_broken"] == 1
        assert result["summary"]["content_changed"] == 1
