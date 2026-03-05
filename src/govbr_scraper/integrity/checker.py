"""Funções puras de verificação de integridade de notícias."""

import hashlib
import logging
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from govbr_scraper.scrapers.webscraper import DEFAULT_HEADERS

logger = logging.getLogger(__name__)

IMAGE_CHECK_TIMEOUT = 10
CONTENT_CHECK_TIMEOUT = 15


def check_image(image_url: str, timeout: int = IMAGE_CHECK_TIMEOUT) -> dict:
    """HTTP HEAD na URL da imagem para verificar disponibilidade.

    Args:
        image_url: URL da imagem a verificar.
        timeout: Timeout em segundos.

    Returns:
        Dict com image_status, image_http_code, image_checked_at, image_content_type.
    """
    now = datetime.now(timezone.utc).isoformat()

    if not image_url:
        return {
            "image_status": "no_image",
            "image_http_code": None,
            "image_checked_at": now,
            "image_content_type": None,
        }

    try:
        resp = requests.head(
            image_url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            allow_redirects=True,
        )
        content_type = resp.headers.get("content-type", "")
        is_image = content_type.startswith("image/") or resp.status_code == 200

        if resp.status_code == 200 and is_image:
            status = "ok"
        elif 300 <= resp.status_code < 400:
            status = "redirect"
        else:
            status = "broken"

        return {
            "image_status": status,
            "image_http_code": resp.status_code,
            "image_checked_at": now,
            "image_content_type": content_type,
        }
    except requests.exceptions.Timeout:
        return {
            "image_status": "timeout",
            "image_http_code": None,
            "image_checked_at": now,
            "image_content_type": None,
        }
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao verificar imagem {image_url}: {e}")
        return {
            "image_status": "broken",
            "image_http_code": None,
            "image_checked_at": now,
            "image_content_type": None,
        }


def check_content(
    source_url: str,
    stored_hash: str | None = None,
    stored_etag: str | None = None,
    timeout: int = CONTENT_CHECK_TIMEOUT,
) -> dict:
    """GET condicional na URL fonte para detectar mudanças de conteúdo.

    Usa If-None-Match (ETag) quando disponível para evitar download completo.
    Compara hash SHA-256 do HTML do corpo da notícia.

    Args:
        source_url: URL original da notícia.
        stored_hash: Hash SHA-256 armazenado da última verificação.
        stored_etag: ETag armazenado da última verificação.
        timeout: Timeout em segundos.

    Returns:
        Dict com content_status, content_hash, content_checked_at, source_etag, new_image_url.
    """
    now = datetime.now(timezone.utc).isoformat()

    if not source_url:
        return {
            "content_status": "error",
            "content_hash": stored_hash,
            "content_checked_at": now,
            "source_etag": stored_etag,
            "new_image_url": None,
        }

    headers = dict(DEFAULT_HEADERS)
    if stored_etag:
        headers["If-None-Match"] = stored_etag

    try:
        resp = requests.get(source_url, headers=headers, timeout=timeout)

        if resp.status_code == 304:
            return {
                "content_status": "unchanged",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        if resp.status_code == 404:
            return {
                "content_status": "removed",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": None,
                "new_image_url": None,
            }

        if resp.status_code != 200:
            return {
                "content_status": "error",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        # Extrair corpo do artigo e calcular hash
        soup = BeautifulSoup(resp.content, "html.parser")
        article_body = soup.find("div", {"id": "content-core"}) or soup.find(
            "div", class_="content"
        )

        body_html = str(article_body) if article_body else resp.text
        new_hash = "sha256:" + hashlib.sha256(body_html.encode("utf-8")).hexdigest()

        # Extrair imagem atual da página
        new_image_url = None
        if article_body:
            first_img = article_body.find("img")
            if first_img and first_img.get("src"):
                new_image_url = first_img["src"]

        new_etag = resp.headers.get("etag")

        if stored_hash and new_hash != stored_hash:
            content_status = "changed"
        else:
            content_status = "unchanged"

        return {
            "content_status": content_status,
            "content_hash": new_hash,
            "content_checked_at": now,
            "source_etag": new_etag,
            "new_image_url": new_image_url,
        }
    except requests.exceptions.Timeout:
        return {
            "content_status": "error",
            "content_hash": stored_hash,
            "content_checked_at": now,
            "source_etag": stored_etag,
            "new_image_url": None,
        }
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro ao verificar conteúdo {source_url}: {e}")
        return {
            "content_status": "error",
            "content_hash": stored_hash,
            "content_checked_at": now,
            "source_etag": stored_etag,
            "new_image_url": None,
        }
