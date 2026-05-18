"""Funções puras de verificação de integridade de notícias."""

import hashlib
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from govbr_scraper.scrapers.webscraper import DEFAULT_HEADERS

logger = logging.getLogger(__name__)

IMAGE_CHECK_TIMEOUT = 10
CONTENT_CHECK_TIMEOUT = 15
MAX_CONTENT_SIZE = 5 * 1024 * 1024  # 5 MB — limite de download para evitar OOM no Cloud Run
_CONTENT_CHUNK_SIZE = 65536


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
        is_image = content_type.startswith("image/")

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


def _extract_article_html(soup: BeautifulSoup):
    """Localiza o corpo do artigo no DOM.

    Espelha o seletor usado por ``WebScraper.get_article_content`` em produção
    (`div#content`) e adiciona fallback para Volto/Plone 6 antes de desistir.

    Retorna o elemento BeautifulSoup ou ``None`` se não for possível localizar
    um corpo confiável — o chamador deve tratar ``None`` como ``error`` em vez
    de hashear a página inteira, que contém header/footer variáveis e produz
    falso positivo ``changed`` permanente.
    """
    article_body = soup.find("div", id="content")
    if article_body:
        return article_body

    # Volto / Plone 6 (SPA). O scraper de produção consome API REST para essas
    # agências em plone6_api_scraper.py, mas tentar o DOM é um fallback barato.
    main = soup.find("main", id="main-content")
    if main:
        article = main.find("article")
        if article:
            return article

    return None


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
        resp = requests.get(source_url, headers=headers, timeout=timeout, stream=True)

        if resp.status_code == 304:
            resp.close()
            return {
                "content_status": "unchanged",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        if resp.status_code == 404:
            resp.close()
            return {
                "content_status": "removed",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": None,
                "new_image_url": None,
            }

        if resp.status_code != 200:
            resp.close()
            return {
                "content_status": "error",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        # Download limitado para evitar OOM (5MB). Páginas gov.br típicas
        # pesam <500KB; exceder o limite indica payload anômalo → error.
        chunks = bytearray()
        oversize = False
        for chunk in resp.iter_content(chunk_size=_CONTENT_CHUNK_SIZE):
            if not chunk:
                continue
            chunks.extend(chunk)
            if len(chunks) > MAX_CONTENT_SIZE:
                oversize = True
                break
        resp.close()

        if oversize:
            logger.warning(
                f"Conteúdo de {source_url} excedeu {MAX_CONTENT_SIZE} bytes — abortado"
            )
            return {
                "content_status": "error",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        body_bytes = bytes(chunks)
        soup = BeautifulSoup(body_bytes, "html.parser")
        article_body = _extract_article_html(soup)

        if article_body is None:
            logger.warning(f"Corpo do artigo não encontrado em {source_url}")
            return {
                "content_status": "error",
                "content_hash": stored_hash,
                "content_checked_at": now,
                "source_etag": stored_etag,
                "new_image_url": None,
            }

        body_html = str(article_body)
        new_hash = "sha256:" + hashlib.sha256(body_html.encode("utf-8")).hexdigest()

        new_image_url = None
        first_img = article_body.find("img")
        if first_img and first_img.get("src"):
            img_src = first_img["src"]
            new_image_url = urljoin(source_url, img_src) if not img_src.startswith("http") else img_src

        new_etag = resp.headers.get("etag")

        if stored_hash is None:
            # Primeira verificação: estabelece baseline, não conclui nada sobre
            # mudança. Tratar como "unchanged" inflaria métricas e mascararia
            # artigos sem histórico.
            content_status = "baseline"
        elif new_hash != stored_hash:
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
