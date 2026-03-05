"""Orquestrador de verificação de integridade em batch."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from govbr_scraper.integrity.checker import check_content, check_image

logger = logging.getLogger(__name__)

MAX_WORKERS = 20


def _verify_article(article: dict) -> dict:
    """Verifica integridade de um único artigo.

    Args:
        article: Dict com unique_id, url, image_url, content_hash, source_etag, check_content.

    Returns:
        Dict com unique_id e resultados de imagem e conteúdo.
    """
    unique_id = article["unique_id"]
    result = {"unique_id": unique_id}

    # Verificar imagem
    image_result = check_image(article.get("image_url"))
    result.update(image_result)

    # Verificar conteúdo (apenas se solicitado)
    if article.get("check_content"):
        content_result = check_content(
            source_url=article.get("url"),
            stored_hash=article.get("content_hash"),
            stored_etag=article.get("source_etag"),
        )
        result.update(content_result)
    else:
        result["content_status"] = "unchecked"

    return result


def verify_batch(articles: list[dict]) -> dict:
    """Verifica integridade de um batch de artigos em paralelo.

    Args:
        articles: Lista de dicts com unique_id, url, image_url, content_hash,
                  source_etag, check_content.

    Returns:
        Dict com results (lista) e summary (contadores).
    """
    if not articles:
        return {"results": [], "summary": _empty_summary()}

    results = []
    workers = min(MAX_WORKERS, len(articles))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_verify_article, article): article["unique_id"]
            for article in articles
        }

        for future in as_completed(futures):
            uid = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Erro verificando {uid}: {e}")
                results.append({"unique_id": uid, "image_status": "error", "content_status": "error"})

    summary = _compute_summary(results)
    logger.info(
        f"Verificação concluída: {summary['total']} artigos, "
        f"{summary['images_broken']} imagens quebradas, "
        f"{summary['content_changed']} conteúdos alterados"
    )

    return {"results": results, "summary": summary}


def _empty_summary() -> dict:
    return {
        "total": 0,
        "images_ok": 0,
        "images_broken": 0,
        "images_timeout": 0,
        "images_no_image": 0,
        "content_unchanged": 0,
        "content_changed": 0,
        "content_removed": 0,
        "content_error": 0,
        "content_unchecked": 0,
    }


def _compute_summary(results: list[dict]) -> dict:
    summary = _empty_summary()
    summary["total"] = len(results)

    for r in results:
        img = r.get("image_status", "error")
        if img == "ok":
            summary["images_ok"] += 1
        elif img == "broken":
            summary["images_broken"] += 1
        elif img == "timeout":
            summary["images_timeout"] += 1
        elif img == "no_image":
            summary["images_no_image"] += 1

        cnt = r.get("content_status", "unchecked")
        if cnt == "unchanged":
            summary["content_unchanged"] += 1
        elif cnt == "changed":
            summary["content_changed"] += 1
        elif cnt == "removed":
            summary["content_removed"] += 1
        elif cnt == "error":
            summary["content_error"] += 1
        elif cnt == "unchecked":
            summary["content_unchecked"] += 1

    return summary
