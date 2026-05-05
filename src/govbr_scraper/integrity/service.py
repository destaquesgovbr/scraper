"""Orquestrador de verificação de integridade em batch."""

import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timezone

from govbr_scraper.integrity.checker import check_content, check_image

logger = logging.getLogger(__name__)

MAX_WORKERS = 20
DEFAULT_DEADLINE_SECONDS = 100  # < SCRAPER_REQUEST_TIMEOUT (120s) da DAG no data-platform


def _verify_article(article: dict) -> dict:
    """Verifica integridade de um único artigo.

    Args:
        article: Dict com unique_id, url, image_url, content_hash, source_etag, check_content.

    Returns:
        Dict com unique_id e resultados de imagem e conteúdo.
    """
    unique_id = article["unique_id"]
    result = {"unique_id": unique_id}

    image_result = check_image(article.get("image_url"))
    result.update(image_result)

    # Só incluir chaves de content quando de fato verificamos. Quando
    # check_content=False, o merge JSONB no data-platform (`features ||
    # new_features`) preserva o valor anterior; se injetássemos
    # content_status="unchecked" aqui, sobrescreveríamos um "unchanged"/
    # "changed" real da execução anterior.
    if article.get("check_content"):
        content_result = check_content(
            source_url=article.get("url"),
            stored_hash=article.get("content_hash"),
            stored_etag=article.get("source_etag"),
        )
        result.update(content_result)

    return result


def verify_batch(
    articles: list[dict],
    deadline_seconds: float | None = DEFAULT_DEADLINE_SECONDS,
) -> dict:
    """Verifica integridade de um batch de artigos em paralelo.

    Args:
        articles: Lista de dicts com unique_id, url, image_url, content_hash,
                  source_etag, check_content.
        deadline_seconds: Tempo máximo (segundos) para processar o batch
                          inteiro. Ao estourar, futures pendentes são
                          canceladas e marcadas como ``image_status=timeout``.
                          Default alinhado com o timeout HTTP da DAG Airflow
                          no data-platform (evita retry do Airflow com workers
                          órfãos continuando no Cloud Run).

    Returns:
        Dict com results (lista) e summary (contadores).
    """
    if not articles:
        return {"results": [], "summary": _empty_summary()}

    results = []
    pending_ids = {a["unique_id"] for a in articles}
    workers = min(MAX_WORKERS, len(articles))

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_verify_article, article): article["unique_id"]
            for article in articles
        }

        try:
            for future in as_completed(futures, timeout=deadline_seconds):
                uid = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Erro verificando {uid}: {e}")
                    results.append(
                        {
                            "unique_id": uid,
                            "image_status": "error",
                            "content_status": "error",
                        }
                    )
                pending_ids.discard(uid)
        except FuturesTimeoutError:
            logger.warning(
                f"Deadline de {deadline_seconds}s estourado com "
                f"{len(pending_ids)} artigos pendentes — marcando como timeout"
            )
            for future, uid in futures.items():
                if uid in pending_ids and not future.done():
                    future.cancel()
            now = datetime.now(timezone.utc).isoformat()
            for uid in pending_ids:
                results.append(
                    {
                        "unique_id": uid,
                        "image_status": "timeout",
                        "image_http_code": None,
                        "image_checked_at": now,
                        "image_content_type": None,
                    }
                )

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
        "content_baseline": 0,
        "content_unchanged": 0,
        "content_changed": 0,
        "content_removed": 0,
        "content_error": 0,
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

        # Artigos sem chave content_status (check_content=False) não contam.
        cnt = r.get("content_status")
        if cnt == "baseline":
            summary["content_baseline"] += 1
        elif cnt == "unchanged":
            summary["content_unchanged"] += 1
        elif cnt == "changed":
            summary["content_changed"] += 1
        elif cnt == "removed":
            summary["content_removed"] += 1
        elif cnt == "error":
            summary["content_error"] += 1

    return summary
