"""
Pub/Sub event publisher for the scraper pipeline.

Publishes events to dgb.news.scraped topic after successful article inserts.
Graceful degradation: if Pub/Sub is unavailable, logs warning but does not
fail the scrape — articles are already persisted in PostgreSQL.
"""

import json
import os
import time
import uuid
from concurrent import futures as concurrent_futures
from datetime import UTC, datetime
from typing import Any

from loguru import logger


class EventPublisher:
    """
    Publishes news events to Google Cloud Pub/Sub.

    Requires PUBSUB_TOPIC_NEWS_SCRAPED env var.
    If not set or Pub/Sub client unavailable, all publishes are no-ops.

    ``close()`` (fim da requisição) espera as publicações pendentes e fecha o cliente.
    """

    # Publicação em lotes de BATCH_SIZE, com BATCH_PAUSE_SECONDS entre eles. O
    # enrichment-worker (concorrência 10, CPU com throttling) cai por liveness com ~40
    # mensagens de uma vez e perde em silêncio o dgb.news.enriched (artigo sem embedding e
    # fora do Typesense). A recuperação pela marca d'água (dias de notícias de uma agência)
    # ou um disparo com datas grava dezenas de artigos num INSERT só. Na execução normal
    # (poucas notícias novas por agência) não há pausa.
    BATCH_SIZE = 20
    BATCH_PAUSE_SECONDS = 30.0

    # Futures das publicações ainda não confirmadas (esperadas no close). Atributo de classe
    # como padrão para instâncias criadas sem __init__ (testes).
    _pending: list | None = None
    # Espera máxima pelas publicações pendentes no close().
    CLOSE_TIMEOUT_SECONDS = 10

    def __init__(self) -> None:
        self._topic: str | None = os.getenv("PUBSUB_TOPIC_NEWS_SCRAPED")
        self._client: Any = None
        self._enabled = False

        if not self._topic:
            logger.info("PUBSUB_TOPIC_NEWS_SCRAPED not set — event publishing disabled")
            return

        try:
            from google.cloud import pubsub_v1

            self._client = pubsub_v1.PublisherClient()
            self._enabled = True
            logger.info(f"Event publisher enabled: topic={self._topic}")
        except Exception as e:
            logger.warning(f"Failed to initialize Pub/Sub client — publishing disabled: {e}")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def publish_scraped(
        self,
        inserted_ids: list[dict[str, Any]],
    ) -> int:
        """
        Publish dgb.news.scraped events for newly inserted articles.

        Args:
            inserted_ids: List of dicts with keys: unique_id, agency_key, published_at

        Returns:
            Number of messages successfully published.
        """
        if not self._enabled or not inserted_ids:
            return 0

        published = 0
        trace_id = str(uuid.uuid4())

        for index, article in enumerate(inserted_ids):
            if index and index % self.BATCH_SIZE == 0:
                logger.info(
                    f"Publicados {index}/{len(inserted_ids)} eventos; pausa de "
                    f"{self.BATCH_PAUSE_SECONDS:.0f} s antes do próximo lote de "
                    f"{self.BATCH_SIZE} (liveness do enrichment-worker)"
                )
                time.sleep(self.BATCH_PAUSE_SECONDS)
            try:
                published_at = article.get("published_at")
                if isinstance(published_at, datetime):
                    published_at = published_at.isoformat()

                message = {
                    "unique_id": article["unique_id"],
                    "agency_key": article.get("agency_key", ""),
                    "published_at": published_at or "",
                    "scraped_at": datetime.now(UTC).isoformat(),
                }

                future = self._client.publish(
                    self._topic,
                    json.dumps(message).encode("utf-8"),
                    trace_id=trace_id,
                    event_version="1.0",
                )
                if self._pending is None:
                    self._pending = []
                self._pending.append(future)
                published += 1

            except Exception as e:
                logger.warning(f"Failed to publish event for {article.get('unique_id')}: {e}")

        if published:
            logger.info(f"Published {published}/{len(inserted_ids)} events to {self._topic}")

        return published

    def close(self, timeout: float | None = None) -> None:
        """Espera as publicações pendentes (até ``timeout`` s) e fecha o cliente.

        No Cloud Run a CPU é cortada depois da resposta: sem esperar, uma mensagem ainda no
        lote do cliente pode sair atrasada ou se perder. Nunca levanta exceção; publicações
        que falharam ou não confirmaram a tempo só viram warning (as notícias já estão no
        Postgres). Depois do close, o publisher fica desativado.
        """
        if not self._enabled or self._client is None:
            return
        pending = list(self._pending or [])
        self._pending = []
        self._enabled = False
        timeout = self.CLOSE_TIMEOUT_SECONDS if timeout is None else timeout
        try:
            self._client.stop()  # envia o lote pendente
        except Exception as e:
            logger.warning(f"Falha ao encerrar o publisher do Pub/Sub: {e}")
        all_done = True
        if pending:
            try:
                done, not_done = concurrent_futures.wait(pending, timeout=timeout)
                failed = sum(1 for f in done if f.exception() is not None)
                all_done = not not_done
                if failed or not_done:
                    logger.warning(
                        f"Pub/Sub: {failed} publicação(ões) com erro e {len(not_done)} sem "
                        f"confirmação em {timeout:.0f} s ({self._topic})"
                    )
            except Exception as e:
                all_done = False
                logger.warning(f"Falha ao esperar as publicações pendentes: {e}")
        if all_done:
            # Só com tudo confirmado: fechar o canal antes cancelaria o que falta.
            try:
                self._client.transport.close()
            except Exception as e:  # o cliente sai de cena de qualquer jeito
                logger.debug(f"Falha ao fechar o canal do Pub/Sub: {e}")
