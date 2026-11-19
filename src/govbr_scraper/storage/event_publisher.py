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
from datetime import UTC, datetime
from typing import Any

from loguru import logger


class EventPublisher:
    """
    Publishes news events to Google Cloud Pub/Sub.

    Requires PUBSUB_TOPIC_NEWS_SCRAPED env var.
    If not set or Pub/Sub client unavailable, all publishes are no-ops.
    """

    # Publicação em lotes de BATCH_SIZE, com BATCH_PAUSE_SECONDS entre eles. O
    # enrichment-worker (concorrência 10, CPU com throttling) cai por liveness com ~40
    # mensagens de uma vez e perde em silêncio o dgb.news.enriched (artigo sem embedding e
    # fora do Typesense). A recuperação pela marca d'água (dias de notícias de uma agência)
    # ou um disparo com datas grava dezenas de artigos num INSERT só. Na execução normal
    # (poucas notícias novas por agência) não há pausa.
    BATCH_SIZE = 20
    BATCH_PAUSE_SECONDS = 30.0

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

                self._client.publish(
                    self._topic,
                    json.dumps(message).encode("utf-8"),
                    trace_id=trace_id,
                    event_version="1.0",
                )
                published += 1

            except Exception as e:
                logger.warning(f"Failed to publish event for {article.get('unique_id')}: {e}")

        if published:
            logger.info(f"Published {published}/{len(inserted_ids)} events to {self._topic}")

        return published
