"""
Pub/Sub event publisher for the scraper pipeline.

Publishes events to dgb.news.scraped topic after successful article inserts.
Graceful degradation: if Pub/Sub is unavailable, logs warning but does not
fail the scrape — articles are already persisted in PostgreSQL.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from loguru import logger


class EventPublisher:
    """
    Publishes news events to Google Cloud Pub/Sub.

    Requires PUBSUB_TOPIC_NEWS_SCRAPED env var.
    If not set or Pub/Sub client unavailable, all publishes are no-ops.
    """

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

        for article in inserted_ids:
            try:
                published_at = article.get("published_at")
                if isinstance(published_at, datetime):
                    published_at = published_at.isoformat()

                message = {
                    "unique_id": article["unique_id"],
                    "agency_key": article.get("agency_key", ""),
                    "published_at": published_at or "",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                }

                self._client.publish(
                    self._topic,
                    json.dumps(message).encode("utf-8"),
                    trace_id=trace_id,
                    event_version="1.0",
                )
                published += 1

            except Exception as e:
                logger.warning(
                    f"Failed to publish event for {article.get('unique_id')}: {e}"
                )

        if published:
            logger.info(f"Published {published}/{len(inserted_ids)} events to {self._topic}")

        return published
