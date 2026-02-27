"""
Storage Adapter for DestaquesGovBr Scraper.

Simplified postgres-only storage interface for the scraper service.
"""

import os
from collections import OrderedDict
from datetime import datetime
from typing import Any

from loguru import logger

from govbr_scraper.storage.postgres_manager import PostgresManager
from govbr_scraper.models.news import NewsInsert


class StorageAdapter:
    """
    Postgres-only storage adapter for the scraper service.

    Environment variables:
    - DATABASE_URL: PostgreSQL connection string
    """

    def __init__(self, postgres_manager: PostgresManager | None = None):
        """
        Initialize StorageAdapter.

        Args:
            postgres_manager: Optional pre-configured PostgresManager.
        """
        logger.info("StorageAdapter initialized: backend=postgres")
        self._postgres_manager = postgres_manager

    @property
    def postgres(self) -> PostgresManager:
        """Lazy-load PostgresManager."""
        if self._postgres_manager is None:
            logger.info("Initializing PostgresManager...")
            self._postgres_manager = PostgresManager()
            self._postgres_manager.load_cache()
        return self._postgres_manager

    def insert(self, new_data: OrderedDict, allow_update: bool = False) -> int:
        """
        Insert new records into PostgreSQL.

        Args:
            new_data: OrderedDict with arrays for each column
            allow_update: If True, update existing records with same unique_id

        Returns:
            Number of records inserted/updated
        """
        total_records = len(new_data.get("unique_id", []))
        logger.info(f"Inserting {total_records} records into PostgreSQL")

        news_list = self._convert_to_news_insert(new_data)
        if not news_list:
            logger.warning(
                f"No valid records to insert into PostgreSQL "
                f"(all {total_records} records were skipped due to missing required fields)"
            )
            return 0

        inserted = self.postgres.insert(news_list, allow_update=allow_update)
        logger.success(f"PostgreSQL: inserted {inserted} records")
        return inserted

    def _convert_to_news_insert(self, data: OrderedDict) -> list[NewsInsert]:
        """Convert OrderedDict data to list of NewsInsert objects."""
        news_list = []

        # Get number of records
        num_records = len(data.get("unique_id", []))

        def safe_get(field: str, default=None):
            """Safely get a value from data at index i, with default if missing."""
            values = data.get(field, [])
            return values[i] if i < len(values) else default

        for i in range(num_records):
            try:
                # Extract fields with defaults
                published_at = safe_get("published_at")
                if published_at is None:
                    record_url = safe_get("url", "unknown")
                    logger.warning(f"Skipping record {i}: missing published_at (url={record_url})")
                    continue

                # Parse datetime if string
                if isinstance(published_at, str):
                    published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))

                # Resolve agency_key to agency_id
                agency_key = safe_get("agency", "")
                agency = self.postgres._agencies_by_key.get(agency_key)
                if not agency:
                    logger.warning(f"Skipping record {i}: unknown agency '{agency_key}'")
                    continue
                agency_id = agency.id

                # Resolve theme codes to IDs (themes may not be present for new records)
                theme_l1_code = safe_get("theme_1_level_1_code")
                theme_l2_code = safe_get("theme_1_level_2_code")
                theme_l3_code = safe_get("theme_1_level_3_code")
                most_specific_code = safe_get("most_specific_theme_code")

                theme_l1_id = self._resolve_theme_id(theme_l1_code)
                theme_l2_id = self._resolve_theme_id(theme_l2_code)
                theme_l3_id = self._resolve_theme_id(theme_l3_code)
                most_specific_id = self._resolve_theme_id(most_specific_code)

                news = NewsInsert(
                    unique_id=safe_get("unique_id", ""),
                    agency_id=agency_id,
                    agency_key=agency_key,
                    agency_name=agency.name,
                    theme_l1_id=theme_l1_id,
                    theme_l2_id=theme_l2_id,
                    theme_l3_id=theme_l3_id,
                    most_specific_theme_id=most_specific_id,
                    title=safe_get("title", ""),
                    url=safe_get("url"),
                    image_url=safe_get("image"),  # HF uses 'image', not 'image_url'
                    video_url=safe_get("video_url"),
                    category=safe_get("category"),
                    tags=safe_get("tags") or [],
                    content=safe_get("content"),
                    editorial_lead=safe_get("editorial_lead"),
                    subtitle=safe_get("subtitle"),
                    summary=safe_get("summary"),
                    published_at=published_at,
                    updated_datetime=self._parse_datetime(safe_get("updated_datetime")),
                    extracted_at=self._parse_datetime(safe_get("extracted_at")),
                )
                news_list.append(news)
            except Exception as e:
                logger.warning(f"Error converting record {i}: {e}")
                continue

        return news_list

    def _resolve_theme_id(self, theme_code: str | None) -> int | None:
        """Resolve theme code to ID using cache."""
        if not theme_code:
            return None
        theme = self.postgres._themes_by_code.get(theme_code)
        return theme.id if theme else None

    def _parse_datetime(self, value: Any) -> datetime | None:
        """Parse datetime from various formats."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except:
                return None
        # Handle pandas Timestamp
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        return None
