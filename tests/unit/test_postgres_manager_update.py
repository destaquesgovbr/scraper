"""
Unit tests for PostgresManager._update_existing_articles()
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from govbr_scraper.models.news import NewsInsert


@pytest.fixture
def sample_update():
    """Sample update data with timezone-aware datetimes and tags array."""
    return [
        (
            "existing-uid",
            NewsInsert(
                unique_id="new-uid",
                agency_id=1,
                agency_key="agencia_brasil",
                agency_name="Agência Brasil",
                title="Test Article",
                url="https://agenciabrasil.ebc.com.br/test",
                content="Test content",
                content_hash="abc123",
                tags=["tag1", "tag2"],
                updated_datetime=datetime(2026, 6, 2, 12, 0, tzinfo=timezone.utc),
                extracted_at=datetime(2026, 6, 2, 12, 5, tzinfo=timezone.utc),
                published_at=datetime(2026, 6, 1, 10, 0, tzinfo=timezone.utc),
                category="Notícias",
            ),
        )
    ]


def test_update_query_has_type_casts(pg_manager, mock_pool, sample_update):
    """UPDATE query must cast tags::TEXT[] and timestamps::TIMESTAMPTZ."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        pg_manager._update_existing_articles(sample_update, mock_cursor)

    sql = mock_exec.call_args[0][1]
    assert "tags::TEXT[]" in sql
    assert "updated_datetime::TIMESTAMPTZ" in sql
    assert "extracted_at::TIMESTAMPTZ" in sql


def test_update_empty_list_skips_query(pg_manager, mock_pool):
    """Empty updates list should not call execute_values."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        result = pg_manager._update_existing_articles([], mock_cursor)

    assert result == []
    assert not mock_exec.called
