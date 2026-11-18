"""
Unit tests for StorageAdapter event publishing after insert.

Tests that events are published after successful inserts,
not published on empty inserts, and not published on failures.
All external dependencies (PostgresManager, EventPublisher) are mocked.
"""

from collections import OrderedDict
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.models.news import Agency
from govbr_scraper.storage.postgres_manager import InsertResult
from govbr_scraper.storage.storage_adapter import InsertCounts, StorageAdapter

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_postgres():
    """Mock PostgresManager."""
    pg = MagicMock()
    pg._agencies_by_key = {
        "mec": Agency(id=1, key="mec", name="Ministério da Educação"),
    }
    pg._themes_by_code = {}
    return pg


@pytest.fixture
def mock_event_publisher():
    """Mock EventPublisher."""
    publisher = MagicMock()
    publisher.enabled = True
    publisher.publish_scraped.return_value = 2
    return publisher


@pytest.fixture
def adapter(mock_postgres, mock_event_publisher):
    """StorageAdapter with mocked dependencies."""
    with patch(
        "govbr_scraper.storage.storage_adapter.EventPublisher",
        return_value=mock_event_publisher,
    ):
        sa = StorageAdapter(postgres_manager=mock_postgres)
    return sa


@pytest.fixture
def sample_data():
    """Sample OrderedDict input data for insert."""
    return OrderedDict(
        {
            "unique_id": ["mec-2026-01-01-noticia-1", "mec-2026-01-02-noticia-2"],
            "title": ["Notícia 1", "Notícia 2"],
            "published_at": [
                datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
                datetime(2026, 1, 2, 15, 0, tzinfo=UTC),
            ],
            "agency": ["mec", "mec"],
        }
    )


# =============================================================================
# Publish after insert
# =============================================================================


class TestPublishAfterInsert:
    """Tests that events are published after successful insert."""

    def test_publish_called_after_insert(
        self, adapter, mock_postgres, mock_event_publisher, sample_data
    ):
        """publish_scraped is called with inserted articles."""
        inserted_articles = [
            {
                "unique_id": "mec-2026-01-01-noticia-1",
                "agency_key": "mec",
                "published_at": datetime(2026, 1, 1),
            },
        ]
        mock_postgres.insert.return_value = InsertResult(1, inserted_articles, 1)

        adapter.insert(sample_data)

        mock_event_publisher.publish_scraped.assert_called_once_with(inserted_articles)

    def test_publish_receives_all_articles(
        self, adapter, mock_postgres, mock_event_publisher, sample_data
    ):
        """All inserted articles are passed to publish_scraped."""
        inserted_articles = [
            {
                "unique_id": "mec-2026-01-01-noticia-1",
                "agency_key": "mec",
                "published_at": datetime(2026, 1, 1),
            },
            {
                "unique_id": "mec-2026-01-02-noticia-2",
                "agency_key": "mec",
                "published_at": datetime(2026, 1, 2),
            },
        ]
        mock_postgres.insert.return_value = InsertResult(2, inserted_articles, 2)

        adapter.insert(sample_data)

        args = mock_event_publisher.publish_scraped.call_args[0][0]
        assert len(args) == 2

    def test_insert_returns_counts_only(
        self, adapter, mock_postgres, mock_event_publisher, sample_data
    ):
        """StorageAdapter.insert() returns only the counts (saved, new), not the articles."""
        mock_postgres.insert.return_value = InsertResult(3, [{"unique_id": "x"}] * 3, 1)

        result = adapter.insert(sample_data)

        assert result == InsertCounts(saved=3, new=1)
        assert isinstance(result, InsertCounts)


# =============================================================================
# No publish when empty
# =============================================================================


class TestNoPublishOnEmpty:
    """Tests that events are NOT published when no articles inserted."""

    def test_no_publish_on_zero_inserts(
        self, adapter, mock_postgres, mock_event_publisher, sample_data
    ):
        """publish_scraped not called when insert returns empty list."""
        mock_postgres.insert.return_value = InsertResult(0, [], 0)

        adapter.insert(sample_data)

        mock_event_publisher.publish_scraped.assert_not_called()

    def test_no_publish_on_invalid_data(self, adapter, mock_postgres, mock_event_publisher):
        """publish_scraped not called when all records are invalid."""
        bad_data = OrderedDict(
            {
                "unique_id": ["test-1"],
                "title": ["Test"],
                "published_at": [None],  # Will be skipped
                "agency": ["unknown_agency"],  # Will be skipped
            }
        )

        result = adapter.insert(bad_data)

        assert result == InsertCounts(saved=0, new=0)
        mock_event_publisher.publish_scraped.assert_not_called()


# =============================================================================
# No publish on failure
# =============================================================================


class TestNoPublishOnFailure:
    """Tests that events are NOT published when insert fails."""

    def test_no_publish_on_insert_exception(
        self, adapter, mock_postgres, mock_event_publisher, sample_data
    ):
        """publish_scraped not called when postgres.insert() raises."""
        mock_postgres.insert.side_effect = Exception("DB connection lost")

        with pytest.raises(Exception, match="DB connection lost"):
            adapter.insert(sample_data)

        mock_event_publisher.publish_scraped.assert_not_called()
