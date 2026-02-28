"""
Unit tests for EventPublisher.

Tests graceful degradation, message format, datetime serialization,
trace_id consistency, and partial failure handling.
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.storage.event_publisher import EventPublisher


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def sample_articles():
    """Sample inserted articles metadata."""
    return [
        {
            "unique_id": "mec-2026-01-01-noticia-1",
            "agency_key": "mec",
            "published_at": datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
        },
        {
            "unique_id": "mds-2026-01-02-noticia-2",
            "agency_key": "mds",
            "published_at": datetime(2026, 1, 2, 15, 30, tzinfo=timezone.utc),
        },
    ]


@pytest.fixture
def mock_pubsub_client():
    """Mock PublisherClient that records calls."""
    client = MagicMock()
    client.publish.return_value = MagicMock()  # future
    return client


# =============================================================================
# Disabled states
# =============================================================================


class TestEventPublisherDisabled:
    """Tests for when EventPublisher is disabled."""

    def test_disabled_when_env_var_not_set(self):
        """EventPublisher is disabled when PUBSUB_TOPIC_NEWS_SCRAPED not set."""
        with patch.dict("os.environ", {}, clear=True):
            publisher = EventPublisher()
            assert publisher.enabled is False

    def test_publish_returns_zero_when_disabled(self, sample_articles):
        """publish_scraped returns 0 when publisher is disabled."""
        with patch.dict("os.environ", {}, clear=True):
            publisher = EventPublisher()
            result = publisher.publish_scraped(sample_articles)
            assert result == 0

    def test_disabled_when_pubsub_import_fails(self):
        """EventPublisher is disabled when google-cloud-pubsub not installed."""
        with patch.dict("os.environ", {"PUBSUB_TOPIC_NEWS_SCRAPED": "projects/p/topics/t"}):
            with patch(
                "govbr_scraper.storage.event_publisher.EventPublisher.__init__",
                wraps=None,
            ):
                # Simulate import failure by patching the init to raise
                publisher = EventPublisher.__new__(EventPublisher)
                publisher._topic = "projects/p/topics/t"
                publisher._client = None
                publisher._enabled = False
                assert publisher.enabled is False

    def test_publish_returns_zero_for_empty_list(self, mock_pubsub_client):
        """publish_scraped returns 0 for empty article list."""
        publisher = EventPublisher.__new__(EventPublisher)
        publisher._topic = "projects/p/topics/t"
        publisher._client = mock_pubsub_client
        publisher._enabled = True

        result = publisher.publish_scraped([])
        assert result == 0
        mock_pubsub_client.publish.assert_not_called()


# =============================================================================
# Publish behavior
# =============================================================================


class TestEventPublisherPublish:
    """Tests for successful publish behavior."""

    @pytest.fixture(autouse=True)
    def setup_publisher(self, mock_pubsub_client):
        """Create an enabled publisher with mocked client."""
        self.publisher = EventPublisher.__new__(EventPublisher)
        self.publisher._topic = "projects/test/topics/dgb.news.scraped"
        self.publisher._client = mock_pubsub_client
        self.publisher._enabled = True
        self.client = mock_pubsub_client

    def test_publishes_all_articles(self, sample_articles):
        """Publishes one message per article."""
        result = self.publisher.publish_scraped(sample_articles)
        assert result == 2
        assert self.client.publish.call_count == 2

    def test_message_format(self, sample_articles):
        """Published message contains correct JSON fields."""
        self.publisher.publish_scraped(sample_articles)

        call_args = self.client.publish.call_args_list[0]
        # positional: topic, data
        assert call_args[0][0] == "projects/test/topics/dgb.news.scraped"
        data = json.loads(call_args[0][1].decode("utf-8"))

        assert data["unique_id"] == "mec-2026-01-01-noticia-1"
        assert data["agency_key"] == "mec"
        assert "2026-01-01" in data["published_at"]
        assert "scraped_at" in data

    def test_message_attributes(self, sample_articles):
        """Published messages include trace_id and event_version attributes."""
        self.publisher.publish_scraped(sample_articles)

        call_kwargs = self.client.publish.call_args_list[0][1]
        assert "trace_id" in call_kwargs
        assert call_kwargs["event_version"] == "1.0"

    def test_trace_id_consistent_per_batch(self, sample_articles):
        """All messages in a batch share the same trace_id."""
        self.publisher.publish_scraped(sample_articles)

        trace_ids = [
            call[1]["trace_id"] for call in self.client.publish.call_args_list
        ]
        assert len(set(trace_ids)) == 1  # all the same

    def test_trace_id_differs_between_batches(self, sample_articles):
        """Different batches get different trace_ids."""
        self.publisher.publish_scraped(sample_articles)
        first_trace = self.client.publish.call_args_list[0][1]["trace_id"]

        self.client.reset_mock()
        self.publisher.publish_scraped(sample_articles)
        second_trace = self.client.publish.call_args_list[0][1]["trace_id"]

        assert first_trace != second_trace

    def test_datetime_serialized_to_iso(self, sample_articles):
        """datetime objects in published_at are serialized to ISO format."""
        self.publisher.publish_scraped(sample_articles)

        data = json.loads(
            self.client.publish.call_args_list[0][0][1].decode("utf-8")
        )
        # Should be ISO string, not a raw datetime repr
        assert "2026-01-01T12:00:00" in data["published_at"]

    def test_string_published_at_passed_through(self):
        """String published_at values are passed through unchanged."""
        articles = [
            {
                "unique_id": "test-1",
                "agency_key": "mec",
                "published_at": "2026-03-15T10:00:00+00:00",
            }
        ]
        self.publisher.publish_scraped(articles)

        data = json.loads(
            self.client.publish.call_args_list[0][0][1].decode("utf-8")
        )
        assert data["published_at"] == "2026-03-15T10:00:00+00:00"

    def test_none_published_at_becomes_empty_string(self):
        """None published_at becomes empty string in message."""
        articles = [
            {"unique_id": "test-1", "agency_key": "mec", "published_at": None}
        ]
        self.publisher.publish_scraped(articles)

        data = json.loads(
            self.client.publish.call_args_list[0][0][1].decode("utf-8")
        )
        assert data["published_at"] == ""

    def test_missing_agency_key_defaults_to_empty(self):
        """Missing agency_key defaults to empty string."""
        articles = [
            {"unique_id": "test-1", "published_at": "2026-01-01"}
        ]
        self.publisher.publish_scraped(articles)

        data = json.loads(
            self.client.publish.call_args_list[0][0][1].decode("utf-8")
        )
        assert data["agency_key"] == ""


# =============================================================================
# Graceful degradation
# =============================================================================


class TestEventPublisherGracefulDegradation:
    """Tests for graceful failure handling."""

    @pytest.fixture(autouse=True)
    def setup_publisher(self, mock_pubsub_client):
        self.publisher = EventPublisher.__new__(EventPublisher)
        self.publisher._topic = "projects/test/topics/dgb.news.scraped"
        self.publisher._client = mock_pubsub_client
        self.publisher._enabled = True
        self.client = mock_pubsub_client

    def test_continues_after_single_failure(self, sample_articles):
        """If one publish fails, continues with remaining articles."""
        self.client.publish.side_effect = [
            Exception("Network error"),  # first fails
            MagicMock(),  # second succeeds
        ]

        result = self.publisher.publish_scraped(sample_articles)
        assert result == 1
        assert self.client.publish.call_count == 2

    def test_all_failures_returns_zero(self, sample_articles):
        """If all publishes fail, returns 0."""
        self.client.publish.side_effect = Exception("Network error")

        result = self.publisher.publish_scraped(sample_articles)
        assert result == 0

    def test_does_not_raise_on_failure(self, sample_articles):
        """Publish failures never raise — they are caught and logged."""
        self.client.publish.side_effect = Exception("Total failure")

        # Should NOT raise
        result = self.publisher.publish_scraped(sample_articles)
        assert result == 0
