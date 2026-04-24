from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from psycopg2 import errors

from govbr_scraper.models.news import NewsInsert
from govbr_scraper.storage.postgres_manager import PostgresManager


@pytest.fixture
def mock_pool():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = []
    mock_conn.cursor.return_value = mock_cursor

    mock_pool = MagicMock()
    mock_pool.getconn.return_value = mock_conn

    return mock_pool, mock_conn, mock_cursor


@pytest.fixture
def pg_manager(mock_pool):
    pool_obj, _, _ = mock_pool
    manager = PostgresManager.__new__(PostgresManager)
    manager.connection_string = "postgresql://test"
    manager.pool = pool_obj
    manager._agencies_by_key = {}
    manager._agencies_by_id = {}
    manager._themes_by_code = {}
    manager._themes_by_id = {}
    manager._cache_loaded = False
    return manager


def _make_news(unique_id, agency_key="ebc", url="https://example.com/article", title="Titulo", content="Conteudo"):
    return NewsInsert(
        unique_id=unique_id,
        agency_id=1,
        agency_key=agency_key,
        agency_name="EBC",
        title=title,
        url=url,
        content=content,
        published_at=datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc),
    )


class TestUrlBasedDedup:

    def test_same_url_same_agency_updates_existing(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid-123", "ebc", "https://example.com/article"),
        ]

        news = [_make_news("new-uid-456", title="Titulo editado")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        assert mock_exec.call_count == 1
        sql = mock_exec.call_args[0][1]
        rows = mock_exec.call_args[0][2]
        assert "UPDATE news SET" in sql
        assert rows[0][0] == "Titulo editado"
        assert rows[0][-1] == "existing-uid-123"
        assert count == 1
        assert articles[0]["unique_id"] == "existing-uid-123"

    def test_same_url_different_agency_inserts_both(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = []

        news = [
            _make_news("uid-ebc", agency_key="ebc", url="https://example.com/same"),
            _make_news("uid-mec", agency_key="mec", url="https://example.com/same"),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            return_value=[("uid-ebc",), ("uid-mec",)],
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        insert_call = mock_exec.call_args
        values = insert_call[0][2]
        assert len(values) == 2
        assert count == 2

    def test_null_url_not_deduped_by_url(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool

        news = [
            _make_news("uid-1", url=None),
            _make_news("uid-2", url=None),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            return_value=[("uid-1",), ("uid-2",)],
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        values = mock_exec.call_args[0][2]
        assert len(values) == 2

    def test_update_preserves_original_unique_id(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("original-uid", "ebc", "https://example.com/article"),
        ]

        news = [_make_news("new-uid-different")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles = pg_manager.insert(news)

        assert articles[0]["unique_id"] == "original-uid"

    def test_update_changes_title_content_content_hash(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article"),
        ]

        news = [_make_news(
            "new-uid",
            title="Novo titulo",
            content="Novo conteudo",
        )]
        news[0].content_hash = "abc123def456789a"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            pg_manager.insert(news)

        rows = mock_exec.call_args[0][2]
        assert rows[0][0] == "Novo titulo"
        assert rows[0][1] == "Novo conteudo"
        assert rows[0][2] == "abc123def456789a"
        assert rows[0][-1] == "existing-uid"

    def test_update_includes_updated_datetime_and_extracted_at(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article"),
        ]

        updated_dt = datetime(2026, 1, 2, 10, 0, tzinfo=timezone.utc)
        extracted_dt = datetime(2026, 1, 2, 12, 0, tzinfo=timezone.utc)
        news = [_make_news("new-uid")]
        news[0].updated_datetime = updated_dt
        news[0].extracted_at = extracted_dt

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            pg_manager.insert(news)

        sql = mock_exec.call_args[0][1]
        rows = mock_exec.call_args[0][2]
        assert "updated_datetime" in sql
        assert "extracted_at" in sql
        assert rows[0][10] == updated_dt
        assert rows[0][11] == extracted_dt

    def test_update_invalidates_embedding(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article"),
        ]

        news = [_make_news("new-uid", title="Titulo editado")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            pg_manager.insert(news)

        sql = mock_exec.call_args[0][1]
        assert "content_embedding = NULL" in sql
        assert "embedding_generated_at = NULL" in sql

    def test_mixed_batch_new_and_existing(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/existing"),
        ]

        news = [
            _make_news("existing-uid-new", url="https://example.com/existing"),
            _make_news("brand-new-uid", url="https://example.com/new"),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[None, [("brand-new-uid",)]],
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        assert mock_exec.call_count == 2
        update_sql = mock_exec.call_args_list[0][0][1]
        assert "UPDATE news SET" in update_sql
        insert_values = mock_exec.call_args_list[1][0][2]
        assert len(insert_values) == 1
        assert insert_values[0][0] == "brand-new-uid"
        assert count == 2
        assert len(articles) == 2

    def test_returns_metadata_for_updated_articles(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article"),
        ]

        news = [_make_news("new-uid")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles = pg_manager.insert(news)

        assert len(articles) == 1
        assert articles[0]["unique_id"] == "existing-uid"
        assert articles[0]["agency_key"] == "ebc"
        assert articles[0]["published_at"] == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

    def test_in_memory_url_dedup_keeps_last(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = []

        news = [
            _make_news("uid-old", title="Titulo antigo"),
            _make_news("uid-new", title="Titulo novo"),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            return_value=[("uid-new",)],
        ) as mock_exec:
            pg_manager.insert(news)

        values = mock_exec.call_args[0][2]
        assert len(values) == 1
        assert values[0][6] == "Titulo novo"

    def test_race_condition_retries_on_unique_violation(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article")],
        ]

        news = [_make_news("new-uid")]
        violation = errors.UniqueViolation()

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[violation, None],
        ):
            count, articles = pg_manager.insert(news)

        savepoint_calls = [
            c for c in mock_cursor.execute.call_args_list
            if isinstance(c[0][0], str) and "SAVEPOINT" in c[0][0]
        ]
        assert any("ROLLBACK TO SAVEPOINT" in c[0][0] for c in savepoint_calls)
        assert count == 1
        assert articles[0]["unique_id"] == "race-uid"
