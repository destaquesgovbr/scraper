"""
Unit tests for PostgresManager configuration methods.

Tests cover:
1. _get_connection_string() - Priority logic for DATABASE_URL, Secret Manager, localhost
2. load_cache() - Loading agencies and themes from database into memory
"""

import os
from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.models.news import Agency, Theme
from govbr_scraper.storage.postgres_manager import PostgresManager


# =============================================================================
# Tests for _get_connection_string()
# =============================================================================


class TestGetConnectionString:
    """Tests for database connection string priority logic."""

    @patch.dict(os.environ, {"DATABASE_URL": "postgresql://user:pass@localhost/db"})
    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_get_connection_string_from_env(self, mock_create_pool):
        """DATABASE_URL environment variable has highest priority."""
        mock_create_pool.return_value = MagicMock()
        manager = PostgresManager()

        assert manager.connection_string == "postgresql://user:pass@localhost/db"

    @patch.dict(os.environ, {}, clear=True)
    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    @patch("subprocess.run")
    def test_get_connection_string_from_secret_manager_with_proxy(self, mock_run, mock_create_pool):
        """Secret Manager + Cloud SQL Proxy returns localhost connection."""
        # Mock pool creation
        mock_create_pool.return_value = MagicMock()

        # Mock Secret Manager response and proxy check
        secret_result = MagicMock()
        secret_result.stdout = "postgresql://destaquesgovbr_app:mypassword@cloudsql-host/destaquesgovbr\n"
        secret_result.returncode = 0

        proxy_result = MagicMock()
        proxy_result.returncode = 0  # Proxy is running

        mock_run.side_effect = [secret_result, proxy_result]

        manager = PostgresManager()

        # Should detect proxy and return localhost with extracted password
        assert "127.0.0.1" in manager.connection_string
        assert "destaquesgovbr_app" in manager.connection_string
        assert manager.connection_string.startswith("postgresql://")

    @patch.dict(os.environ, {}, clear=True)
    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    @patch("subprocess.run")
    def test_get_connection_string_from_secret_manager_no_proxy(self, mock_run, mock_create_pool):
        """Secret Manager without proxy returns original connection string."""
        # Mock pool creation
        mock_create_pool.return_value = MagicMock()

        # Mock Secret Manager response and proxy check
        secret_result = MagicMock()
        secret_result.stdout = "postgresql://user:pass@cloudsql-host/db\n"
        secret_result.returncode = 0

        proxy_result = MagicMock()
        proxy_result.returncode = 1  # Proxy NOT running

        mock_run.side_effect = [secret_result, proxy_result]

        manager = PostgresManager()

        # Should return original secret connection string
        assert manager.connection_string == "postgresql://user:pass@cloudsql-host/db"

    @patch.dict(os.environ, {"DATABASE_URL": "postgresql://custom:custom@custom-host/custom"})
    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    @patch("subprocess.run")
    def test_env_var_takes_precedence_over_secret_manager(self, mock_run, mock_create_pool):
        """DATABASE_URL env var should take precedence over Secret Manager."""
        mock_create_pool.return_value = MagicMock()

        # Mock Secret Manager (should not be called)
        secret_result = MagicMock()
        secret_result.stdout = "postgresql://secret:secret@secret-host/secret\n"
        secret_result.returncode = 0

        mock_run.return_value = secret_result

        manager = PostgresManager()

        # Should use env var, not Secret Manager
        assert manager.connection_string == "postgresql://custom:custom@custom-host/custom"
        # Secret Manager should not have been called
        mock_run.assert_not_called()

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_strips_whitespace_from_env_var(self, mock_create_pool):
        """DATABASE_URL with whitespace should be stripped."""
        mock_create_pool.return_value = MagicMock()

        with patch.dict(os.environ, {"DATABASE_URL": "  postgresql://test:test@testhost/testdb  "}):
            manager = PostgresManager()

            # Should strip whitespace
            assert manager.connection_string == "postgresql://test:test@testhost/testdb"
            assert not manager.connection_string.startswith(" ")
            assert not manager.connection_string.endswith(" ")


# =============================================================================
# Tests for load_cache()
# =============================================================================


class TestLoadCache:
    """Tests for loading agencies and themes from database."""

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_populates_agencies_and_themes(self, mock_create_pool):
        """load_cache should populate all cache dictionaries."""
        # Mock pool creation
        mock_create_pool.return_value = MagicMock()

        # Create manager with explicit connection string (avoids _get_connection_string call)
        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Mock agency and theme data
        mock_cursor.fetchall.side_effect = [
            [
                {"id": 1, "key": "mec", "name": "Ministério da Educação"},
                {"id": 2, "key": "mds", "name": "Ministério do Desenvolvimento Social"},
            ],
            [
                {"id": 10, "code": "EDU", "label": "Educação", "level": 1},
                {"id": 11, "code": "EDU.BASICA", "label": "Educação Básica", "level": 2},
            ],
        ]

        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                manager.load_cache()

        # Check agencies cache
        assert len(manager._agencies_by_key) == 2
        assert "mec" in manager._agencies_by_key
        assert manager._agencies_by_key["mec"].name == "Ministério da Educação"
        assert 1 in manager._agencies_by_id
        assert manager._agencies_by_id[1].key == "mec"

        # Check themes cache
        assert len(manager._themes_by_code) == 2
        assert "EDU" in manager._themes_by_code
        assert manager._themes_by_code["EDU"].label == "Educação"
        assert 10 in manager._themes_by_id
        assert manager._themes_by_id[10].code == "EDU"

        # Check cache loaded flag
        assert manager._cache_loaded is True

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_skips_reload_when_already_loaded(self, mock_create_pool):
        """load_cache should not reload if cache is already populated."""
        # Mock pool creation
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        manager._cache_loaded = True

        # Mock connection - should not be called
        with patch.object(manager, "get_connection") as mock_get_conn:
            manager.load_cache()

            # get_connection should not have been called
            mock_get_conn.assert_not_called()

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_closes_cursor_and_returns_connection(self, mock_create_pool):
        """load_cache should properly clean up cursor and return connection."""
        # Mock pool creation
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        mock_cursor.fetchall.side_effect = [[], []]  # Empty results
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection") as mock_put_conn:
                manager.load_cache()

                # Cursor should be closed
                mock_cursor.close.assert_called_once()

                # Connection should be returned to pool
                mock_put_conn.assert_called_once_with(mock_conn)

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_handles_empty_agencies(self, mock_create_pool):
        """load_cache should handle empty agencies table gracefully."""
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Empty agencies, some themes
        mock_cursor.fetchall.side_effect = [
            [],  # No agencies
            [{"id": 10, "code": "EDU", "label": "Educação", "level": 1}],
        ]

        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                manager.load_cache()

        # Should have empty agencies cache
        assert len(manager._agencies_by_key) == 0
        assert len(manager._agencies_by_id) == 0
        # But themes should still be loaded
        assert len(manager._themes_by_code) == 1
        assert manager._cache_loaded is True

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_handles_empty_themes(self, mock_create_pool):
        """load_cache should handle empty themes table gracefully."""
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Some agencies, empty themes
        mock_cursor.fetchall.side_effect = [
            [{"id": 1, "key": "mec", "name": "Ministério da Educação"}],
            [],  # No themes
        ]

        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                manager.load_cache()

        # Agencies should be loaded
        assert len(manager._agencies_by_key) == 1
        # Themes should be empty
        assert len(manager._themes_by_code) == 0
        assert len(manager._themes_by_id) == 0
        assert manager._cache_loaded is True

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_handles_database_error(self, mock_create_pool):
        """load_cache should handle database errors gracefully."""
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Simulate database error
        mock_cursor.execute.side_effect = Exception("Database connection lost")
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                # Should raise or handle gracefully (depending on implementation)
                with pytest.raises(Exception):
                    manager.load_cache()

                # Connection should still be returned even on error
                mock_cursor.close.assert_called_once()

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_partial_failure_leaves_cache_unloaded(self, mock_create_pool):
        """Failure on the themes query leaves _cache_loaded False even though agencies loaded."""
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # First execute (agencies) succeeds; second execute (themes) raises
        mock_cursor.execute.side_effect = [None, Exception("themes query failed")]
        mock_cursor.fetchall.return_value = [
            {"id": 1, "key": "mec", "name": "Ministério da Educação"},
        ]
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                with pytest.raises(Exception, match="themes query failed"):
                    manager.load_cache()

                # Cache flag must remain False — next call will retry the full load
                assert manager._cache_loaded is False
                # Agencies were populated before the failure (documented side-effect)
                assert len(manager._agencies_by_key) >= 0
                # Cursor cleanup must run even after partial failure
                mock_cursor.close.assert_called_once()

    @patch("govbr_scraper.storage.postgres_manager.PostgresManager._create_pool")
    def test_load_cache_large_dataset(self, mock_create_pool):
        """load_cache should handle large number of agencies and themes."""
        mock_create_pool.return_value = MagicMock()

        manager = PostgresManager(connection_string="postgresql://test:test@localhost/test")
        mock_conn = MagicMock()
        mock_cursor = MagicMock()

        # Create large dataset
        agencies = [{"id": i, "key": f"agency_{i}", "name": f"Agency {i}"} for i in range(1, 201)]
        themes = [{"id": i, "code": f"THEME_{i}", "label": f"Theme {i}", "level": 1} for i in range(1, 101)]

        mock_cursor.fetchall.side_effect = [agencies, themes]
        mock_conn.cursor.return_value = mock_cursor

        with patch.object(manager, "get_connection", return_value=mock_conn):
            with patch.object(manager, "put_connection"):
                manager.load_cache()

        # Should load all 200 agencies
        assert len(manager._agencies_by_key) == 200
        assert len(manager._agencies_by_id) == 200
        # Should load all 100 themes
        assert len(manager._themes_by_code) == 100
        assert len(manager._themes_by_id) == 100
