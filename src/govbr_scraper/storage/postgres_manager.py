"""
PostgreSQL Storage Manager for DestaquesGovBr Scraper.

Manages news storage in PostgreSQL with connection pooling, caching, and error handling.
"""

import os
import subprocess
from typing import Any, cast
from urllib.parse import quote_plus

from loguru import logger
from psycopg2 import extensions, pool
from psycopg2.extras import RealDictCursor, execute_values

from govbr_scraper.models.news import Agency, NewsInsert, Theme


class PostgresManager:
    """
    PostgreSQL storage manager with connection pooling and caching.

    Features:
    - Connection pooling for performance
    - In-memory cache for agencies and themes
    - Batch insert operations
    """

    def __init__(
        self,
        connection_string: str | None = None,
        min_connections: int = 1,
        max_connections: int = 10,
    ):
        """
        Initialize PostgresManager.

        Args:
            connection_string: PostgreSQL connection string. If None, auto-detect.
            min_connections: Minimum number of pooled connections
            max_connections: Maximum number of pooled connections
        """
        self.connection_string = connection_string or self._get_connection_string()
        self.pool = self._create_pool(min_connections, max_connections)

        # In-memory caches
        self._agencies_by_key: dict[str, Agency] = {}
        self._agencies_by_id: dict[int, Agency] = {}
        self._themes_by_code: dict[str, Theme] = {}
        self._themes_by_id: dict[int, Theme] = {}
        self._cache_loaded = False

    def _get_connection_string(self) -> str:
        """
        Get database connection string from environment, Secret Manager, or use localhost.

        Priority:
        1. DATABASE_URL environment variable
        2. Secret Manager (for Cloud deployment)
        3. Cloud SQL Proxy detection
        """
        # Check for DATABASE_URL environment variable first (for local development)
        database_url = os.getenv("DATABASE_URL", "").strip()
        if database_url:
            logger.info("Using DATABASE_URL from environment")
            return database_url

        try:
            # Try Secret Manager for Cloud deployment
            result = subprocess.run(
                [
                    "gcloud",
                    "secrets",
                    "versions",
                    "access",
                    "latest",
                    "--secret=destaquesgovbr-postgres-connection-string",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            secret_conn_str = result.stdout.strip()

            # Parse password from connection string
            if "://" in secret_conn_str and "@" in secret_conn_str:
                after_protocol = secret_conn_str.split("://")[1]
                user_pass, _ = after_protocol.rsplit("@", 1)
                if ":" in user_pass:
                    _, password = user_pass.split(":", 1)
                else:
                    password = "password"
            else:
                password = "password"

        except subprocess.CalledProcessError:
            logger.warning("Failed to fetch connection string from Secret Manager")
            password = "password"

        # Check if Cloud SQL Proxy is running
        proxy_check = subprocess.run(
            ["pgrep", "-f", "cloud-sql-proxy"],
            capture_output=True,
        )

        if proxy_check.returncode == 0:
            logger.info("Cloud SQL Proxy detected, using localhost connection")
            encoded_password = quote_plus(password)
            return (
                f"postgresql://destaquesgovbr_app:{encoded_password}@127.0.0.1:5432/destaquesgovbr"
            )

        # Return original secret for direct connection
        return secret_conn_str

    def _create_pool(self, min_conn: int, max_conn: int) -> pool.SimpleConnectionPool:
        """Create connection pool."""
        logger.info(f"Creating connection pool (min={min_conn}, max={max_conn})")
        return pool.SimpleConnectionPool(
            min_conn,
            max_conn,
            self.connection_string,
        )

    def get_connection(self) -> extensions.connection:
        """Get connection from pool."""
        return self.pool.getconn()

    def put_connection(self, conn: extensions.connection) -> None:
        """Return connection to pool."""
        self.pool.putconn(conn)

    def close_all(self) -> None:
        """Close all connections in pool."""
        logger.info("Closing all database connections")
        self.pool.closeall()

    def load_cache(self) -> None:
        """Load agencies and themes into memory cache."""
        if self._cache_loaded:
            logger.debug("Cache already loaded")
            return

        logger.info("Loading agencies and themes into cache...")
        conn = self.get_connection()

        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Load agencies
            cursor.execute("SELECT * FROM agencies")
            agencies = cursor.fetchall()
            for row in agencies:
                agency = Agency(**row)
                self._agencies_by_key[agency.key] = agency
                self._agencies_by_id[cast(int, agency.id)] = agency

            # Load themes
            cursor.execute("SELECT * FROM themes")
            themes = cursor.fetchall()
            for row in themes:
                theme = Theme(**row)
                self._themes_by_code[theme.code] = theme
                self._themes_by_id[cast(int, theme.id)] = theme

            self._cache_loaded = True
            logger.success(
                f"Cache loaded: {len(self._agencies_by_key)} agencies, "
                f"{len(self._themes_by_code)} themes"
            )

        finally:
            cursor.close()
            self.put_connection(conn)

    def insert(self, news: list[NewsInsert], allow_update: bool = False) -> int:
        """
        Insert news records (batch operation).

        Args:
            news: List of news to insert
            allow_update: If True, update existing records (ON CONFLICT UPDATE)

        Returns:
            Number of records inserted/updated
        """
        if not news:
            raise ValueError("News list cannot be empty")

        logger.info(f"Inserting {len(news)} news records (allow_update={allow_update})")

        conn = self.get_connection()
        inserted = 0

        try:
            cursor = conn.cursor()

            # Prepare INSERT query
            columns = [
                "unique_id",
                "agency_id",
                "theme_l1_id",
                "theme_l2_id",
                "theme_l3_id",
                "most_specific_theme_id",
                "title",
                "url",
                "image_url",
                "video_url",
                "category",
                "tags",
                "content",
                "editorial_lead",
                "subtitle",
                "summary",
                "published_at",
                "updated_datetime",
                "extracted_at",
                "agency_key",
                "agency_name",
            ]

            # Build values list
            values = []
            for n in news:
                values.append(
                    (
                        n.unique_id,
                        n.agency_id,
                        n.theme_l1_id,
                        n.theme_l2_id,
                        n.theme_l3_id,
                        n.most_specific_theme_id,
                        n.title,
                        n.url,
                        n.image_url,
                        n.video_url,
                        n.category,
                        n.tags,
                        n.content,
                        n.editorial_lead,
                        n.subtitle,
                        n.summary,
                        n.published_at,
                        n.updated_datetime,
                        n.extracted_at,
                        n.agency_key,
                        n.agency_name,
                    )
                )

            # Base INSERT
            insert_query = f"""
                INSERT INTO news ({", ".join(columns)})
                VALUES %s
            """

            if allow_update:
                # ON CONFLICT UPDATE
                update_cols = [
                    c for c in columns if c not in ["unique_id", "agency_id", "published_at"]
                ]
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                insert_query += f"""
                    ON CONFLICT (unique_id)
                    DO UPDATE SET {update_set}, updated_at = NOW()
                """
            else:
                # ON CONFLICT DO NOTHING
                insert_query += " ON CONFLICT (unique_id) DO NOTHING"

            # Execute batch insert
            execute_values(cursor, insert_query, values)
            inserted = cursor.rowcount
            conn.commit()

            logger.success(f"Inserted/updated {inserted} news records")
            return inserted

        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting news: {e}")
            raise

        finally:
            cursor.close()
            self.put_connection(conn)

    def __enter__(self) -> "PostgresManager":
        """Context manager entry."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Context manager exit."""
        self.close_all()
