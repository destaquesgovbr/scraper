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

# Avoid circular import — ScrapeRunResult is used via TYPE_CHECKING
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from govbr_scraper.models.monitoring import ScrapeRunResult


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

    def get_recent_urls(self, agency_key: str, limit: int = 200) -> set[str]:
        """Return URLs of recent articles for an agency (used for known URL fence optimization)."""
        query = """
            SELECT url FROM news
            WHERE agency_key = %s AND url IS NOT NULL
            ORDER BY published_at DESC
            LIMIT %s
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (agency_key, limit))
                return {row[0] for row in cur.fetchall()}
        finally:
            self.pool.putconn(conn)

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

    def insert(
        self, news: list[NewsInsert], allow_update: bool = False
    ) -> tuple[int, list[dict]]:
        """
        Insert news records (batch operation).

        Args:
            news: List of news to insert
            allow_update: If True, update existing records (ON CONFLICT UPDATE)

        Returns:
            Tuple of (count, inserted_articles) where inserted_articles is a list
            of dicts with unique_id, agency_key, published_at for each inserted row.
        """
        if not news:
            raise ValueError("News list cannot be empty")

        # Deduplicate by unique_id (keep first occurrence)
        seen_ids: set[str] = set()
        deduped_news: list[NewsInsert] = []
        for n in news:
            if n.unique_id not in seen_ids:
                seen_ids.add(n.unique_id)
                deduped_news.append(n)

        if len(deduped_news) < len(news):
            logger.info(f"Removed {len(news) - len(deduped_news)} duplicate items by unique_id")

        # Deduplicate by (agency_key, url) in-memory (last wins = most recent title)
        seen_urls: dict[tuple[str, str], NewsInsert] = {}
        url_deduped: list[NewsInsert] = []
        for n in deduped_news:
            if n.url and n.agency_key:
                key = (n.agency_key, n.url)
                seen_urls[key] = n
            else:
                url_deduped.append(n)
        url_deduped.extend(seen_urls.values())

        if len(url_deduped) < len(deduped_news):
            logger.info(
                f"Removed {len(deduped_news) - len(url_deduped)} duplicate items by (agency_key, url)"
            )

        news = url_deduped

        logger.info(f"Inserting {len(news)} news records (allow_update={allow_update})")

        conn = self.get_connection()
        inserted = 0
        inserted_articles: list[dict] = []

        # Build lookup for metadata (unique_id -> news item)
        news_by_uid = {n.unique_id: n for n in news}

        try:
            cursor = conn.cursor()

            # Two-phase insert: pre-check URLs against existing records
            url_pairs = [(n.agency_key, n.url) for n in news if n.url and n.agency_key]
            existing_by_url = self._find_existing_by_url(url_pairs, cursor)

            to_update: list[tuple[str, NewsInsert]] = []
            to_insert: list[NewsInsert] = []
            for n in news:
                key = (n.agency_key, n.url) if n.url and n.agency_key else None
                if key and key in existing_by_url:
                    to_update.append((existing_by_url[key], n))
                else:
                    to_insert.append(n)

            if to_update:
                logger.info(f"Updating {len(to_update)} existing articles by URL match")

            # Phase 1: UPDATE existing articles matched by URL
            updated_articles = self._update_existing_articles(to_update, cursor)
            inserted_articles.extend(updated_articles)

            # Phase 2: INSERT new articles
            if to_insert:
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
                    "content_hash",
                    "published_at",
                    "updated_datetime",
                    "extracted_at",
                    "agency_key",
                    "agency_name",
                ]

                values = []
                for n in to_insert:
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
                            n.content_hash,
                            n.published_at,
                            n.updated_datetime,
                            n.extracted_at,
                            n.agency_key,
                            n.agency_name,
                        )
                    )

                insert_query = f"""
                    INSERT INTO news ({", ".join(columns)})
                    VALUES %s
                """

                if allow_update:
                    update_cols = [
                        c for c in columns if c not in ["unique_id", "agency_id", "published_at"]
                    ]
                    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                    insert_query += f"""
                        ON CONFLICT (unique_id)
                        DO UPDATE SET {update_set}, updated_at = NOW()
                    """
                else:
                    insert_query += " ON CONFLICT (unique_id) DO NOTHING"

                insert_query += " RETURNING unique_id"

                result = execute_values(cursor, insert_query, values, fetch=True)
                returned_ids = [row[0] for row in result]
                inserted = len(returned_ids)

                for uid in returned_ids:
                    n = news_by_uid.get(uid)
                    if n:
                        inserted_articles.append({
                            "unique_id": uid,
                            "agency_key": n.agency_key or "",
                            "published_at": n.published_at,
                        })

            conn.commit()

            total = inserted + len(updated_articles)
            logger.success(
                f"Inserted {inserted}, updated {len(updated_articles)} news records (total={total})"
            )
            return total, inserted_articles

        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting news: {e}")
            raise

        finally:
            cursor.close()
            self.put_connection(conn)

    def _find_existing_by_url(
        self,
        url_pairs: list[tuple[str, str]],
        cursor,
    ) -> dict[tuple[str, str], str]:
        if not url_pairs:
            return {}

        query = """
            SELECT unique_id, agency_key, url FROM news
            WHERE (agency_key, url) IN %s
        """
        cursor.execute(query, (tuple(url_pairs),))
        result: dict[tuple[str, str], str] = {}
        for row in cursor.fetchall():
            result[(row[1], row[2])] = row[0]
        return result

    def _update_existing_articles(
        self,
        updates: list[tuple[str, NewsInsert]],
        cursor,
    ) -> list[dict]:
        if not updates:
            return []

        updated_articles: list[dict] = []
        for existing_uid, new_data in updates:
            cursor.execute(
                """
                UPDATE news
                SET title = %s, content = %s, content_hash = %s,
                    summary = %s, image_url = %s, video_url = %s,
                    category = %s, tags = %s, editorial_lead = %s,
                    subtitle = %s, updated_at = NOW()
                WHERE unique_id = %s
                """,
                (
                    new_data.title,
                    new_data.content,
                    new_data.content_hash,
                    new_data.summary,
                    new_data.image_url,
                    new_data.video_url,
                    new_data.category,
                    new_data.tags,
                    new_data.editorial_lead,
                    new_data.subtitle,
                    existing_uid,
                ),
            )
            updated_articles.append({
                "unique_id": existing_uid,
                "agency_key": new_data.agency_key or "",
                "published_at": new_data.published_at,
            })
        return updated_articles

    def record_scrape_run(self, run: "ScrapeRunResult") -> None:
        """Record a scrape execution result.

        Args:
            run: The scrape run result to persist.
        """
        query = """
            INSERT INTO scrape_runs
                (agency_key, status, error_category, error_message,
                 articles_scraped, articles_saved, execution_time_seconds, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, (
                    run.agency_key,
                    run.status,
                    str(run.error_category) if run.error_category else None,
                    str(run.error_message)[:500] if run.error_message else None,
                    run.articles_scraped,
                    run.articles_saved,
                    run.execution_time_seconds,
                    run.scraped_at,
                ))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error recording scrape run for {run.agency_key}: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    def get_recent_runs(self, agency_key: str, limit: int = 5) -> list[dict]:
        """Get the most recent scrape runs for an agency.

        Args:
            agency_key: The agency identifier.
            limit: Maximum number of runs to return.

        Returns:
            List of run dicts ordered by scraped_at DESC.
        """
        query = """
            SELECT agency_key, status, error_category, error_message,
                   articles_scraped, articles_saved, execution_time_seconds, scraped_at
            FROM scrape_runs
            WHERE agency_key = %s
            ORDER BY scraped_at DESC
            LIMIT %s
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (agency_key, limit))
                return [dict(row) for row in cur.fetchall()]
        finally:
            self.pool.putconn(conn)

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
