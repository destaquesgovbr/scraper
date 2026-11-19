"""
PostgreSQL Storage Manager for DestaquesGovBr Scraper.

Manages news storage in PostgreSQL with connection pooling, caching, and error handling.
"""

import os
import subprocess  # nosec B404

# Avoid circular import — ScrapeRunResult is used via TYPE_CHECKING
from typing import TYPE_CHECKING, Any, NamedTuple, cast
from urllib.parse import quote_plus

from loguru import logger
from psycopg2 import errors, extensions, pool
from psycopg2.extras import RealDictCursor, execute_values

from govbr_scraper.models.news import Agency, NewsInsert, Theme

if TYPE_CHECKING:
    from govbr_scraper.models.monitoring import ScrapeRunResult


class _ExistingArticle(NamedTuple):
    """Artigo já gravado, casado por (agency_key, url) no pré-check da Phase 1."""

    unique_id: str
    content_hash: str | None


class _UrlMatch(NamedTuple):
    """Pré-check por (agency_key, url) (``PostgresManager._match_existing_by_url``).

    - ``to_update``: pares (unique_id existente, dados do re-scrape) que vão para o UPDATE
      da Phase 1;
    - ``to_insert``: os que não casaram, para o INSERT da Phase 2;
    - ``unchanged_uids``: unique_ids existentes com o mesmo ``content_hash`` do re-scrape.
      Nunca são republicados. Com ``allow_update=False`` também ficam fora de ``to_update``
      (sem UPDATE e fora de ``articles_saved``); com ``allow_update=True`` são atualizados.
    """

    to_update: list[tuple[str, NewsInsert]]
    to_insert: list[NewsInsert]
    unchanged_uids: set[str]


class InsertResult(NamedTuple):
    """Resultado de ``PostgresManager.insert``.

    - ``saved``: linhas gravadas, inseridas + atualizadas (vai para ``articles_saved``);
    - ``articles``: artigos a publicar em ``dgb.news.scraped`` (ver ``insert``);
    - ``new``: linhas de fato inseridas em ``news`` (vai para ``articles_new``).
    """

    saved: int
    articles: list[dict]
    new: int


class _Phase2Result(NamedTuple):
    """Resultado do INSERT da Phase 2 (incluindo o retry do UniqueViolation)."""

    written: int  # linhas devolvidas pelo RETURNING + as atualizadas no retry
    new: int  # só as inseridas
    articles: list[dict]


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
        self._connection_string = connection_string or self._get_connection_string()
        self.pool = self._create_pool(min_connections, max_connections)

        # In-memory caches
        self._agencies_by_key: dict[str, Agency] = {}
        self._agencies_by_id: dict[int, Agency] = {}
        self._themes_by_code: dict[str, Theme] = {}
        self._themes_by_id: dict[int, Theme] = {}
        self._cache_loaded = False

    def __repr__(self) -> str:
        return f"PostgresManager(pool_size={self.pool.minconn}-{self.pool.maxconn})"

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
            # Arguments are fixed and shell execution is disabled.
            result = subprocess.run(  # nosec B603 B607
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
                    # Local development fallback; production uses Secret Manager.
                    password = "password"  # nosec B105
            else:
                # Local development fallback; production uses Secret Manager.
                password = "password"  # nosec B105

        except subprocess.CalledProcessError:
            logger.warning("Failed to fetch connection string from Secret Manager")
            # Local development fallback; production uses Secret Manager.
            password = "password"  # nosec B105

        # Check if Cloud SQL Proxy is running
        # Arguments are fixed and shell execution is disabled.
        proxy_check = subprocess.run(  # nosec B603 B607
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
            self._connection_string,
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

    def insert(self, news: list[NewsInsert], allow_update: bool = False) -> InsertResult:
        """
        Insert news records (batch operation).

        Artigo que já existe com o mesmo (agency_key, url) é o re-scrape (Phase 1):

        - ``allow_update=False`` (o das DAGs): só é atualizado se o ``content_hash``
          mudou (hash gravado NULL conta como mudou). Com o mesmo hash não há UPDATE,
          não conta em ``saved`` e não é republicado;
        - ``allow_update=True`` (forçar regravação, ex.: depois de corrigir o parser):
          todo artigo casado é atualizado e conta em ``saved``, mas só o de hash
          diferente é republicado.

        O artigo sem tema não é mais republicado no re-scrape: o retry de um
        enriquecimento que falhou fica com o reconciliador diário do ``/dispatch``
        (Passo 3 da rearquitetura da ingestão).

        Args:
            news: List of news to insert
            allow_update: If True, update existing records (ON CONFLICT UPDATE and the
                Phase 1 UPDATE even when content_hash did not change)

        Returns:
            InsertResult(saved, articles, new). saved includes every row written
            (inserted or updated): the articles_saved of scrape_runs. new counts
            only the rows actually inserted (articles_new): the RETURNING of
            ON CONFLICT DO NOTHING, or the rows with xmax = 0 in the RETURNING
            of ON CONFLICT DO UPDATE (allow_update=True). articles is a list of
            dicts with unique_id, agency_key, published_at for each row to
            publish as dgb.news.scraped: new rows, rows returned by the
            ON CONFLICT DO UPDATE (allow_update=True), plus existing rows
            (matched by URL) whose content_hash changed.
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

        # Deduplicate by (agency_key, url) in-memory. Last occurrence wins.
        # The scraper delivers articles in listing order (newest first), so the
        # last item for a given URL is the oldest in the page — but the final
        # value is overwritten by the Phase-1 UPDATE anyway, making the choice
        # within a single batch inconsequential.
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
        written = 0
        new = 0
        inserted_articles: list[dict] = []

        # Build lookup for metadata (unique_id -> news item)
        news_by_uid = {n.unique_id: n for n in news}

        try:
            cursor = conn.cursor()

            # Two-phase insert: pre-check URLs against existing records
            match = self._match_existing_by_url(news, cursor, allow_update)
            self._log_url_match(match, allow_update)

            # Phase 1: UPDATE existing articles matched by URL. Go back for
            # publication (dgb.news.scraped) only the ones whose content changed.
            updated_articles = self._update_existing_articles(match.to_update, cursor)
            inserted_articles.extend(
                a for a in updated_articles if a["unique_id"] not in match.unchanged_uids
            )

            # Phase 2: INSERT new articles
            if match.to_insert:
                phase2 = self._insert_new_articles(
                    match.to_insert,
                    allow_update,
                    cursor,
                    news_by_uid,
                )
                written = phase2.written
                new = phase2.new
                inserted_articles.extend(phase2.articles)

            conn.commit()

            total = written + len(updated_articles)
            logger.success(f"Inserted {new}, updated {total - new} news records (total={total})")
            return InsertResult(saved=total, articles=inserted_articles, new=new)

        except Exception as e:
            conn.rollback()
            logger.error(f"Error inserting news: {e}")
            raise

        finally:
            cursor.close()
            self.put_connection(conn)

    _INSERT_COLUMNS = [
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

    # Colunas gravadas pelo enriquecimento (enrichment-worker), não pelo scraper.
    # No ON CONFLICT do allow_update=True usam COALESCE: o NULL do scraper não
    # apaga o valor gravado; um valor não nulo enviado explicitamente prevalece.
    _ENRICHED_COLUMNS = frozenset(
        {
            "summary",
            "theme_l1_id",
            "theme_l2_id",
            "theme_l3_id",
            "most_specific_theme_id",
        }
    )

    # Colunas que o scraper às vezes entrega vazias ('' ou NULL) e que um worker
    # downstream preenche: image_url recebe a miniatura do thumbnail-worker em
    # artigos com vídeo e sem imagem (TV Brasil). '' ou NULL não apaga o valor
    # gravado; uma imagem não vazia vinda da fonte prevalece.
    _DOWNSTREAM_FILLED_COLUMNS = frozenset({"image_url"})

    def _insert_new_articles(
        self,
        to_insert: list[NewsInsert],
        allow_update: bool,
        cursor,
        news_by_uid: dict[str, NewsInsert],
    ) -> _Phase2Result:
        """INSERT new articles with SAVEPOINT to handle (agency_key, url) race conditions.

        O RETURNING do ON CONFLICT DO NOTHING só traz as linhas inseridas. O do
        ON CONFLICT DO UPDATE (allow_update=True) traz também as atualizadas; nele
        ``xmax = 0`` separa as inseridas (a linha nova ainda não foi travada nem
        atualizada por nenhuma transação).
        """
        values = [
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
            for n in to_insert
        ]

        # Column names come exclusively from the class-level _INSERT_COLUMNS constant.
        column_names = ", ".join(self._INSERT_COLUMNS)
        insert_query = f"INSERT INTO news ({column_names}) VALUES %s"  # nosec B608

        if allow_update:
            update_cols = [
                c
                for c in self._INSERT_COLUMNS
                if c not in ["unique_id", "agency_id", "published_at"]
            ]
            update_set = ", ".join(self._on_conflict_assignment(c) for c in update_cols)
            # update_cols is derived exclusively from _INSERT_COLUMNS.
            conflict_clause = (
                f" ON CONFLICT (unique_id) DO UPDATE SET {update_set}, updated_at = NOW()"  # nosec B608
            )
            insert_query += conflict_clause
            insert_query += " RETURNING unique_id, (xmax = 0) AS inserted"
        else:
            insert_query += " ON CONFLICT (unique_id) DO NOTHING RETURNING unique_id"

        cursor.execute("SAVEPOINT phase2_insert")
        try:
            result = execute_values(cursor, insert_query, values, fetch=True)
            cursor.execute("RELEASE SAVEPOINT phase2_insert")
        except errors.UniqueViolation:
            cursor.execute("ROLLBACK TO SAVEPOINT phase2_insert")
            logger.warning(
                "UniqueViolation on INSERT (race condition with concurrent worker). "
                "Re-checking URLs and retrying."
            )
            retry_match = self._match_existing_by_url(to_insert, cursor, allow_update)
            self._log_url_match(retry_match, allow_update)
            updated_articles = self._update_existing_articles(retry_match.to_update, cursor)
            retry_insert = retry_match.to_insert
            if retry_insert:
                retry_values = [
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
                    for n in retry_insert
                ]
                result = execute_values(cursor, insert_query, retry_values, fetch=True)
            else:
                result = []
            returned_ids = [row[0] for row in result]
            inserted_articles = [
                a for a in updated_articles if a["unique_id"] not in retry_match.unchanged_uids
            ]
            inserted_articles.extend(self._returned_articles(returned_ids, news_by_uid))
            return _Phase2Result(
                written=len(returned_ids) + len(updated_articles),
                new=self._count_inserted(result, allow_update),
                articles=inserted_articles,
            )

        returned_ids = [row[0] for row in result]
        return _Phase2Result(
            written=len(returned_ids),
            new=self._count_inserted(result, allow_update),
            articles=self._returned_articles(returned_ids, news_by_uid),
        )

    @staticmethod
    def _count_inserted(rows, allow_update: bool) -> int:
        """Linhas de fato inseridas no RETURNING da Phase 2.

        Sem allow_update (DO NOTHING), toda linha devolvida é nova. Com allow_update
        (DO UPDATE), a 2ª coluna é ``xmax = 0``: True só para as inseridas.
        """
        if not allow_update:
            return len(rows)
        return sum(1 for row in rows if row[1])

    @staticmethod
    def _returned_articles(
        returned_ids: list[str], news_by_uid: dict[str, NewsInsert]
    ) -> list[dict]:
        """Metadados (para dgb.news.scraped) das linhas devolvidas pelo RETURNING."""
        articles: list[dict] = []
        for uid in returned_ids:
            n = news_by_uid.get(uid)
            if n:
                articles.append(
                    {
                        "unique_id": uid,
                        "agency_key": n.agency_key or "",
                        "published_at": n.published_at,
                    }
                )
        return articles

    def _on_conflict_assignment(self, column: str) -> str:
        """SET de uma coluna no ON CONFLICT (unique_id) DO UPDATE do allow_update=True."""
        if column in self._ENRICHED_COLUMNS:
            return f"{column} = COALESCE(EXCLUDED.{column}, news.{column})"
        if column in self._DOWNSTREAM_FILLED_COLUMNS:
            return f"{column} = COALESCE(NULLIF(EXCLUDED.{column}, ''), news.{column})"
        return f"{column} = EXCLUDED.{column}"

    def _match_existing_by_url(
        self,
        news: list[NewsInsert],
        cursor,
        allow_update: bool = False,
    ) -> _UrlMatch:
        """Separa os artigos que já existem (casados por (agency_key, url)) dos novos.

        O artigo existente com o mesmo ``content_hash`` do re-scrape fica em
        ``unchanged_uids`` e não é republicado em dgb.news.scraped: o enrichment-worker
        pularia o já enriquecido e o bronze-writer reescreveria o mesmo objeto. Sem
        ``allow_update`` ele também não é atualizado. O hash gravado NULL (linha legada)
        conta como mudança. O artigo ainda sem tema não tem mais tratamento especial:
        o retry do enriquecimento é o reconciliador diário do ``/dispatch`` (Passo 3).
        """
        url_pairs = [(n.agency_key, n.url) for n in news if n.url and n.agency_key]
        existing_by_url = self._find_existing_by_url(url_pairs, cursor)

        to_update: list[tuple[str, NewsInsert]] = []
        to_insert: list[NewsInsert] = []
        unchanged_uids: set[str] = set()
        for n in news:
            key = (n.agency_key, n.url) if n.url and n.agency_key else None
            if key and key in existing_by_url:
                existing = existing_by_url[key]
                if existing.content_hash is not None and existing.content_hash == n.content_hash:
                    unchanged_uids.add(existing.unique_id)
                    if not allow_update:
                        continue  # nada mudou: sem UPDATE, sem contagem, sem republicação
                to_update.append((existing.unique_id, n))
            else:
                to_insert.append(n)
        return _UrlMatch(to_update, to_insert, unchanged_uids)

    @staticmethod
    def _log_url_match(match: _UrlMatch, allow_update: bool) -> None:
        """Loga quantos artigos casados por URL vão ao UPDATE e quantos ficam iguais."""
        if not match.to_update and not match.unchanged_uids:
            return
        if allow_update:
            logger.info(
                f"Updating {len(match.to_update)} existing articles by URL match "
                f"({len(match.unchanged_uids)} with unchanged content, not republished)"
            )
        else:
            logger.info(
                f"Updating {len(match.to_update)} existing articles by URL match with changed "
                f"content; {len(match.unchanged_uids)} unchanged (no UPDATE, not republished)"
            )

    def _find_existing_by_url(
        self,
        url_pairs: list[tuple[str, str]],
        cursor,
    ) -> dict[tuple[str, str], _ExistingArticle]:
        """Mapeia (agency_key, url) -> (unique_id, content_hash)."""
        if not url_pairs:
            return {}

        query = """
            SELECT unique_id, agency_key, url, content_hash FROM news
            WHERE (agency_key, url) IN %s
        """
        cursor.execute(query, (tuple(url_pairs),))
        result: dict[tuple[str, str], _ExistingArticle] = {}
        for row in cursor.fetchall():
            result[(row[1], row[2])] = _ExistingArticle(row[0], row[3])
        return result

    def _update_existing_articles(
        self,
        updates: list[tuple[str, NewsInsert]],
        cursor,
    ) -> list[dict]:
        """Batch-UPDATE existing articles matched by URL.

        Must be called within the caller's transaction (shared cursor).

        Preserva o que o enriquecimento gravou: o scraper nunca preenche summary
        nem tema, então summary usa COALESCE (NULL do re-scrape não apaga o
        resumo) e as colunas de tema e de embedding não são tocadas; image_url
        vazio ou NULL não apaga a miniatura do thumbnail-worker. O embedding
        não é zerado nem quando o conteúdo muda: nada o regeraria (o
        enrichment-worker pula o já enriquecido e o embeddings-api só assina
        dgb.news.enriched), e o vetor é de título + resumo, que é preservado.
        No SET, as referências a `news.*` leem os valores anteriores ao UPDATE.
        """
        if not updates:
            return []

        rows = [
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
                new_data.updated_datetime,
                new_data.extracted_at,
                existing_uid,
            )
            for existing_uid, new_data in updates
        ]

        execute_values(
            cursor,
            """
            UPDATE news SET
                title = v.title, content = v.content, content_hash = v.content_hash,
                summary = COALESCE(v.summary, news.summary),
                image_url = COALESCE(NULLIF(v.image_url, ''), news.image_url),
                video_url = v.video_url,
                category = v.category, tags = v.tags::TEXT[], editorial_lead = v.editorial_lead,
                subtitle = v.subtitle,
                updated_datetime = v.updated_datetime::TIMESTAMPTZ,
                extracted_at = v.extracted_at::TIMESTAMPTZ,
                updated_at = NOW()
            FROM (VALUES %s) AS v(
                title, content, content_hash, summary, image_url, video_url,
                category, tags, editorial_lead, subtitle, updated_datetime,
                extracted_at, unique_id
            )
            WHERE news.unique_id = v.unique_id
            """,
            rows,
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        )

        return [
            {
                "unique_id": existing_uid,
                "agency_key": new_data.agency_key or "",
                "published_at": new_data.published_at,
            }
            for existing_uid, new_data in updates
        ]

    # O aviso de scrape_runs sem articles_new sai uma vez por processo, não por agência.
    _warned_missing_articles_new = False

    def record_scrape_run(self, run: "ScrapeRunResult") -> None:
        """Record a scrape execution result.

        A coluna articles_new vem da migration 037 do data-platform. Se o banco ainda
        não a tem (deploy do scraper antes da migration), o INSERT é refeito sem ela:
        perder a linha de scrape_runs faria o monitor acusar todas as agências.

        Args:
            run: The scrape run result to persist.
        """
        values = {
            "agency_key": run.agency_key,
            "status": run.status,
            "error_category": str(run.error_category) if run.error_category else None,
            "error_message": str(run.error_message)[:500] if run.error_message else None,
            "articles_scraped": run.articles_scraped,
            "articles_saved": run.articles_saved,
            "execution_time_seconds": run.execution_time_seconds,
            "scraped_at": run.scraped_at,
            "fallback_triggered": run.fallback_triggered,
            "articles_new": run.articles_new,
        }
        conn = self.pool.getconn()
        try:
            try:
                self._insert_scrape_run(conn, values)
            except errors.UndefinedColumn as e:
                if "articles_new" not in str(e):
                    raise
                conn.rollback()
                self._warn_missing_articles_new()
                values.pop("articles_new")
                self._insert_scrape_run(conn, values)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error recording scrape run for {run.agency_key}: {e}")
            raise
        finally:
            self.pool.putconn(conn)

    @staticmethod
    def _insert_scrape_run(conn, values: dict[str, Any]) -> None:
        """INSERT de uma linha em scrape_runs com as colunas de ``values``."""
        # Os nomes de coluna são as chaves fixas montadas em record_scrape_run.
        columns = ", ".join(values)
        placeholders = ", ".join(["%s"] * len(values))
        query = f"INSERT INTO scrape_runs ({columns}) VALUES ({placeholders})"  # nosec B608
        with conn.cursor() as cur:
            cur.execute(query, tuple(values.values()))

    @classmethod
    def _warn_missing_articles_new(cls) -> None:
        if cls._warned_missing_articles_new:
            return
        cls._warned_missing_articles_new = True
        logger.warning(
            "scrape_runs sem a coluna articles_new (migration 037 do data-platform não "
            "aplicada): gravando sem ela"
        )

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
                   articles_scraped, articles_saved, execution_time_seconds, scraped_at,
                   fallback_triggered
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
