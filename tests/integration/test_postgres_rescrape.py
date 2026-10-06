"""
Testes de integração do PostgresManager contra um Postgres descartável.

Provam que o re-scrape (Phase 1: artigo existente casado por (agency_key, url))
preserva o que os workers downstream gravaram (summary, temas, embedding e a
miniatura do thumbnail-worker em image_url) e só republica dgb.news.scraped
quando o conteúdo mudou ou quando o artigo ainda não tem tema (a republicação é
o retry de um enriquecimento que falhou).

Requer SCRAPER_TEST_POSTGRES_URL apontando para um Postgres LOCAL e descartável,
com pgvector. Sem a variável, os testes são pulados. Exemplo:

    docker run -d --rm --name scraper-it-pg -e POSTGRES_HOST_AUTH_METHOD=trust \\
        -p 127.0.0.1:55432:5432 pgvector/pgvector:pg16
    SCRAPER_TEST_POSTGRES_URL=postgresql://postgres@127.0.0.1:55432/postgres \\
        PYTHONPATH=src pytest -m postgres --no-cov
    docker rm -f scraper-it-pg

Cada execução cria um schema próprio (scraper_it_<hex>), com o subconjunto do
schema de produção que o scraper usa (data-platform/scripts/create_schema.sql +
migrações 002, 009 e 012), e o remove ao final. Nada fora dele é tocado.
"""

import os
import uuid
from collections import OrderedDict
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor

from govbr_scraper.models.news import NewsInsert
from govbr_scraper.scrapers.content_hash import compute_content_hash
from govbr_scraper.storage.postgres_manager import PostgresManager
from govbr_scraper.storage.storage_adapter import StorageAdapter
from tests.integration import postgres_guard

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"
_EMBEDDING_DIM = 768

_DDL = f"""
CREATE TABLE agencies (
    id SERIAL PRIMARY KEY,
    key VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(500) NOT NULL,
    type VARCHAR(100),
    parent_key VARCHAR(100),
    url VARCHAR(1000),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE themes (
    id SERIAL PRIMARY KEY,
    code VARCHAR(20) UNIQUE NOT NULL,
    label VARCHAR(500) NOT NULL,
    full_name VARCHAR(600),
    level SMALLINT NOT NULL CHECK (level IN (1, 2, 3)),
    parent_code VARCHAR(20) REFERENCES themes(code) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE news (
    id SERIAL PRIMARY KEY,
    unique_id VARCHAR(120) UNIQUE NOT NULL,
    agency_id INTEGER NOT NULL REFERENCES agencies(id),
    theme_l1_id INTEGER REFERENCES themes(id),
    theme_l2_id INTEGER REFERENCES themes(id),
    theme_l3_id INTEGER REFERENCES themes(id),
    most_specific_theme_id INTEGER REFERENCES themes(id),
    title TEXT NOT NULL,
    url TEXT,
    image_url TEXT,
    video_url TEXT,
    category VARCHAR(500),
    tags TEXT[],
    content TEXT,
    editorial_lead TEXT,
    subtitle TEXT,
    summary TEXT,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_datetime TIMESTAMP WITH TIME ZONE,
    extracted_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    agency_key VARCHAR(100),
    agency_name VARCHAR(500),
    legacy_unique_id VARCHAR(32),
    content_embedding vector({_EMBEDDING_DIM}),
    embedding_generated_at TIMESTAMP WITH TIME ZONE,
    content_hash VARCHAR(16)
);

CREATE UNIQUE INDEX idx_news_agency_url_unique ON news (agency_key, url) WHERE url IS NOT NULL;

INSERT INTO agencies (key, name) VALUES ('agencia_brasil', 'Agência Brasil');
INSERT INTO themes (code, label, level) VALUES ('01', 'Economia e Finanças', 1);
INSERT INTO themes (code, label, level, parent_code) VALUES ('01.01', 'Política Econômica', 2, '01');
INSERT INTO themes (code, label, level, parent_code)
    VALUES ('01.01.01', 'Política Fiscal', 3, '01.01');
"""

AGENCY_KEY = "agencia_brasil"
UID = "governo-anuncia-programa_abc123"
URL = "https://agenciabrasil.ebc.com.br/geral/noticia/2026-10/governo-anuncia-programa"
TITLE = "Governo anuncia programa"
CONTENT = "Conteúdo original da notícia sobre o programa."
PUBLISHED_AT = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)
FIRST_EXTRACTED_AT = datetime(2026, 10, 5, 12, 10, tzinfo=UTC)
RESCRAPE_EXTRACTED_AT = datetime(2026, 10, 5, 22, 40, tzinfo=UTC)
SUMMARY = "Resumo gerado pelo enriquecimento."
EMBEDDED_AT = datetime(2026, 10, 5, 13, 0, tzinfo=UTC)
EMBEDDING_LITERAL = "[" + ",".join(["0.125"] * _EMBEDDING_DIM) + "]"
VIDEO_URL = "https://tvbrasil.ebc.com.br/video/governo-anuncia-programa.mp4"
THUMBNAIL_URL = "https://storage.googleapis.com/destaquesgovbr-thumbnails/" + UID + ".jpg"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def base_dsn() -> str:
    dsn = os.getenv(ENV_VAR, "").strip()
    if not dsn:
        pytest.skip(f"{ENV_VAR} não definido: requer um Postgres local descartável")
    reason = postgres_guard.unsafe_dsn_reason(dsn)
    if reason:
        pytest.fail(f"{ENV_VAR} deve apontar para um Postgres LOCAL descartável: {reason}")
    return dsn


@pytest.fixture(scope="module")
def schema_dsn(base_dsn):
    """Cria um schema isolado com o subconjunto do schema de produção; remove ao final."""
    schema = f"scraper_it_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(base_dsn)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            except psycopg2.Error as e:
                pytest.skip(f"pgvector indisponível no Postgres de teste: {e}")
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET search_path TO {schema}, public")
            cur.execute(_DDL)
        yield make_dsn(base_dsn, options=f"-c search_path={schema},public")
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()


@pytest.fixture(scope="module")
def db(schema_dsn):
    """Conexão direta (autocommit) para preparar e inspecionar o estado."""
    conn = psycopg2.connect(schema_dsn)
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def pg(schema_dsn):
    manager = PostgresManager(connection_string=schema_dsn, min_connections=1, max_connections=2)
    manager.load_cache()
    yield manager
    manager.close_all()


@pytest.fixture(autouse=True)
def _clean_news(db):
    with db.cursor() as cur:
        cur.execute("TRUNCATE news RESTART IDENTITY")
    yield


@pytest.fixture
def theme_ids(db) -> dict[str, int]:
    with db.cursor() as cur:
        cur.execute("SELECT code, id FROM themes")
        return dict(cur.fetchall())


class TestDisposableDatabaseGuard:
    """A guarda pós-conexão contra um Postgres real (ver tests/unit/test_postgres_test_guard.py)."""

    def test_accepts_disposable_database_with_test_schema(self, db):
        with db.cursor() as cur:
            assert postgres_guard.unsafe_database_reason(cur) is None

    def test_refuses_database_with_news_table_outside_test_schema(self, base_dsn, schema_dsn):
        conn = psycopg2.connect(base_dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE public.news (id integer)")  # desfeito no rollback
                reason = postgres_guard.unsafe_database_reason(cur)
        finally:
            conn.rollback()
            conn.close()
        assert "news" in reason


# =============================================================================
# Helpers
# =============================================================================


def _scraped(
    pg: PostgresManager,
    *,
    unique_id: str = UID,
    url: str | None = URL,
    title: str = TITLE,
    content: str = CONTENT,
    summary: str | None = None,
    image_url: str | None = "",
    video_url: str | None = None,
    extracted_at: datetime = FIRST_EXTRACTED_AT,
) -> NewsInsert:
    """Artigo como o scraper entrega: sem summary/tema, com content_hash calculado."""
    agency = pg._agencies_by_key[AGENCY_KEY]
    return NewsInsert(
        unique_id=unique_id,
        agency_id=agency.id,
        agency_key=agency.key,
        agency_name=agency.name,
        title=title,
        url=url,
        content=content,
        summary=summary,
        image_url=image_url,
        video_url=video_url,
        content_hash=compute_content_hash(title, content),
        tags=["governo"],
        category="Geral",
        published_at=PUBLISHED_AT,
        updated_datetime=PUBLISHED_AT,
        extracted_at=extracted_at,
    )


def _enrich(db, theme_ids: dict[str, int], unique_id: str = UID) -> None:
    """Simula enrichment-worker (tema + resumo) e embeddings (vetor + timestamp)."""
    with db.cursor() as cur:
        cur.execute(
            """
            UPDATE news SET
                theme_l1_id = %s, theme_l2_id = %s, theme_l3_id = %s,
                most_specific_theme_id = %s, summary = %s,
                content_embedding = %s::vector, embedding_generated_at = %s
            WHERE unique_id = %s
            """,
            (
                theme_ids["01"],
                theme_ids["01.01"],
                theme_ids["01.01.01"],
                theme_ids["01.01.01"],
                SUMMARY,
                EMBEDDING_LITERAL,
                EMBEDDED_AT,
                unique_id,
            ),
        )
        assert cur.rowcount == 1


def _row(db, unique_id: str = UID) -> dict:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT unique_id, url, title, content, content_hash, summary, image_url,
                   theme_l1_id, theme_l2_id, theme_l3_id, most_specific_theme_id,
                   content_embedding IS NOT NULL AS has_embedding,
                   embedding_generated_at, extracted_at
            FROM news WHERE unique_id = %s
            """,
            (unique_id,),
        )
        row = cur.fetchone()
    assert row is not None, f"artigo {unique_id} não encontrado"
    return dict(row)


def _set_thumbnail(db, unique_id: str = UID) -> None:
    """Simula o thumbnail-worker: grava image_url num artigo com vídeo e sem imagem."""
    with db.cursor() as cur:
        cur.execute(
            "UPDATE news SET image_url = %s WHERE unique_id = %s",
            (THUMBNAIL_URL, unique_id),
        )
        assert cur.rowcount == 1


def _count_news(db) -> int:
    with db.cursor() as cur:
        cur.execute("SELECT count(*) FROM news")
        return cur.fetchone()[0]


def _assert_themes(row: dict, theme_ids: dict[str, int]) -> None:
    assert row["theme_l1_id"] == theme_ids["01"]
    assert row["theme_l2_id"] == theme_ids["01.01"]
    assert row["theme_l3_id"] == theme_ids["01.01.01"]
    assert row["most_specific_theme_id"] == theme_ids["01.01.01"]


# =============================================================================
# Phase 1 (re-scrape casado por (agency_key, url))
# =============================================================================


class TestRescrapePreservesEnrichment:
    def test_same_content_preserves_summary_themes_and_embedding(self, pg, db, theme_ids):
        pg.insert([_scraped(pg)])
        _enrich(db, theme_ids)

        # Re-scrape ~10 h depois: mesmo conteúdo, scraper sem summary.
        pg.insert([_scraped(pg, extracted_at=RESCRAPE_EXTRACTED_AT)])

        row = _row(db)
        assert row["extracted_at"] == RESCRAPE_EXTRACTED_AT  # o UPDATE da Phase 1 rodou
        assert row["summary"] == SUMMARY
        _assert_themes(row, theme_ids)
        assert row["has_embedding"] is True
        assert row["embedding_generated_at"] == EMBEDDED_AT
        assert _count_news(db) == 1

    def test_changed_content_preserves_summary_and_embedding(self, pg, db, theme_ids):
        """Edição de conteúdo não zera o embedding: nada o regeraria (o enrichment-worker
        pula o já enriquecido e o embeddings-api só assina dgb.news.enriched)."""
        pg.insert([_scraped(pg)])
        _enrich(db, theme_ids)

        new_content = "Conteúdo corrigido pela agência após a publicação."
        pg.insert([_scraped(pg, content=new_content, extracted_at=RESCRAPE_EXTRACTED_AT)])

        row = _row(db)
        assert row["content"] == new_content
        assert row["content_hash"] == compute_content_hash(TITLE, new_content)
        assert row["has_embedding"] is True
        assert row["embedding_generated_at"] == EMBEDDED_AT
        assert row["summary"] == SUMMARY
        _assert_themes(row, theme_ids)

    def test_title_edit_with_new_unique_id_updates_existing_row(self, pg, db, theme_ids):
        """Título editado gera outro unique_id; o casamento por URL atualiza a linha original."""
        pg.insert([_scraped(pg)])
        _enrich(db, theme_ids)

        new_title = "Governo anuncia novo programa"
        pg.insert(
            [
                _scraped(
                    pg,
                    unique_id="governo-anuncia-novo-programa_def456",
                    title=new_title,
                    extracted_at=RESCRAPE_EXTRACTED_AT,
                )
            ]
        )

        assert _count_news(db) == 1
        row = _row(db)
        assert row["title"] == new_title
        assert row["has_embedding"] is True  # vetor um pouco desatualizado > nenhum
        assert row["summary"] == SUMMARY
        _assert_themes(row, theme_ids)

    @pytest.mark.parametrize("rescrape_image", ["", None], ids=["vazia", "nula"])
    def test_rescrape_without_image_preserves_thumbnail(self, pg, db, rescrape_image):
        """TV Brasil: o scraper entrega image '' para vídeo sem imagem e o
        thumbnail-worker grava a miniatura; o re-scrape não pode apagá-la."""
        pg.insert([_scraped(pg, video_url=VIDEO_URL)])
        _set_thumbnail(db)

        pg.insert(
            [
                _scraped(
                    pg,
                    image_url=rescrape_image,
                    video_url=VIDEO_URL,
                    extracted_at=RESCRAPE_EXTRACTED_AT,
                )
            ]
        )

        row = _row(db)
        assert row["extracted_at"] == RESCRAPE_EXTRACTED_AT
        assert row["image_url"] == THUMBNAIL_URL

    def test_rescrape_with_image_overwrites_image_url(self, pg, db):
        """Uma imagem não vazia vinda da fonte continua prevalecendo."""
        pg.insert([_scraped(pg, video_url=VIDEO_URL)])
        _set_thumbnail(db)

        source_image = "https://imagens.ebc.com.br/governo-anuncia-programa.jpg"
        pg.insert([_scraped(pg, image_url=source_image, extracted_at=RESCRAPE_EXTRACTED_AT)])

        assert _row(db)["image_url"] == source_image

    def test_summary_provided_by_rescrape_wins(self, pg, db, theme_ids):
        """COALESCE só protege contra NULL: um summary vindo do re-scrape prevalece."""
        pg.insert([_scraped(pg)])
        _enrich(db, theme_ids)

        pg.insert([_scraped(pg, summary="Resumo vindo da fonte.")])

        assert _row(db)["summary"] == "Resumo vindo da fonte."


# =============================================================================
# Phase 2 com allow_update=True (ON CONFLICT (unique_id) DO UPDATE)
# =============================================================================


class TestAllowUpdatePreservesEnrichment:
    """O ON CONFLICT (unique_id) só é alcançado quando o (agency_key, url) não casa
    na Phase 1: URL alterada na fonte ou artigo sem URL."""

    @pytest.mark.parametrize(
        "new_url",
        [f"{URL}?origem=defeso", None],
        ids=["url-alterada", "sem-url"],
    )
    def test_on_conflict_does_not_overwrite_summary_or_themes(self, pg, db, theme_ids, new_url):
        pg.insert([_scraped(pg)])
        _enrich(db, theme_ids)

        pg.insert(
            [_scraped(pg, url=new_url, extracted_at=RESCRAPE_EXTRACTED_AT)],
            allow_update=True,
        )

        assert _count_news(db) == 1
        row = _row(db)
        assert row["url"] == new_url  # o DO UPDATE rodou
        assert row["extracted_at"] == RESCRAPE_EXTRACTED_AT
        assert row["summary"] == SUMMARY
        _assert_themes(row, theme_ids)

    def test_on_conflict_does_not_erase_thumbnail(self, pg, db):
        pg.insert([_scraped(pg, video_url=VIDEO_URL)])
        _set_thumbnail(db)

        pg.insert(
            [
                _scraped(
                    pg,
                    url=f"{URL}?origem=defeso",
                    video_url=VIDEO_URL,
                    extracted_at=RESCRAPE_EXTRACTED_AT,
                )
            ],
            allow_update=True,
        )

        row = _row(db)
        assert row["url"] == f"{URL}?origem=defeso"
        assert row["image_url"] == THUMBNAIL_URL


# =============================================================================
# Republicação de dgb.news.scraped (StorageAdapter → EventPublisher)
# =============================================================================


def _scraped_batch(**overrides) -> OrderedDict:
    """Lote no formato colunar que os scrape managers entregam ao StorageAdapter."""
    item = {
        "unique_id": UID,
        "agency": AGENCY_KEY,
        "published_at": PUBLISHED_AT,
        "updated_datetime": PUBLISHED_AT,
        "title": TITLE,
        "content": CONTENT,
        "url": URL,
        "tags": ["governo"],
        "category": "Geral",
        "extracted_at": FIRST_EXTRACTED_AT,
    }
    item.update(overrides)
    item["content_hash"] = compute_content_hash(item["title"], item["content"])
    return OrderedDict((key, [value]) for key, value in item.items())


def _published_uids(publisher: MagicMock) -> list[str]:
    return [
        article["unique_id"]
        for call in publisher.publish_scraped.call_args_list
        for article in call.args[0]
    ]


class TestRepublishOnlyOnContentChange:
    @pytest.fixture
    def publisher(self) -> MagicMock:
        publisher = MagicMock()
        publisher.enabled = True
        return publisher

    @pytest.fixture
    def adapter(self, pg, publisher) -> StorageAdapter:
        with patch(
            "govbr_scraper.storage.storage_adapter.EventPublisher",
            return_value=publisher,
        ):
            return StorageAdapter(postgres_manager=pg)

    def test_new_article_is_published(self, adapter, publisher):
        assert adapter.insert(_scraped_batch()) == 1
        assert _published_uids(publisher) == [UID]

    def test_rescrape_without_content_change_is_not_republished(
        self, adapter, publisher, db, theme_ids
    ):
        adapter.insert(_scraped_batch())
        _enrich(db, theme_ids)
        publisher.reset_mock()

        saved = adapter.insert(_scraped_batch(extracted_at=RESCRAPE_EXTRACTED_AT))

        assert saved == 1  # a Phase 1 atualizou a linha (articles_saved inalterado)
        assert _row(db)["extracted_at"] == RESCRAPE_EXTRACTED_AT
        publisher.publish_scraped.assert_not_called()

    def test_rescrape_without_content_change_but_not_enriched_is_republished(
        self, adapter, publisher, db
    ):
        """Enriquecimento falhou (sem tema gravado): o enrichment-worker faz ACK sem
        retry, então a republicação do re-scrape é a nova tentativa."""
        adapter.insert(_scraped_batch())
        publisher.reset_mock()

        saved = adapter.insert(_scraped_batch(extracted_at=RESCRAPE_EXTRACTED_AT))

        assert saved == 1
        assert _row(db)["most_specific_theme_id"] is None
        assert _published_uids(publisher) == [UID]

    def test_rescrape_with_content_change_is_republished(self, adapter, publisher, db, theme_ids):
        adapter.insert(_scraped_batch())
        _enrich(db, theme_ids)
        publisher.reset_mock()

        adapter.insert(
            _scraped_batch(
                content="Conteúdo corrigido pela agência.",
                extracted_at=RESCRAPE_EXTRACTED_AT,
            )
        )

        assert _published_uids(publisher) == [UID]
        row = _row(db)
        assert row["summary"] == SUMMARY
        assert row["has_embedding"] is True
