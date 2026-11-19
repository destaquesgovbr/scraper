"""
EBC de ponta a ponta contra um Postgres descartável: EBCScrapeManager + StorageAdapter +
PostgresManager reais, com o HTTP simulado.

Provam, no banco, as correções de custo do Passo 2 da rearquitetura da ingestão
(_plan/rearquitetura-ingestao.md) para a EBC:

- falha da primeira página do índice grava scrape_runs com status ``error`` e a
  categoria (``network_error`` ou ``html_changed``), em vez de ``success`` com 0 artigos;
- a cerca de URLs conhecidas usa ``get_recent_urls`` com a chave da fonte e para sem
  baixar os artigos;
- re-scrape sem mudança de conteúdo (backfill, cerca desligada) não grava nem conta em
  articles_saved; com o conteúdo alterado, atualiza a linha.

E, pela rota POST /scrape/ebc (API real, com o schema na DATABASE_URL), que a falha da
primeira página chega ao estado por agência (scrape_agency_state, migration 038) e ao evento
scrape_attempt: html_changed duas vezes abre o disjuntor e a fonte passa a ser pulada;
network_error é passageiro; o sucesso move a marca d'água e a janela explícita não mexe no
estado.

Mesma guarda e mesmo Postgres dos outros testes `postgres` (ver
test_postgres_rescrape.py): requer SCRAPER_TEST_POSTGRES_URL apontando para um
Postgres LOCAL e descartável, numa porta diferente de 5432. O schema
(scraper_it_<hex>) é removido ao final.
"""

import os
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg2
import pytest
import requests
from fastapi.testclient import TestClient
from psycopg2.extensions import make_dsn
from psycopg2.extras import RealDictCursor

from govbr_scraper.api import app
from govbr_scraper.models.news import NewsInsert
from govbr_scraper.monitoring import structured_log
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
from govbr_scraper.scrapers.ebc_webscraper import today_brt
from govbr_scraper.storage.postgres_manager import PostgresManager
from govbr_scraper.storage.storage_adapter import StorageAdapter
from tests.integration import postgres_guard
from tests.integration.migration_038 import MIGRATION_038

pytestmark = [pytest.mark.integration, pytest.mark.postgres]

ENV_VAR = "SCRAPER_TEST_POSTGRES_URL"

# Subconjunto do schema de produção (data-platform/scripts/create_schema.sql + migrações
# 008, 009, 012, 026 e 037) que o caminho EBC usa, mais a 038 (MIGRATION_038). Sem
# pgvector: o scraper não lê nem grava o embedding.
_DDL = """
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
    content_hash VARCHAR(16)
);

CREATE UNIQUE INDEX idx_news_agency_url_unique ON news (agency_key, url) WHERE url IS NOT NULL;

CREATE TABLE scrape_runs (
    id SERIAL PRIMARY KEY,
    agency_key VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'error')),
    error_category VARCHAR(50),
    error_message TEXT,
    articles_scraped INTEGER DEFAULT 0,
    articles_saved INTEGER DEFAULT 0,
    execution_time_seconds REAL,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fallback_triggered BOOLEAN DEFAULT FALSE,
    articles_new INTEGER
);

INSERT INTO agencies (key, name) VALUES ('agencia_brasil', 'Agência Brasil'), ('tvbrasil', 'TV Brasil');
"""

INDEX_URL = "https://agenciabrasil.ebc.com.br/ultimas"
BACKFILL_DAY = "2026-11-10"


def _article_url(slug: str) -> str:
    return f"https://agenciabrasil.ebc.com.br/geral/noticia/2026-11/{slug}"


def _index_html(urls: list[str]) -> bytes:
    links = "".join(f'<a class="capa-noticia" href="{u}">x</a>' for u in urls)
    return f"<html><body>{links}</body></html>".encode()


def _article_html(title: str, content: str, date_text: str = "10/11/2026 - 10:00") -> bytes:
    return f"""
    <html><body>
      <h1 class="titulo-materia">{title}</h1>
      <div class="data">Publicado em {date_text}</div>
      <div class="conteudo-noticia"><p>{content}</p></div>
    </body></html>
    """.encode()


def _response(status: int, content: bytes = b"", url: str = INDEX_URL) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.url = url
    response._content = content
    return response


class _FakeSite:
    """requests.get simulado: índice na página 0, páginas seguintes vazias e artigos."""

    def __init__(self, index: requests.Response, articles: dict[str, bytes] | None = None):
        self.index = index
        self.articles = articles or {}
        self.calls: list[str] = []

    def get(self, url, *args, **kwargs):
        self.calls.append(url)
        if url == f"{INDEX_URL}?page=0":
            return self.index
        if url.startswith(f"{INDEX_URL}?page="):
            return _response(200, b"<html><body></body></html>", url)
        if url in self.articles:
            return _response(200, self.articles[url], url)
        return _response(404, b"", url)


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
    schema = f"{postgres_guard.TEST_SCHEMA_PREFIX}{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(base_dsn)
    admin.autocommit = True
    with admin.cursor() as cur:
        reason = postgres_guard.unsafe_database_reason(cur)
    if reason:
        admin.close()
        pytest.fail(f"{ENV_VAR} não aponta para um Postgres descartável: {reason}")
    try:
        with admin.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(f"SET search_path TO {schema}")
            cur.execute(_DDL)
            cur.execute(MIGRATION_038)
        yield make_dsn(base_dsn, options=f"-c search_path={schema}")
    finally:
        with admin.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        admin.close()


@pytest.fixture(scope="module")
def db(schema_dsn):
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
def _clean(db):
    with db.cursor() as cur:
        cur.execute("TRUNCATE news, scrape_runs, scrape_agency_state RESTART IDENTITY")
    yield


@pytest.fixture(autouse=True)
def _no_sleep_no_pubsub(monkeypatch):
    monkeypatch.delenv("PUBSUB_TOPIC_NEWS_SCRAPED", raising=False)
    monkeypatch.delenv("PUBSUB_TOPIC_OPS_ALERTS", raising=False)
    with patch("govbr_scraper.scrapers.ebc_webscraper.time.sleep"):
        yield


def _run(pg, site: _FakeSite, min_date: str, max_date: str) -> dict:
    with patch("requests.get", side_effect=site.get):
        return EBCScrapeManager(StorageAdapter(postgres_manager=pg)).run_scraper(
            min_date=min_date,
            max_date=max_date,
            sequential=True,
            allow_update=False,
            agencies=["agencia_brasil"],
        )


def _runs(db) -> list[dict]:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM scrape_runs ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


def _state(db, agency_key: str = "agencia_brasil") -> dict | None:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM scrape_agency_state WHERE agency_key = %s", (agency_key,))
        row = cur.fetchone()
        return dict(row) if row is not None else None


def _news(db) -> list[dict]:
    with db.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT unique_id, url, content, extracted_at FROM news ORDER BY id")
        return [dict(r) for r in cur.fetchall()]


# =============================================================================
# Primeira página do índice
# =============================================================================


class TestListingFailureIsRecorded:
    @pytest.mark.parametrize(
        ("index", "category", "retryable"),
        [
            (_response(503), "network_error", True),
            (
                _response(200, b"<html><body><div>layout novo</div></body></html>"),
                "html_changed",
                False,
            ),
        ],
        ids=["503", "sem-links"],
    )
    def test_first_page_failure_records_error_row(self, pg, db, index, category, retryable):
        with patch("retry.api.time.sleep"):
            result = _run(pg, _FakeSite(index), BACKFILL_DAY, BACKFILL_DAY)

        (error,) = result["errors"]
        assert (error["category"], error["retryable"]) == (category, retryable)
        (run,) = _runs(db)
        assert run["agency_key"] == "agencia_brasil"
        assert run["status"] == "error"
        assert run["error_category"] == category
        assert run["articles_new"] is None
        assert _news(db) == []
        # O estado por agência registra a falha (sem janela explícita no StorageAdapter).
        state = _state(db)
        assert state["last_error_category"] == category
        assert state["last_success_at"] is None
        counter = (
            "consecutive_transient_failures" if retryable else "consecutive_permanent_failures"
        )
        assert state[counter] == 1


# =============================================================================
# Cerca de URLs conhecidas
# =============================================================================


def _known_article(pg, slug: str) -> NewsInsert:
    agency = pg._agencies_by_key["agencia_brasil"]
    return NewsInsert(
        unique_id=f"{slug}_000000",
        agency_id=agency.id,
        agency_key=agency.key,
        agency_name=agency.name,
        title=slug,
        url=_article_url(slug),
        content="Conteúdo já gravado.",
        published_at=datetime(2026, 11, 19, 9, 0, tzinfo=UTC),
    )


class TestKnownUrlFence:
    def test_known_urls_stop_the_source_without_fetching_articles(self, pg, db):
        pg.insert([_known_article(pg, f"antiga-{i}") for i in (1, 2, 3)])
        listing = [_article_url(s) for s in ("antiga-1", "antiga-2", "antiga-3", "outra")]
        site = _FakeSite(_response(200, _index_html(listing)))
        today = today_brt().isoformat()

        result = _run(pg, site, today, today)

        assert site.calls == [f"{INDEX_URL}?page=0"]  # nenhum artigo nem página 1
        assert result["errors"] == []
        (run,) = _runs(db)
        assert run["status"] == "success"
        assert (run["articles_scraped"], run["articles_saved"], run["articles_new"]) == (0, 0, 0)


# =============================================================================
# Re-scrape (backfill: cerca desligada) com e sem mudança de conteúdo
# =============================================================================


class TestRescrapeCounts:
    URL = _article_url("governo-anuncia-programa")
    TITLE = "Governo anuncia programa de teste"
    CONTENT = "Conteúdo original da notícia sobre o programa de teste."

    def _site(self, content: str) -> _FakeSite:
        return _FakeSite(
            _response(200, _index_html([self.URL])),
            {self.URL: _article_html(self.TITLE, content)},
        )

    def test_unchanged_rescrape_is_not_saved_and_changed_is_updated(self, pg, db):
        first = _run(pg, self._site(self.CONTENT), BACKFILL_DAY, BACKFILL_DAY)
        assert (first["articles_saved"], first["articles_new"]) == (1, 1)
        (stored,) = _news(db)

        # Backfill de novo, mesmo conteúdo: o artigo é baixado (cerca desligada), mas
        # não há UPDATE nem contagem.
        again = _run(pg, self._site(self.CONTENT), BACKFILL_DAY, BACKFILL_DAY)
        assert (again["articles_scraped"], again["articles_saved"], again["articles_new"]) == (
            1,
            0,
            0,
        )
        assert _news(db) == [stored]

        changed = "Conteúdo corrigido pela agência depois da publicação."
        edited = _run(pg, self._site(changed), BACKFILL_DAY, BACKFILL_DAY)
        assert (edited["articles_saved"], edited["articles_new"]) == (1, 0)
        (row,) = _news(db)
        assert row["content"] == changed

        assert [(r["articles_saved"], r["articles_new"]) for r in _runs(db)] == [
            (1, 1),
            (0, 0),
            (1, 0),
        ]


# =============================================================================
# POST /scrape/ebc: falha da 1ª página no estado por agência e no scrape_attempt
# =============================================================================

_CHANGED_LAYOUT = b"<html><body><div>layout novo</div></body></html>"


@pytest.fixture
def api(schema_dsn, monkeypatch):
    """Cliente da API real com o schema de teste na DATABASE_URL, o scrape_attempt
    espionado e o alerta do disjuntor interceptado."""
    monkeypatch.setenv("DATABASE_URL", schema_dsn)
    monkeypatch.setattr(PostgresManager, "_warned_missing_agency_state", False)
    log = MagicMock(wraps=structured_log.log_scrape_attempt)
    with (
        patch("govbr_scraper.api.log_scrape_attempt", log),
        patch("govbr_scraper.state.alerts.publish_ops_alert", return_value=True) as publish,
        patch("retry.api.time.sleep"),
    ):
        yield SimpleNamespace(client=TestClient(app), log=log, publish=publish)


def _post(api, site: _FakeSite, **payload):
    body = {"agencies": ["agencia_brasil"], "sequential": True, **payload}
    with patch("requests.get", side_effect=site.get):
        return api.client.post("/scrape/ebc", json=body)


def _today_article_html(title: str, content: str) -> bytes:
    return _article_html(title, content, today_brt().strftime("%d/%m/%Y") + " - 00:05")


class TestApiEbcAgencyState:
    URL = _article_url("noticia-de-hoje")

    def test_html_changed_twice_opens_the_circuit_and_then_skips(self, api, db):
        site = _FakeSite(_response(200, _CHANGED_LAYOUT))
        today = today_brt()

        first = _post(api, site)
        assert first.status_code == 500
        body = first.json()
        (error,) = body["errors"]
        assert (error["agency"], error["category"], error["retryable"]) == (
            "agencia_brasil",
            "html_changed",
            False,
        )
        # Sem histórico: janela de ontem a hoje (marca d'água).
        assert (body["start_date"], body["end_date"]) == (
            (today - timedelta(days=1)).isoformat(),
            today.isoformat(),
        )
        state = _state(db)
        assert state["consecutive_permanent_failures"] == 1
        assert state["circuit_state"] == "closed"
        assert state["last_error_category"] == "html_changed"
        assert state["last_success_at"] is None
        attempt = api.log.call_args.kwargs
        assert (attempt["agency"], attempt["outcome"], attempt["category"]) == (
            "agencia_brasil",
            "failed",
            "html_changed",
        )
        assert attempt["retryable"] is False
        assert attempt["explicit_window"] is False
        assert attempt["tier"] == "h2"

        second = _post(api, site)
        assert second.status_code == 500
        state = _state(db)
        assert state["circuit_state"] == "open"
        assert state["consecutive_permanent_failures"] == 2
        assert state["next_due_at"] == state["circuit_opened_at"] + timedelta(hours=2)
        (alert,) = api.publish.call_args_list
        assert alert.kwargs["text"].startswith("🔴 Disjuntor aberto: agencia_brasil")

        calls = len(site.calls)
        third = _post(api, site)
        assert third.status_code == 200
        assert third.json()["status"] == "skipped"
        (skipped,) = third.json()["skipped"]
        assert (skipped["agency"], skipped["reason"]) == ("agencia_brasil", "circuit_open")
        assert skipped["next_probe_at"] is not None
        assert len(site.calls) == calls  # não raspou
        assert [r["status"] for r in _runs(db)] == ["error", "error"]  # nem gravou scrape_runs
        assert api.log.call_args.kwargs["outcome"] == "skipped"

    def test_network_error_is_transient(self, api, db):
        response = _post(api, _FakeSite(_response(503)))

        assert response.status_code == 500
        (error,) = response.json()["errors"]
        assert (error["category"], error["retryable"]) == ("network_error", True)
        state = _state(db)
        assert state["consecutive_transient_failures"] == 1
        assert state["consecutive_permanent_failures"] == 0
        assert state["failure_streak_started_at"] is not None
        assert state["circuit_state"] == "closed"
        attempt = api.log.call_args.kwargs
        assert (attempt["outcome"], attempt["category"], attempt["retryable"]) == (
            "failed",
            "network_error",
            True,
        )

    def test_success_moves_the_watermark_and_failure_keeps_it(self, api, db):
        site = _FakeSite(
            _response(200, _index_html([self.URL])),
            {self.URL: _today_article_html("Notícia de hoje", "Conteúdo da notícia de hoje.")},
        )

        first = _post(api, site)
        assert first.status_code == 200
        assert (first.json()["articles_saved"], first.json()["articles_new"]) == (1, 1)
        state = _state(db)
        watermark = state["last_success_at"]
        assert watermark is not None
        assert state["boost_until"] == state["last_new_article_at"] + timedelta(hours=6)
        assert api.log.call_args.kwargs["articles_new"] == 1

        # De novo, sem mudança: a cerca para no artigo conhecido (sem baixá-lo) e nada é
        # gravado nem contado; a marca d'água avança.
        site.calls.clear()
        again = _post(api, site)
        assert again.status_code == 200
        assert (again.json()["articles_saved"], again.json()["articles_new"]) == (0, 0)
        assert self.URL not in site.calls
        state = _state(db)
        assert state["last_success_at"] > watermark
        watermark = state["last_success_at"]

        # Falha da 1ª página: a marca d'água fica, e a próxima janela começa nela.
        failed = _post(api, _FakeSite(_response(503)))
        assert failed.status_code == 500
        state = _state(db)
        assert state["last_success_at"] == watermark
        assert state["consecutive_transient_failures"] == 1
        assert [(r["status"], r["articles_saved"], r["articles_new"]) for r in _runs(db)] == [
            ("success", 1, 1),
            ("success", 0, 0),
            ("error", 0, None),
        ]

    def test_explicit_window_failure_does_not_touch_the_state(self, api, db):
        response = _post(
            api,
            _FakeSite(_response(200, _CHANGED_LAYOUT)),
            start_date=BACKFILL_DAY,
            end_date=BACKFILL_DAY,
        )

        assert response.status_code == 500
        assert response.json()["errors"][0]["category"] == "html_changed"
        (run,) = _runs(db)
        assert (run["status"], run["error_category"]) == ("error", "html_changed")
        state = _state(db)
        assert state["consecutive_permanent_failures"] == 0
        assert state["last_error_category"] is None
        assert api.log.call_args.kwargs["explicit_window"] is True
