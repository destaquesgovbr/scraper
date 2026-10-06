"""
Unit tests for PostgresManager._update_existing_articles()
"""

from datetime import UTC, datetime
from unittest.mock import patch

import pytest

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
                updated_datetime=datetime(2026, 6, 2, 12, 0, tzinfo=UTC),
                extracted_at=datetime(2026, 6, 2, 12, 5, tzinfo=UTC),
                published_at=datetime(2026, 6, 1, 10, 0, tzinfo=UTC),
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


def _normalized_sql(mock_exec) -> str:
    """SQL passado ao execute_values, com espaços colapsados (comparação estável)."""
    return " ".join(mock_exec.call_args[0][1].split())


def test_update_preserves_existing_summary_when_rescrape_has_none(
    pg_manager, mock_pool, sample_update
):
    """Re-scrape não traz summary (o scraper nunca o preenche): o UPDATE não pode
    gravar NULL por cima do resumo gerado pelo enriquecimento."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        pg_manager._update_existing_articles(sample_update, mock_cursor)

    sql = _normalized_sql(mock_exec)
    assert "summary = COALESCE(v.summary, news.summary)" in sql
    assert "summary = v.summary" not in sql


def test_update_does_not_touch_embedding_columns(pg_manager, mock_pool, sample_update):
    """content_embedding/embedding_generated_at são do embeddings-api: o re-scrape não
    os toca, nem com o conteúdo alterado. Zerar sem um caminho de regeneração tiraria
    o artigo da busca semântica para sempre (o vetor é de título + resumo, e o resumo
    é preservado)."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        pg_manager._update_existing_articles(sample_update, mock_cursor)

    sql = _normalized_sql(mock_exec)
    assert "content_embedding" not in sql
    assert "embedding_generated_at" not in sql


def test_update_does_not_touch_theme_columns(pg_manager, mock_pool, sample_update):
    """Temas são gravados pelo enriquecimento; o re-scrape não os toca."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        pg_manager._update_existing_articles(sample_update, mock_cursor)

    sql = _normalized_sql(mock_exec)
    for col in ("theme_l1_id", "theme_l2_id", "theme_l3_id", "most_specific_theme_id"):
        assert f"{col} =" not in sql


def test_update_preserves_image_url_when_rescrape_has_none_or_empty(
    pg_manager, mock_pool, sample_update
):
    """O thumbnail-worker grava image_url em artigos com vídeo e sem imagem (TV Brasil).
    O re-scrape traz '' ou NULL nesses casos e não pode apagar o thumbnail; uma
    imagem não vazia vinda da fonte continua prevalecendo."""
    _, _, mock_cursor = mock_pool

    with patch("govbr_scraper.storage.postgres_manager.execute_values") as mock_exec:
        pg_manager._update_existing_articles(sample_update, mock_cursor)

    sql = _normalized_sql(mock_exec)
    assert "image_url = COALESCE(NULLIF(v.image_url, ''), news.image_url)" in sql
    assert "image_url = v.image_url" not in sql
