from datetime import UTC, datetime
from unittest.mock import patch

from psycopg2 import errors

from govbr_scraper.models.news import NewsInsert

# mock_pool and pg_manager fixtures provided by tests/unit/conftest.py

# content_hash já gravado no banco (4ª coluna do SELECT de _find_existing_by_url).
# Os artigos de _make_news não têm content_hash, então por padrão o conteúdo "mudou".
STORED_HASH = "hash-gravado-000"
# most_specific_theme_id gravado (5ª coluna): artigo já enriquecido. None = sem tema.
STORED_THEME_ID = 42


def _make_news(
    unique_id,
    agency_key="ebc",
    url="https://example.com/article",
    title="Titulo",
    content="Conteudo",
):
    return NewsInsert(
        unique_id=unique_id,
        agency_id=1,
        agency_key=agency_key,
        agency_name="EBC",
        title=title,
        url=url,
        content=content,
        published_at=datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    )


class TestUrlBasedDedup:
    def test_same_url_same_agency_updates_existing(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            (
                "existing-uid-123",
                "ebc",
                "https://example.com/article",
                STORED_HASH,
                STORED_THEME_ID,
            ),
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
            ("original-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
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
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]

        news = [
            _make_news(
                "new-uid",
                title="Novo titulo",
                content="Novo conteudo",
            )
        ]
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
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]

        updated_dt = datetime(2026, 1, 2, 10, 0, tzinfo=UTC)
        extracted_dt = datetime(2026, 1, 2, 12, 0, tzinfo=UTC)
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

    def test_update_invalidates_embedding_only_on_content_change(self, pg_manager, mock_pool):
        """O embedding só é invalidado quando o content_hash muda (antes: sempre)."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]

        news = [_make_news("new-uid", title="Titulo editado")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            pg_manager.insert(news)

        sql = " ".join(mock_exec.call_args[0][1].split())
        assert "content_embedding = NULL" not in sql
        assert "embedding_generated_at = NULL" not in sql
        assert "news.content_hash IS DISTINCT FROM v.content_hash THEN NULL" in sql

    def test_mixed_batch_new_and_existing(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/existing", STORED_HASH, STORED_THEME_ID),
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
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]

        news = [_make_news("new-uid")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles = pg_manager.insert(news)

        assert len(articles) == 1
        assert articles[0]["unique_id"] == "existing-uid"
        assert articles[0]["agency_key"] == "ebc"
        assert articles[0]["published_at"] == datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

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
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID)],
        ]

        news = [_make_news("new-uid")]
        violation = errors.UniqueViolation()

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[violation, None],
        ):
            count, articles = pg_manager.insert(news)

        savepoint_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if isinstance(c[0][0], str) and "SAVEPOINT" in c[0][0]
        ]
        assert any("ROLLBACK TO SAVEPOINT" in c[0][0] for c in savepoint_calls)
        assert count == 1
        assert articles[0]["unique_id"] == "race-uid"


class TestRepublishOnlyChangedContent:
    """Phase 1 continua atualizando o artigo existente, mas só devolve para
    publicação (dgb.news.scraped) os que tiveram o conteúdo alterado ou que
    ainda não foram enriquecidos (sem tema: a republicação é o retry)."""

    def test_find_existing_by_url_selects_content_hash_and_theme(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]

        result = pg_manager._find_existing_by_url(
            [("ebc", "https://example.com/article")], mock_cursor
        )

        select_sql = " ".join(mock_cursor.execute.call_args[0][0].split())
        assert (
            "SELECT unique_id, agency_key, url, content_hash, most_specific_theme_id FROM news"
            in select_sql
        )
        assert result == {
            ("ebc", "https://example.com/article"): ("existing-uid", STORED_HASH, STORED_THEME_ID)
        }

    def test_unchanged_content_is_updated_but_not_returned(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        assert "UPDATE news SET" in mock_exec.call_args[0][1]  # a Phase 1 rodou
        assert count == 1  # articles_saved não muda de semântica
        assert articles == []

    def test_unchanged_content_without_theme_is_returned(self, pg_manager, mock_pool):
        """Artigo ainda sem tema (enriquecimento falhou: throttling, timeout, modelo
        fora): o enrichment-worker não tem retry próprio e faz ACK mesmo em erro,
        então a republicação do re-scrape é a única nova tentativa."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, None),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles = pg_manager.insert(news)

        assert "UPDATE news SET" in mock_exec.call_args[0][1]
        assert count == 1
        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_changed_content_is_returned(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID),
        ]
        news = [_make_news("existing-uid", content="Conteudo corrigido")]
        news[0].content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles = pg_manager.insert(news)

        assert count == 1
        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_stored_hash_null_counts_as_changed(self, pg_manager, mock_pool):
        """Linha legada sem content_hash: o re-scrape que traz o hash é mudança."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", None, STORED_THEME_ID),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            _, articles = pg_manager.insert(news)

        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_mixed_batch_returns_new_changed_and_not_enriched(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("uid-igual", "ebc", "https://example.com/igual", "hash-igual-00000", STORED_THEME_ID),
            ("uid-sem-tema", "ebc", "https://example.com/sem-tema", "hash-sem-tema-00", None),
            ("uid-mudou", "ebc", "https://example.com/mudou", "hash-antigo-0000", STORED_THEME_ID),
        ]
        igual = _make_news("uid-igual", url="https://example.com/igual")
        igual.content_hash = "hash-igual-00000"
        sem_tema = _make_news("uid-sem-tema", url="https://example.com/sem-tema")
        sem_tema.content_hash = "hash-sem-tema-00"
        mudou = _make_news("uid-mudou", url="https://example.com/mudou")
        mudou.content_hash = "hash-novo-000000"
        nova = _make_news("uid-nova", url="https://example.com/nova")

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[None, [("uid-nova",)]],
        ):
            count, articles = pg_manager.insert([igual, sem_tema, mudou, nova])

        assert count == 4
        assert sorted(a["unique_id"] for a in articles) == [
            "uid-mudou",
            "uid-nova",
            "uid-sem-tema",
        ]

    def test_race_retry_does_not_return_unchanged_content(self, pg_manager, mock_pool):
        """No retry do UniqueViolation (worker concorrente inseriu antes), o artigo
        casado por URL com o mesmo conteúdo também não é republicado."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH, STORED_THEME_ID)],
        ]
        news = [_make_news("new-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation(), None],
        ):
            count, articles = pg_manager.insert(news)

        assert count == 1
        assert articles == []

    def test_race_retry_returns_unchanged_content_without_theme(self, pg_manager, mock_pool):
        """No retry do UniqueViolation, o artigo casado por URL ainda sem tema é
        republicado mesmo com o conteúdo igual (mesma regra da Phase 1)."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH, None)],
        ]
        news = [_make_news("new-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation(), None],
        ):
            count, articles = pg_manager.insert(news)

        assert count == 1
        assert [a["unique_id"] for a in articles] == ["race-uid"]
