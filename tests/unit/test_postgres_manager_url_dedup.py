from datetime import UTC, datetime
from unittest.mock import patch

from psycopg2 import errors

from govbr_scraper.models.news import NewsInsert

# mock_pool and pg_manager fixtures provided by tests/unit/conftest.py

# content_hash já gravado no banco (4ª coluna do SELECT de _find_existing_by_url).
# Os artigos de _make_news não têm content_hash, então por padrão o conteúdo "mudou".
STORED_HASH = "hash-gravado-000"


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
            ),
        ]

        news = [_make_news("new-uid-456", title="Titulo editado")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        assert mock_exec.call_count == 1
        sql = mock_exec.call_args[0][1]
        rows = mock_exec.call_args[0][2]
        assert "UPDATE news SET" in sql
        assert rows[0][0] == "Titulo editado"
        assert rows[0][-1] == "existing-uid-123"
        assert count == 1
        assert new == 0  # só a Phase 1 (UPDATE): nenhuma linha nova
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
            count, articles, new = pg_manager.insert(news)

        insert_call = mock_exec.call_args
        values = insert_call[0][2]
        assert len(values) == 2
        assert count == 2
        assert new == 2

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
            count, articles, new = pg_manager.insert(news)

        values = mock_exec.call_args[0][2]
        assert len(values) == 2

    def test_update_preserves_original_unique_id(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("original-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]

        news = [_make_news("new-uid-different")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles, new = pg_manager.insert(news)

        assert articles[0]["unique_id"] == "original-uid"

    def test_update_changes_title_content_content_hash(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
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
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
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

    def test_update_preserves_embedding_on_content_change(self, pg_manager, mock_pool):
        """Nem com o conteúdo alterado o re-scrape zera o embedding (antes: sempre zerava).
        O vetor é de título + resumo, o resumo não é regerado na edição e nada regera
        o embedding depois (embeddings-api só assina dgb.news.enriched)."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]

        news = [_make_news("new-uid", title="Titulo editado")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            pg_manager.insert(news)

        sql = mock_exec.call_args[0][1]
        assert "UPDATE news SET" in sql
        assert "content_embedding" not in sql
        assert "embedding_generated_at" not in sql

    def test_mixed_batch_new_and_existing(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/existing", STORED_HASH),
        ]

        news = [
            _make_news("existing-uid-new", url="https://example.com/existing"),
            _make_news("brand-new-uid", url="https://example.com/new"),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[None, [("brand-new-uid",)]],
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        assert mock_exec.call_count == 2
        update_sql = mock_exec.call_args_list[0][0][1]
        assert "UPDATE news SET" in update_sql
        insert_values = mock_exec.call_args_list[1][0][2]
        assert len(insert_values) == 1
        assert insert_values[0][0] == "brand-new-uid"
        assert count == 2
        assert new == 1
        assert len(articles) == 2

    def test_returns_metadata_for_updated_articles(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]

        news = [_make_news("new-uid")]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ):
            count, articles, new = pg_manager.insert(news)

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
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH)],
        ]

        news = [_make_news("new-uid")]
        violation = errors.UniqueViolation()

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[violation, None],
        ):
            count, articles, new = pg_manager.insert(news)

        savepoint_calls = [
            c
            for c in mock_cursor.execute.call_args_list
            if isinstance(c[0][0], str) and "SAVEPOINT" in c[0][0]
        ]
        assert any("ROLLBACK TO SAVEPOINT" in c[0][0] for c in savepoint_calls)
        assert count == 1
        assert new == 0  # o worker concorrente inseriu; aqui foi UPDATE
        assert articles[0]["unique_id"] == "race-uid"

    def test_race_retry_counts_new_rows_inserted_in_retry(self, pg_manager, mock_pool):
        """No retry do UniqueViolation, a parte que ainda não existia é inserida e conta
        em articles_new; a casada por URL (worker concorrente) só em articles_saved."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/race", STORED_HASH)],
        ]
        news = [
            _make_news("new-uid-1", url="https://example.com/race"),
            _make_news("new-uid-2", url="https://example.com/nova"),
        ]

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation(), None, [("new-uid-2",)]],
        ):
            count, articles, new = pg_manager.insert(news)

        assert count == 2
        assert new == 1
        assert sorted(a["unique_id"] for a in articles) == ["new-uid-2", "race-uid"]


class TestRepublishOnlyChangedContent:
    """Re-scrape casado por (agency_key, url): só o artigo com content_hash diferente é
    atualizado (allow_update=False) e republicado em dgb.news.scraped. O de hash igual
    não recebe UPDATE, não conta em articles_saved e não é republicado, com ou sem tema
    (o retry do enriquecimento que falhou é o reconciliador do /dispatch).
    Com allow_update=True todo artigo casado é atualizado, mas só o alterado é
    republicado."""

    def test_find_existing_by_url_selects_content_hash(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]

        result = pg_manager._find_existing_by_url(
            [("ebc", "https://example.com/article")], mock_cursor
        )

        select_sql = " ".join(mock_cursor.execute.call_args[0][0].split())
        assert "SELECT unique_id, agency_key, url, content_hash FROM news" in select_sql
        assert "most_specific_theme_id" not in select_sql
        assert result == {("ebc", "https://example.com/article"): ("existing-uid", STORED_HASH)}

    def test_unchanged_content_is_not_updated_counted_or_returned(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        mock_exec.assert_not_called()  # nem UPDATE (Phase 1) nem INSERT (Phase 2)
        assert count == 0  # articles_saved: nada foi gravado
        assert new == 0
        assert articles == []

    def test_unchanged_content_without_theme_is_not_returned(self, pg_manager, mock_pool):
        """Sem tema também não é republicado: o critério "tema nulo" saiu (o retry do
        enriquecimento que falhou é o reconciliador diário do /dispatch)."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        mock_exec.assert_not_called()
        assert (count, new, articles) == (0, 0, [])

    def test_unchanged_content_with_allow_update_is_updated_but_not_returned(
        self, pg_manager, mock_pool
    ):
        """allow_update=True força a regravação (ex.: parser corrigido em campo fora do
        hash): o UPDATE roda e conta em articles_saved, mas não há republicação."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news, allow_update=True)

        assert mock_exec.call_count == 1
        assert "UPDATE news SET" in mock_exec.call_args[0][1]
        assert count == 1
        assert new == 0
        assert articles == []

    def test_changed_content_is_updated_and_returned(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", STORED_HASH),
        ]
        news = [_make_news("existing-uid", content="Conteudo corrigido")]
        news[0].content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        assert "UPDATE news SET" in mock_exec.call_args[0][1]
        assert mock_exec.call_args[0][2][0][2] == "hash-novo-000000"
        assert count == 1
        assert new == 0
        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_stored_hash_null_counts_as_changed(self, pg_manager, mock_pool):
        """Linha legada sem content_hash: o re-scrape que traz o hash é mudança."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", None),
        ]
        news = [_make_news("existing-uid")]
        news[0].content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, _ = pg_manager.insert(news)

        assert "UPDATE news SET" in mock_exec.call_args[0][1]
        assert count == 1
        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_both_hashes_null_counts_as_changed(self, pg_manager, mock_pool):
        """Sem hash dos dois lados não dá para afirmar que nada mudou: atualiza."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("existing-uid", "ebc", "https://example.com/article", None),
        ]
        news = [_make_news("existing-uid")]  # content_hash None

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, _ = pg_manager.insert(news)

        assert mock_exec.call_count == 1
        assert count == 1
        assert [a["unique_id"] for a in articles] == ["existing-uid"]

    def test_mixed_batch_counts_and_returns_only_new_and_changed(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("uid-igual", "ebc", "https://example.com/igual", "hash-igual-00000"),
            ("uid-sem-tema", "ebc", "https://example.com/sem-tema", "hash-sem-tema-00"),
            ("uid-mudou", "ebc", "https://example.com/mudou", "hash-antigo-0000"),
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
        ) as mock_exec:
            count, articles, new = pg_manager.insert([igual, sem_tema, mudou, nova])

        update_rows = mock_exec.call_args_list[0][0][2]
        assert [row[-1] for row in update_rows] == ["uid-mudou"]  # só o alterado
        insert_rows = mock_exec.call_args_list[1][0][2]
        assert [row[0] for row in insert_rows] == ["uid-nova"]
        assert count == 2  # 1 atualizado + 1 inserido
        assert new == 1
        assert sorted(a["unique_id"] for a in articles) == ["uid-mudou", "uid-nova"]

    def test_mixed_batch_with_allow_update_updates_all_but_returns_changed(
        self, pg_manager, mock_pool
    ):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.return_value = [
            ("uid-igual", "ebc", "https://example.com/igual", "hash-igual-00000"),
            ("uid-mudou", "ebc", "https://example.com/mudou", "hash-antigo-0000"),
        ]
        igual = _make_news("uid-igual", url="https://example.com/igual")
        igual.content_hash = "hash-igual-00000"
        mudou = _make_news("uid-mudou", url="https://example.com/mudou")
        mudou.content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
        ) as mock_exec:
            count, articles, new = pg_manager.insert([igual, mudou], allow_update=True)

        update_rows = mock_exec.call_args[0][2]
        assert sorted(row[-1] for row in update_rows) == ["uid-igual", "uid-mudou"]
        assert count == 2
        assert new == 0
        assert [a["unique_id"] for a in articles] == ["uid-mudou"]

    def test_race_retry_skips_unchanged_content(self, pg_manager, mock_pool):
        """No retry do UniqueViolation (worker concorrente inseriu antes), o artigo
        casado por URL com o mesmo conteúdo não é atualizado, contado nem republicado."""
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH)],
        ]
        news = [_make_news("new-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation()],
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news)

        assert mock_exec.call_count == 1  # só o INSERT abortado; sem UPDATE no retry
        assert (count, new, articles) == (0, 0, [])

    def test_race_retry_with_allow_update_updates_but_does_not_return(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH)],
        ]
        news = [_make_news("new-uid")]
        news[0].content_hash = STORED_HASH

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation(), None],
        ) as mock_exec:
            count, articles, new = pg_manager.insert(news, allow_update=True)

        assert "UPDATE news SET" in mock_exec.call_args_list[1][0][1]
        assert (count, new, articles) == (1, 0, [])

    def test_race_retry_returns_changed_content(self, pg_manager, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchall.side_effect = [
            [],
            [("race-uid", "ebc", "https://example.com/article", STORED_HASH)],
        ]
        news = [_make_news("new-uid")]
        news[0].content_hash = "hash-novo-000000"

        with patch(
            "govbr_scraper.storage.postgres_manager.execute_values",
            side_effect=[errors.UniqueViolation(), None],
        ):
            count, articles, new = pg_manager.insert(news)

        assert (count, new) == (1, 0)
        assert [a["unique_id"] for a in articles] == ["race-uid"]
