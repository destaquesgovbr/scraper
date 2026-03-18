"""Tests for the readable unique_id generation module."""

from datetime import date, datetime, timezone

from govbr_scraper.scrapers.unique_id import (
    generate_readable_unique_id,
    generate_suffix,
    slugify,
)


class TestSlugify:

    def test_basic_ascii(self):
        assert slugify("hello world") == "hello-world"

    def test_uppercase_to_lowercase(self):
        assert slugify("Hello World") == "hello-world"

    def test_portuguese_accents(self):
        assert slugify("Educação e Ciência") == "educacao-e-ciencia"

    def test_cedilla(self):
        assert slugify("Preço da força") == "preco-da-forca"

    def test_tildes(self):
        assert slugify("Ação em São Paulo") == "acao-em-sao-paulo"

    def test_special_chars_removed(self):
        assert slugify("R$ 1.000,00 em crédito!") == "r-1-000-00-em-credito"

    def test_consecutive_dashes_collapse(self):
        assert slugify("a  -  b") == "a-b"

    def test_leading_trailing_dashes_stripped(self):
        assert slugify("--hello--") == "hello"

    def test_max_length_truncation(self):
        long_title = "governo-federal-anuncia-novo-programa-de-habitacao-popular-para-familias"
        result = slugify(long_title, max_length=50)
        assert len(result) <= 50
        assert not result.endswith("-")

    def test_max_length_truncates_at_word_boundary(self):
        result = slugify("palavra-um palavra-dois palavra-tres", max_length=25)
        assert len(result) <= 25
        # Should not cut in the middle of a word
        assert not result.endswith("-")

    def test_empty_string(self):
        assert slugify("") == ""

    def test_only_special_chars(self):
        assert slugify("!!!@@@###") == ""

    def test_unicode_nfd_normalization(self):
        # cafe with combining accent (NFD form)
        assert slugify("caf\u0065\u0301") == "cafe"

    def test_default_max_length_is_100(self):
        title = "a " * 200  # 400 chars
        result = slugify(title)
        assert len(result) <= 100

    def test_does_not_truncate_short_text(self):
        assert slugify("curto") == "curto"

    def test_numbers_preserved(self):
        assert slugify("Lei 14.133/2021") == "lei-14-133-2021"


class TestGenerateSuffix:

    def test_deterministic(self):
        s1 = generate_suffix("mec", "2025-01-15", "Título da notícia")
        s2 = generate_suffix("mec", "2025-01-15", "Título da notícia")
        assert s1 == s2

    def test_length_is_6(self):
        result = generate_suffix("mec", "2025-01-15", "Título qualquer")
        assert len(result) == 6

    def test_hex_chars_only(self):
        result = generate_suffix("mec", "2025-01-15", "Título qualquer")
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_titles_different_suffixes(self):
        s1 = generate_suffix("mec", "2025-01-15", "Programa A")
        s2 = generate_suffix("mec", "2025-01-15", "Programa B")
        assert s1 != s2

    def test_different_agencies_different_suffixes(self):
        s1 = generate_suffix("mec", "2025-01-15", "Mesmo título")
        s2 = generate_suffix("saude", "2025-01-15", "Mesmo título")
        assert s1 != s2

    def test_different_dates_different_suffixes(self):
        s1 = generate_suffix("mec", "2025-01-15", "Mesmo título")
        s2 = generate_suffix("mec", "2025-01-16", "Mesmo título")
        assert s1 != s2

    def test_accepts_date_object(self):
        result = generate_suffix("mec", date(2025, 1, 15), "Título")
        assert len(result) == 6

    def test_accepts_date_string(self):
        result = generate_suffix("mec", "2025-01-15", "Título")
        assert len(result) == 6

    def test_date_object_and_isoformat_string_produce_same_suffix(self):
        s1 = generate_suffix("mec", date(2025, 1, 15), "Título")
        s2 = generate_suffix("mec", "2025-01-15", "Título")
        assert s1 == s2

    def test_datetime_object_uses_full_isoformat(self):
        """datetime includes time component, producing different suffix than date-only."""
        dt = datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)
        d = date(2025, 1, 15)
        s_datetime = generate_suffix("mec", dt, "Título")
        s_date = generate_suffix("mec", d, "Título")
        # datetime.isoformat() includes time, so suffixes differ — this is expected
        # because the scraper always passes published_at as string, not datetime
        assert s_datetime != s_date
        assert len(s_datetime) == 6


class TestGenerateReadableUniqueId:

    def test_basic_format(self):
        result = generate_readable_unique_id("mec", "2025-01-15", "Governo anuncia programa")
        assert "_" in result
        parts = result.rsplit("_", 1)
        assert len(parts) == 2
        slug, suffix = parts
        assert slug == "governo-anuncia-programa"
        assert len(suffix) == 6

    def test_real_brazilian_title(self):
        result = generate_readable_unique_id(
            "mec",
            "2025-01-15",
            "Governo Federal anuncia novo programa de habitação popular",
        )
        assert result.startswith("governo-federal-anuncia-novo-programa-de-habitacao-popular_")
        assert len(result.rsplit("_", 1)[1]) == 6

    def test_max_total_length_120_single_word(self):
        long_title = "A" * 300
        result = generate_readable_unique_id("mec", "2025-01-15", long_title)
        assert len(result) <= 120

    def test_max_total_length_120_many_words(self):
        # Generates a slug close to 100 chars to test the real boundary
        long_title = "palavra " * 50  # ~400 chars
        result = generate_readable_unique_id("mec", "2025-01-15", long_title)
        assert len(result) <= 120
        assert "_" in result

    def test_max_total_length_120_realistic_long_title(self):
        # Real-world long Brazilian government title
        long_title = (
            "Ministério da Educação divulga resultado final do processo seletivo "
            "para concessão de bolsas de estudo integrais e parciais em instituições "
            "de ensino superior privadas no âmbito do Programa Universidade para Todos"
        )
        result = generate_readable_unique_id("mec", "2025-01-15", long_title)
        assert len(result) <= 120
        slug = result.rsplit("_", 1)[0]
        assert len(slug) <= 100

    def test_idempotent(self):
        args = ("mec", "2025-01-15", "Título importante sobre educação")
        assert generate_readable_unique_id(*args) == generate_readable_unique_id(*args)

    def test_collision_resistance(self):
        id1 = generate_readable_unique_id("mec", "2025-01-15", "Governo anuncia programa A")
        id2 = generate_readable_unique_id("mec", "2025-01-15", "Governo anuncia programa B")
        assert id1 != id2

    def test_empty_title_fallback(self):
        result = generate_readable_unique_id("mec", "2025-01-15", "")
        assert result.startswith("sem-titulo_")
        assert len(result.rsplit("_", 1)[1]) == 6

    def test_special_chars_only_title_fallback(self):
        result = generate_readable_unique_id("mec", "2025-01-15", "!!!")
        assert result.startswith("sem-titulo_")

    def test_suffix_changes_with_title(self):
        id1 = generate_readable_unique_id("mec", "2025-01-15", "Título original")
        id2 = generate_readable_unique_id("mec", "2025-01-15", "Título modificado")
        suffix1 = id1.rsplit("_", 1)[1]
        suffix2 = id2.rsplit("_", 1)[1]
        assert suffix1 != suffix2

    def test_with_date_object(self):
        result = generate_readable_unique_id(
            "mec", date(2025, 1, 15), "Teste com date object"
        )
        assert "_" in result
        assert len(result.rsplit("_", 1)[1]) == 6

    def test_no_trailing_dash_before_suffix(self):
        result = generate_readable_unique_id("mec", "2025-01-15", "Teste simples")
        slug = result.rsplit("_", 1)[0]
        assert not slug.endswith("-")
