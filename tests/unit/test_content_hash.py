from govbr_scraper.scrapers.content_hash import compute_content_hash, normalize_text


class TestNormalizeText:
    def test_lowercase(self):
        assert normalize_text("TITULO") == "titulo"

    def test_remove_accents(self):
        assert normalize_text("Educação Pública") == "educacao publica"

    def test_collapse_whitespace(self):
        assert normalize_text("a  b   c") == "a b c"

    def test_strip_whitespace(self):
        assert normalize_text("  texto  ") == "texto"

    def test_remove_punctuation(self):
        assert normalize_text("titulo: subtitulo!") == "titulo subtitulo"

    def test_preserves_numbers(self):
        assert normalize_text("R$ 1 bilhão em 2025") == "r 1 bilhao em 2025"

    def test_none_returns_empty(self):
        assert normalize_text(None) == ""

    def test_empty_string(self):
        assert normalize_text("") == ""


class TestComputeContentHash:
    def test_deterministic(self):
        h1 = compute_content_hash("titulo", "conteudo")
        h2 = compute_content_hash("titulo", "conteudo")
        assert h1 == h2

    def test_length_16_hex(self):
        h = compute_content_hash("titulo", "conteudo")
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_same_content_different_agencies(self):
        h1 = compute_content_hash("titulo", "conteudo")
        h2 = compute_content_hash("titulo", "conteudo")
        assert h1 == h2

    def test_different_content_different_hash(self):
        h1 = compute_content_hash("titulo A", "conteudo A")
        h2 = compute_content_hash("titulo B", "conteudo B")
        assert h1 != h2

    def test_case_insensitive(self):
        h1 = compute_content_hash("Lula", "conteudo")
        h2 = compute_content_hash("lula", "conteudo")
        assert h1 == h2

    def test_whitespace_insensitive(self):
        h1 = compute_content_hash("a b", "conteudo")
        h2 = compute_content_hash("a  b", "conteudo")
        assert h1 == h2

    def test_accent_insensitive(self):
        h1 = compute_content_hash("educacao", "conteudo")
        h2 = compute_content_hash("educação", "conteudo")
        assert h1 == h2

    def test_none_content_uses_title_only(self):
        h1 = compute_content_hash("titulo", None)
        h2 = compute_content_hash("titulo", None)
        assert h1 == h2
        assert len(h1) == 16

    def test_empty_content_uses_title_only(self):
        h1 = compute_content_hash("titulo", "")
        h2 = compute_content_hash("titulo", None)
        assert h1 == h2
