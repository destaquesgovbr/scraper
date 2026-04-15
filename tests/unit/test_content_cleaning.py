"""
Unit tests for content cleaning algorithm.

Tests cover:
1. _is_junk_line() — contextual detection of junk vs legitimate content
2. _clean_markdown_content() — full markdown cleaning pipeline
3. _clean_html_content() — HTML cleaning with contact/metadata removal
4. _clean_html_with_validation() — fallback mechanism when over-cleaned
5. _clean_markdown_content() fallback — protection against markdown over-cleaning
"""

import pytest
from bs4 import BeautifulSoup
from govbr_scraper.scrapers.webscraper import WebScraper


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def scraper():
    """Create a WebScraper instance for testing."""
    return WebScraper(
        base_url="https://www.gov.br/test/pt-br/noticias",
        min_date="2026-01-01",
    )


@pytest.fixture
def html_article_with_comunicacao():
    """Article where 'comunicação' and 'imprensa' appear in legitimate body content."""
    return """
    <div id="content">
        <h1>Governo lança programa de comunicação digital</h1>
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">04/03/2026 14h00</span>
        </span>
        <p class="discreet">Iniciativa visa modernizar a comunicação entre órgãos federais</p>
        <p>O Ministério das Comunicações anunciou nesta terça-feira o lançamento de um novo
        programa de comunicação digital que vai modernizar a forma como os órgãos federais
        se relacionam com a população.</p>
        <p>O ministro deu coletiva de imprensa no Palácio do Planalto para detalhar as medidas
        que serão implementadas ao longo dos próximos meses. A assessoria jurídica do órgão
        emitiu parecer favorável à iniciativa.</p>
        <p>Entre as ações previstas estão a criação de canais de comunicação direta com o
        cidadão e a modernização dos portais governamentais.</p>
        <p>"A comunicação pública precisa acompanhar as transformações digitais", disse o
        secretário de imprensa do governo.</p>
        <p>O programa terá investimento de R$ 50 milhões e será executado em parceria com
        universidades públicas.</p>
        <p><strong>Assessoria de Comunicação - MCom</strong></p>
    </div>
    """


@pytest.fixture
def html_article_with_phone_service():
    """Article with public service phone numbers in the body content."""
    return """
    <div id="content">
        <h1>Novo canal de denúncias do governo começa a funcionar</h1>
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">04/03/2026 10h00</span>
        </span>
        <p>O Ministério dos Direitos Humanos e da Cidadania lançou um novo canal de denúncias
        para casos de violação de direitos humanos.</p>
        <p>A população pode acessar o serviço pelo telefone (61) 9916-8979 ou pelo
        aplicativo "Aqui é Brasil", disponível para Android e iOS.</p>
        <p>Além do novo canal, o Disque 100 continua funcionando 24 horas por dia. O atendimento
        também pode ser feito pelo telefone (61) 3030-5000, de segunda a sexta-feira.</p>
        <p>O programa "Aqui é Brasil" já atendeu mais de 2.000 brasileiros no exterior desde
        seu lançamento em janeiro.</p>
        <p>Para mais informações, entre em contato pelo email aquiebrasil@mdh.gov.br ou pelo
        WhatsApp (61) 99168-9789.</p>
        <p><strong>Assessoria de Comunicação - MDHC</strong></p>
        <p>imprensa@mdh.gov.br</p>
        <p>(61) 2027-3538</p>
    </div>
    """


@pytest.fixture
def html_article_standard():
    """Standard article with no problematic keywords — baseline test."""
    return """
    <div id="content">
        <h1>Brasil registra recorde de exportações no primeiro trimestre</h1>
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">04/03/2026 09h00</span>
        </span>
        <p class="discreet">Volume exportado cresceu 15% em relação ao mesmo período do ano anterior</p>
        <p>O Brasil registrou um recorde histórico de exportações no primeiro trimestre de 2026,
        com um volume total de US$ 85 bilhões, segundo dados divulgados pelo Ministério do
        Desenvolvimento, Indústria e Comércio.</p>
        <p>O crescimento de 15% em relação ao mesmo período do ano anterior foi puxado
        principalmente pelo agronegócio, que respondeu por 40% do total exportado.</p>
        <p>A soja liderou a pauta de exportações, seguida por minério de ferro, petróleo,
        carne bovina e celulose.</p>
        <p>O superávit comercial acumulado no trimestre chegou a US$ 20 bilhões, o maior
        valor da série histórica iniciada em 1997.</p>
        <p>"Os números refletem a competitividade da economia brasileira no cenário
        internacional", afirmou o ministro durante evento em Brasília.</p>
        <p>As exportações para a China, principal parceiro comercial do Brasil, cresceram
        22% no período.</p>
        <p>Para o segundo trimestre, a expectativa do governo é manter o ritmo de crescimento,
        impulsionado pela safra recorde de grãos.</p>
        <div class="social-links">
            <a href="https://facebook.com/share">Compartilhe no Facebook</a>
            <a href="https://twitter.com/share">Compartilhe no Twitter</a>
        </div>
        <div class="keywords">
            <a href="?origem=keyword">exportações</a>
            <a href="?origem=keyword">comércio exterior</a>
        </div>
    </div>
    """


@pytest.fixture
def html_article_short_with_metadata():
    """Short article (5 paragraphs) with heavy metadata — tests over-cleaning risk."""
    return """
    <div id="content">
        <h1>Anvisa aprova novo medicamento para diabetes</h1>
        <span class="documentPublished">
            <span>Publicado em</span>
            <span class="value">04/03/2026 16h00</span>
        </span>
        <span class="documentModified">
            <span>Atualizado em</span>
            <span class="value">04/03/2026 17h30</span>
        </span>
        <p class="discreet">Medicamento será disponibilizado pelo SUS a partir de abril</p>
        <p>A Agência Nacional de Vigilância Sanitária aprovou nesta segunda-feira um novo
        medicamento para o tratamento de diabetes tipo 2.</p>
        <p>O medicamento, que já é comercializado em mais de 50 países, será disponibilizado
        pelo Sistema Único de Saúde a partir do mês de abril.</p>
        <p>A aprovação foi baseada em estudos clínicos que demonstraram eficácia superior
        aos tratamentos disponíveis atualmente.</p>
        <p>Segundo a Anvisa, o medicamento apresenta perfil de segurança favorável e poucos
        efeitos colaterais.</p>
        <p>A decisão foi publicada no Diário Oficial da União desta segunda-feira.</p>
        <div class="documentByLine">Por Redação</div>
        <div class="keywords">
            <a href="?origem=keyword">saúde</a>
            <a href="?origem=keyword">anvisa</a>
            <a href="?origem=keyword">medicamento</a>
        </div>
        <p>Categoria: Saúde e Vigilância Sanitária</p>
        <p>Tags: saúde, anvisa, medicamento, diabetes, SUS</p>
    </div>
    """


# =============================================================================
# Tests: _is_junk_line — Legitimate content must NOT be detected as junk
# =============================================================================


class TestIsJunkLineLegitimateContent:
    """Lines containing keywords in legitimate article context must NOT be junk."""

    def test_line_with_comunicacao_in_body_is_not_junk(self, scraper):
        line = "O programa de comunicação digital foi lançado pelo ministério"
        assert scraper._is_junk_line(line) is False

    def test_line_with_imprensa_in_body_is_not_junk(self, scraper):
        line = "O ministro deu coletiva de imprensa no Palácio do Planalto"
        assert scraper._is_junk_line(line) is False

    def test_line_with_assessoria_in_body_is_not_junk(self, scraper):
        line = "A assessoria jurídica do órgão emitiu parecer favorável à iniciativa"
        assert scraper._is_junk_line(line) is False

    def test_line_with_phone_in_body_is_not_junk(self, scraper):
        line = "A população pode acessar o serviço pelo telefone (61) 9916-8979 ou pelo aplicativo"
        assert scraper._is_junk_line(line) is False

    def test_line_about_press_secretary_is_not_junk(self, scraper):
        line = 'disse o secretário de imprensa do governo durante o evento'
        assert scraper._is_junk_line(line) is False

    def test_line_about_public_communication_is_not_junk(self, scraper):
        line = '"A comunicação pública precisa acompanhar as transformações digitais"'
        assert scraper._is_junk_line(line) is False

    def test_line_with_multiple_phones_in_service_context_is_not_junk(self, scraper):
        line = "O atendimento pode ser feito pelo telefone (61) 3030-5000, de segunda a sexta"
        assert scraper._is_junk_line(line) is False

    def test_long_line_with_assessoria_mention_is_not_junk(self, scraper):
        line = "A assessoria de comunicação do ministério informou que o prazo foi prorrogado até dezembro"
        assert scraper._is_junk_line(line) is False


# =============================================================================
# Tests: _is_junk_line — Actual junk must still be detected
# =============================================================================


class TestIsJunkLineActualJunk:
    """Lines that are actual junk metadata must still be correctly detected."""

    def test_standalone_assessoria_attribution_is_junk(self, scraper):
        assert scraper._is_junk_line("Assessoria de Comunicação - MDS") is True

    def test_standalone_assessoria_imprensa_is_junk(self, scraper):
        assert scraper._is_junk_line("Assessoria de Imprensa") is True

    def test_bold_assessoria_attribution_is_junk(self, scraper):
        assert scraper._is_junk_line("**Assessoria de Comunicação - MDS**") is True

    def test_standalone_phone_only_is_junk(self, scraper):
        assert scraper._is_junk_line("(61) 2027-3538") is True

    def test_compartilhe_is_junk(self, scraper):
        assert scraper._is_junk_line("Compartilhe esta notícia") is True

    def test_facebook_link_is_junk(self, scraper):
        assert scraper._is_junk_line("facebook.com/ministerio") is True

    def test_publicado_em_is_junk(self, scraper):
        assert scraper._is_junk_line("Publicado em 04/03/2026 14h00") is True

    def test_email_only_is_junk(self, scraper):
        assert scraper._is_junk_line("imprensa@mdh.gov.br") is True

    def test_comunicacao_email_is_junk(self, scraper):
        assert scraper._is_junk_line("comunicacao@mds.gov.br") is True

    def test_navigation_noticias_is_junk(self, scraper):
        assert scraper._is_junk_line("Notícias") is True

    def test_copiar_link_is_junk(self, scraper):
        assert scraper._is_junk_line("Copiar link") is True

    def test_twitter_link_is_junk(self, scraper):
        assert scraper._is_junk_line("twitter.com/governo") is True

    def test_tags_label_is_junk(self, scraper):
        assert scraper._is_junk_line("Tags: saúde, educação") is True

    def test_categoria_label_is_junk(self, scraper):
        assert scraper._is_junk_line("Categoria: Saúde") is True


# =============================================================================
# Tests: _is_junk_line — Edge cases
# =============================================================================


class TestIsJunkLineEdgeCases:
    """Edge cases that test boundary behaviors of junk detection."""

    def test_line_with_only_numbers_is_not_junk(self, scraper):
        """Lines with only numbers should not be considered junk."""
        assert scraper._is_junk_line("12345 67890") is False
        assert scraper._is_junk_line("2026") is False

    def test_line_with_only_special_chars_not_detected_as_junk(self, scraper):
        """Lines with only special characters are not detected by current implementation.

        Note: This documents current behavior. These lines are rare in actual content
        and would be filtered by length checks elsewhere in the pipeline.
        """
        # Current implementation doesn't specifically target special-only lines
        assert scraper._is_junk_line("--- *** ---") is False
        assert scraper._is_junk_line("* * * * *") is False

    def test_url_path_without_domain_not_detected_as_junk(self, scraper):
        """URL paths without domain are not specifically targeted by junk detection.

        Note: These are typically handled by HTML parsing/extraction logic,
        not by the line-by-line junk detection.
        """
        # Short navigation paths
        assert scraper._is_junk_line("/noticias") is False
        # Longer URL paths with context
        assert scraper._is_junk_line("/noticias/2026/01/15/artigo-completo-sobre-programa") is False

    def test_legitimate_content_with_keyword_in_proper_name(self, scraper):
        """Keywords appearing in proper names should not trigger false positives."""
        line = "O Ministério da Assessoria Jurídica da União lançou novo programa de compliance"
        # This is a legitimate ministry name with "Assessoria" in it
        assert scraper._is_junk_line(line) is False

    def test_very_long_line_with_keyword_at_start(self, scraper):
        """Long lines with keyword at start but substantial content should not be junk."""
        line = (
            "Assessoria técnica do governo federal divulgou relatório de 200 páginas "
            "detalhando os investimentos em infraestrutura realizados ao longo de 2026, "
            "incluindo dados sobre rodovias, ferrovias e portos em todas as regiões do país."
        )
        # Long enough to be substantive content, not just attribution
        assert scraper._is_junk_line(line) is False


# =============================================================================
# Tests: _clean_markdown_content — Full pipeline
# =============================================================================


class TestCleanMarkdownContent:
    """Test the full markdown cleaning pipeline."""

    def test_preserves_paragraph_with_comunicacao(self, scraper):
        markdown = (
            "O governo lançou um programa de comunicação digital.\n\n"
            "O investimento será de R$ 50 milhões.\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "comunicação digital" in result

    def test_preserves_paragraph_with_imprensa(self, scraper):
        markdown = (
            "O ministro deu coletiva de imprensa sobre as novas medidas.\n\n"
            "As ações serão implementadas em 90 dias.\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "coletiva de imprensa" in result

    def test_preserves_paragraph_with_phone_service(self, scraper):
        markdown = (
            "A população pode ligar para (61) 9916-8979 para denúncias.\n\n"
            "O serviço funciona 24 horas.\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "(61) 9916-8979" in result

    def test_removes_standalone_attribution_line(self, scraper):
        markdown = (
            "O programa foi lançado nesta terça-feira.\n\n"
            "**Assessoria de Comunicação - MDS**\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "Assessoria de Comunicação - MDS" not in result
        assert "programa foi lançado" in result

    def test_removes_social_media_lines(self, scraper):
        markdown = (
            "O programa foi lançado nesta terça-feira.\n\n"
            "facebook.com/ministerio\n\n"
            "twitter.com/governo\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "facebook.com" not in result
        assert "twitter.com" not in result
        assert "programa foi lançado" in result

    def test_removes_publicado_em_line(self, scraper):
        markdown = (
            "Publicado em 04/03/2026 14h00\n\n"
            "O Brasil registrou recorde de exportações.\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "Publicado em" not in result
        assert "recorde de exportações" in result

    def test_preserves_assessoria_in_long_paragraph(self, scraper):
        markdown = (
            "A assessoria de comunicação do ministério informou que o prazo para inscrições "
            "no programa foi prorrogado até o dia 30 de dezembro de 2026, atendendo a pedidos "
            "de diversas entidades da sociedade civil.\n"
        )
        result = scraper._clean_markdown_content(markdown)
        assert "assessoria de comunicação do ministério informou" in result


# =============================================================================
# Tests: _clean_html_content — HTML cleaning
# =============================================================================


class TestCleanHtmlContent:
    """Test HTML cleaning preserves legitimate content."""

    def test_preserves_paragraph_mentioning_imprensa(self, scraper):
        html = """
        <div id="content">
            <p>O ministro deu coletiva de imprensa no Palácio do Planalto para
            detalhar as medidas que serão implementadas ao longo dos próximos meses.</p>
            <p>As ações incluem investimentos em infraestrutura.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_content(article_body)
        text = cleaned.get_text()
        assert "coletiva de imprensa" in text

    def test_removes_contact_block_at_end(self, scraper):
        html = """
        <div id="content">
            <p>O governo lançou um novo programa social.</p>
            <p>O programa vai beneficiar milhões de famílias.</p>
            <p><strong>Assessoria de Comunicação - MDS</strong></p>
            <p>imprensa@mds.gov.br</p>
            <p>(61) 2027-3538</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_content(article_body)
        text = cleaned.get_text()
        assert "programa social" in text
        assert "imprensa@mds.gov.br" not in text

    def test_preserves_phone_in_content_paragraph(self, scraper):
        html = """
        <div id="content">
            <p>O Disque 100 pode ser acessado pelo telefone (61) 3030-5000, funcionando
            24 horas por dia, sete dias por semana, incluindo feriados.</p>
            <p>O serviço é gratuito e sigiloso.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_content(article_body)
        text = cleaned.get_text()
        assert "(61) 3030-5000" in text

    def test_preserves_paragraph_with_comunicacao_keyword(self, scraper):
        html = """
        <div id="content">
            <p>O Ministério das Comunicações anunciou nesta terça-feira o lançamento de um
            novo programa de comunicação digital que vai modernizar a forma como os órgãos
            federais se relacionam com a população.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_content(article_body)
        text = cleaned.get_text()
        assert "programa de comunicação digital" in text

    def test_removes_sharing_elements(self, scraper):
        html = """
        <div id="content">
            <p>Notícia importante sobre educação.</p>
            <div class="social-links">
                <a href="https://facebook.com/share">Compartilhe no Facebook</a>
                <a href="https://twitter.com/share">Compartilhe no Twitter</a>
            </div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_content(article_body)
        text = cleaned.get_text()
        assert "educação" in text
        assert "Compartilhe" not in text


# =============================================================================
# Tests: _clean_html_with_validation — Fallback mechanism
# =============================================================================


class TestCleanHtmlWithValidation:
    """Test the fallback mechanism when cleaning is too aggressive."""

    def test_fallback_activates_when_too_much_removed(self, scraper):
        """If aggressive cleaning would remove >60% of content, fallback should activate."""
        html = """
        <div id="content">
            <h1>Título da Notícia</h1>
            <p class="discreet">Subtítulo da notícia</p>
            <p>A assessoria de comunicação do ministério informou sobre o novo programa.</p>
            <p>O secretário de imprensa detalhou as medidas em coletiva.</p>
            <p>O canal de comunicação com o cidadão será modernizado.</p>
            <p>A imprensa nacional repercutiu a iniciativa positivamente.</p>
            <p>O programa de comunicação social será expandido para todos os estados.</p>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned = scraper._clean_html_with_validation(article_body, "https://test.gov.br/noticia")
        text = cleaned.get_text()
        # At least some of the content paragraphs should be preserved
        paragraphs_preserved = sum(
            1 for phrase in [
                "assessoria de comunicação",
                "secretário de imprensa",
                "comunicação com o cidadão",
                "imprensa nacional",
                "comunicação social",
            ]
            if phrase in text.lower()
        )
        assert paragraphs_preserved >= 3, (
            f"Only {paragraphs_preserved}/5 content paragraphs preserved. "
            f"Fallback should have activated. Content: {text[:500]}"
        )


# =============================================================================
# Tests: End-to-end — Full pipeline from HTML to final markdown
# =============================================================================


class TestEndToEndCleaning:
    """Test the full cleaning pipeline from HTML to validated markdown."""

    def test_article_with_comunicacao_preserves_content(self, scraper, html_article_with_comunicacao):
        soup = BeautifulSoup(html_article_with_comunicacao, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned_html = scraper._clean_html_with_validation(article_body, "https://test.gov.br")

        from markdownify import markdownify as md
        content = md(str(cleaned_html))
        result = scraper._clean_markdown_content(content)

        # Body content must be preserved
        assert "comunicação digital" in result
        assert "coletiva de imprensa" in result
        assert "assessoria jurídica" in result
        # Attribution line at the end should be removed
        assert "Assessoria de Comunicação - MCom" not in result

    def test_article_with_phone_service_preserves_phones(self, scraper, html_article_with_phone_service):
        soup = BeautifulSoup(html_article_with_phone_service, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned_html = scraper._clean_html_with_validation(article_body, "https://test.gov.br")

        from markdownify import markdownify as md
        content = md(str(cleaned_html))
        result = scraper._clean_markdown_content(content)

        # Service phone numbers in content must be preserved
        assert "(61) 9916-8979" in result
        assert "(61) 3030-5000" in result
        # Footer contact info should be removed
        assert "imprensa@mdh.gov.br" not in result

    def test_standard_article_preserves_all_content(self, scraper, html_article_standard):
        soup = BeautifulSoup(html_article_standard, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned_html = scraper._clean_html_with_validation(article_body, "https://test.gov.br")

        from markdownify import markdownify as md
        content = md(str(cleaned_html))
        result = scraper._clean_markdown_content(content)

        assert "recorde histórico de exportações" in result
        assert "agronegócio" in result
        assert "competitividade da economia" in result
        # Social links should be removed
        assert "facebook.com" not in result

    def test_short_article_preserves_content(self, scraper, html_article_short_with_metadata):
        soup = BeautifulSoup(html_article_short_with_metadata, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned_html = scraper._clean_html_with_validation(article_body, "https://test.gov.br")

        from markdownify import markdownify as md
        content = md(str(cleaned_html))
        result = scraper._clean_markdown_content(content)

        # All 5 content paragraphs should be preserved
        assert "diabetes tipo 2" in result
        assert "Sistema Único de Saúde" in result
        assert "estudos clínicos" in result
        assert "perfil de segurança" in result
        assert "Diário Oficial" in result
        # Metadata should be removed
        assert "Categoria:" not in result

    def test_final_content_meets_minimum_length(self, scraper, html_article_with_comunicacao):
        soup = BeautifulSoup(html_article_with_comunicacao, "html.parser")
        article_body = soup.find("div", id="content")
        cleaned_html = scraper._clean_html_with_validation(article_body, "https://test.gov.br")

        from markdownify import markdownify as md
        content = md(str(cleaned_html))
        result = scraper._clean_markdown_content(content)

        assert scraper._validate_final_content(result, "https://test.gov.br") is True
        assert len(result.strip()) >= 100
