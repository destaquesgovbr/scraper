"""
Validação do conteúdo extraído antes de persistir (scraper#65).

Itens com conteúdo inválido (tela de login do Plone, falha de extração ou vazio) não são
gravados: o ScrapeManager pula, loga e conta. Como a URL não é gravada, ela não entra em
``known_urls`` e o artigo é tentado de novo numa execução seguinte.
"""

import unicodedata
from typing import Any

# Marcador devolvido por WebScraper.get_article_content quando a extração falha.
ERROR_CONTENT = "Error retrieving content"
# Marcador devolvido quando a página do artigo redireciona para a tela de login do Plone.
LOGIN_REQUIRED_CONTENT = "Login required"

# Texto da tela de login do Plone ("É necessário autenticar para visualizar essa página"),
# comparado sem acento e em minúsculas.
_LOGIN_MARKERS = ("necessario autenticar para visualizar",)


def _normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return text.lower()


def invalid_content_reason(item: dict[str, Any]) -> str | None:
    """Devolve o motivo para não persistir o item, ou None se o conteúdo é válido.

    Motivos: ``empty_content``, ``error_content`` e ``login_page``.
    """
    content = (item.get("content") or "").strip()
    if not content:
        return "empty_content"
    if content == ERROR_CONTENT:
        return "error_content"
    if content == LOGIN_REQUIRED_CONTENT:
        return "login_page"
    normalized = _normalize(content)
    if any(marker in normalized for marker in _LOGIN_MARKERS):
        return "login_page"
    return None


def is_login_redirect(final_url: str | None) -> bool:
    """True se a URL final (depois dos redirects) é a tela de login do Plone."""
    if not final_url:
        return False
    return "require_login" in final_url or "/acl_users/" in final_url
