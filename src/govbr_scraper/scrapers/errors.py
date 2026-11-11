"""Exceções dos scrapers.

Ficam fora de ``webscraper.py`` para que ``http_policy.py`` possa usá-las sem import circular.
``webscraper`` continua reexportando ``ScrapingError``.
"""


class ScrapingError(Exception):
    """Raised when scraping fails for a detectable reason (anti-bot, blocked, etc)."""

    pass


class PermanentHTTPError(ScrapingError):
    """Falha HTTP que uma nova tentativa não resolve.

    Casos: resposta 4xx (menos 408 e 429) e listagem que redireciona para o login do Plone.
    Não herda de ``requests.exceptions.RequestException``, então o ``@retry`` dos scrapers não
    a repete, e o ``ScrapeManager`` não aciona o fallback Plone 6 com ela.

    :param http_status: status HTTP da resposta (None no redirect para login, que chega como 200).
    :param url: URL final da resposta, depois dos redirects.
    """

    def __init__(self, message: str, http_status: int | None = None, url: str | None = None):
        super().__init__(message)
        self.http_status = http_status
        self.url = url


class LoginRedirectError(PermanentHTTPError):
    """A URL redirecionou para a tela de login do Plone (seção privada ou removida)."""
