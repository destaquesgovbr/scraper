"""Política HTTP dos scrapers: timeout, retry só em erro transitório e falha rápida no resto.

Até 11/2026 os scrapers repetiam qualquer ``RequestException`` 5 vezes (delay=2, backoff=3,
jitter de 1 a 3 s), inclusive 404 e 401: em média 116 s de espera por chamada que nunca ia dar
certo, e 86% do tempo faturável do Cloud Run do scraper (infra#223).

Agora:
- 4xx, menos 408 e 429, levanta ``PermanentHTTPError`` na primeira resposta, sem retry;
- 5xx, 408, 429, timeout e erro de conexão repetem até 3 vezes (esperas de ~1 s e ~2-3 s);
- ``timeout=(5, 15)``: 5 s para conectar e 15 s para ler.
"""

import requests
from retry import retry

from govbr_scraper.scrapers.errors import LoginRedirectError, PermanentHTTPError

# (connect, read) em segundos, para requests.get.
REQUEST_TIMEOUT = (5, 15)

RETRY_TRIES = 3
RETRY_DELAY = 1
RETRY_BACKOFF = 2
RETRY_JITTER = (0, 1)

# 4xx que ainda valem nova tentativa: Request Timeout e Too Many Requests.
RETRYABLE_4XX = frozenset({408, 429})

# Repete só RequestException (timeout, conexão e o HTTPError de 5xx/408/429). PermanentHTTPError
# não é RequestException e sai na primeira tentativa.
retry_transient = retry(
    exceptions=requests.exceptions.RequestException,
    tries=RETRY_TRIES,
    delay=RETRY_DELAY,
    backoff=RETRY_BACKOFF,
    jitter=RETRY_JITTER,
)


def is_permanent_status(status: int) -> bool:
    """True para 4xx que não melhora com nova tentativa (tudo menos 408 e 429)."""
    return 400 <= status < 500 and status not in RETRYABLE_4XX


def raise_for_http_status(response: requests.Response) -> None:
    """Levanta ``PermanentHTTPError`` em 4xx permanente e ``HTTPError`` nos outros erros.

    O ``HTTPError`` (5xx, 408, 429) é ``RequestException`` e é repetido por ``retry_transient``.
    """
    status = getattr(response, "status_code", None)
    if isinstance(status, int) and is_permanent_status(status):
        url = getattr(response, "url", None)
        raise PermanentHTTPError(
            f"HTTP {status} (permanent, not retried): {url}", http_status=status, url=url
        )
    response.raise_for_status()


def login_redirect_error(agency: str, requested_url: str, final_url: str) -> LoginRedirectError:
    """Erro de listagem que redirecionou para o login do Plone.

    A mensagem contém "login redirect", que ``classify_error`` mapeia para ACCESS_DENIED.
    """
    return LoginRedirectError(
        f"Login redirect for {agency}: {requested_url} -> {final_url}",
        http_status=None,
        url=final_url,
    )
