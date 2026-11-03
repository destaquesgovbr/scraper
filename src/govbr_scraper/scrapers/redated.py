"""
Filtro conservador de notícias redatadas (conteúdo antigo republicado com data nova).

Depois do defeso eleitoral de 2026, vários sites gov.br republicaram conteúdo antigo com data
nova, que aparece também na página do artigo. Como o ``unique_id`` usa título + ``published_at``,
essas notícias entravam como novas e com data errada (em 26-29/10/2026, mais de 600 só pelas
DAGs). Dois sinais baratos, avaliados antes de baixar o artigo:

- Plone 6 (API): ``created`` anterior a ``effective`` em ``REDATED_MIN_GAP_DAYS`` dias ou mais.
- HTML: pasta ``/AAAA/MM/`` (ou ``/AAAA/<mês>/``) da URL anterior ao mês da data da listagem
  por mais de ``URL_FOLDER_MAX_MONTHS`` mês.

Medição em leitura nos 90 dias até 03/11/2026: as duas regras só disparam em ondas de
republicação de um dia (sobretudo 26-29/10) e não pegaram nenhum item de rotina (detalhes no
PR que introduziu o filtro). ``SCRAPER_REDATED_FILTER=observe`` desliga o pulo e só loga.
"""

import logging
import os
import re
from datetime import date, datetime

REDATED_MIN_GAP_DAYS = 30
URL_FOLDER_MAX_MONTHS = 1

# Exceção: conteúdo retido num defeso eleitoral. No defeso de 2026 (04/07 a 25/10) o conteúdo criado
# ficou privado e foi publicado pela primeira vez depois dele, com a data da liberação. Isso não é
# redatação: a data nova é a da primeira publicação. Itens com created (Plone 6) ou pasta de data
# (HTML) dentro de uma dessas janelas não são pulados. Para um defeso futuro, acrescente a janela.
DEFESO_2026_START = date(2026, 7, 4)
DEFESO_2026_END = date(2026, 10, 25)
RETAINED_CONTENT_WINDOWS: tuple[tuple[date, date], ...] = ((DEFESO_2026_START, DEFESO_2026_END),)

_MONTHS = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "março": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
# /2026/10/  /2024/agosto/  /2026/maio-1/ (cópias do Plone ganham sufixo -N)
_FOLDER_RE = re.compile(r"/(20\d{2})/([a-zç]+|\d{2})(?:-\d+)?/")


def filter_mode() -> str:
    """``skip`` (padrão) pula o item; ``observe`` só loga."""
    return (
        "observe" if os.environ.get("SCRAPER_REDATED_FILTER", "").strip() == "observe" else "skip"
    )


def url_folder_month(url: str) -> tuple[int, int] | None:
    """(ano, mês) da pasta de data na URL, ou None se não houver pasta de mês reconhecível."""
    for match in _FOLDER_RE.finditer(url or ""):
        year, month = int(match.group(1)), match.group(2)
        if month.isdigit():
            number = int(month)
            if 1 <= number <= 12:
                return year, number
        elif month in _MONTHS:
            return year, _MONTHS[month]
    return None


def is_stale_url_folder(url: str, ref_date: date | None) -> bool:
    """True se a pasta de data da URL é anterior ao mês de ``ref_date`` por mais de 1 mês."""
    if ref_date is None:
        return False
    folder = url_folder_month(url)
    if folder is None:
        return False
    months = (ref_date.year * 12 + ref_date.month) - (folder[0] * 12 + folder[1])
    return months > URL_FOLDER_MAX_MONTHS


def in_retained_window(day: date) -> bool:
    """True se ``day`` cai numa janela de conteúdo retido (defeso eleitoral)."""
    return any(start <= day <= end for start, end in RETAINED_CONTENT_WINDOWS)


def folder_in_retained_window(year: int, month: int) -> bool:
    """True se a pasta ano/mês cai num mês de janela de conteúdo retido (defeso eleitoral)."""
    key = year * 12 + month
    return any(
        start.year * 12 + start.month <= key <= end.year * 12 + end.month
        for start, end in RETAINED_CONTENT_WINDOWS
    )


def created_date(created: str | None) -> date | None:
    """Data (calendário) do ``created`` ISO da API do Plone; None se não der para ler."""
    if not created:
        return None
    try:
        return datetime.fromisoformat(created).date()
    except (TypeError, ValueError):
        return None


def log_retained(agency: str, url: str, signal: str, detail: str) -> None:
    """Loga o item mantido pela exceção de conteúdo retido no defeso."""
    logging.info(f"redated_kept_retained agency={agency} signal={signal} {detail} url={url}")


def created_gap_days(created: str | None, published_dt: datetime | None) -> float | None:
    """Dias entre ``created`` (ISO, da API do Plone) e a data de publicação; None se não der."""
    if not created or published_dt is None:
        return None
    try:
        created_dt = datetime.fromisoformat(created)
    except (TypeError, ValueError):
        return None
    if (created_dt.tzinfo is None) != (published_dt.tzinfo is None):
        return None
    return (published_dt - created_dt).total_seconds() / 86400


def log_redated(agency: str, url: str, signal: str, detail: str) -> bool:
    """Loga o item com cara de redatado. Devolve True se o item deve ser pulado."""
    mode = filter_mode()
    event = "skipped_redated" if mode == "skip" else "redated_observed"
    logging.warning(f"{event} agency={agency} mode={mode} signal={signal} {detail} url={url}")
    return mode == "skip"
