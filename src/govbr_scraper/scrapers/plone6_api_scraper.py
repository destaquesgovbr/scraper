"""
Scraper para agências gov.br que usam Plone 6 com Volto (React SPA).

Utiliza a REST API do Plone (++api++) em vez de parsing HTML, pois
o conteúdo é renderizado client-side via JavaScript.
"""

import json
import logging
import random
import re
import time
from datetime import UTC, datetime
from urllib.parse import urlparse, urlunparse

import requests
from markdownify import markdownify as md

from govbr_scraper.scrapers.content_validation import is_login_redirect
from govbr_scraper.scrapers.http_policy import (
    REQUEST_TIMEOUT,
    login_redirect_error,
    raise_for_http_status,
    retry_transient,
)

# Importar constantes e exceções do WebScraper
from govbr_scraper.scrapers.redated import (
    REDATED_MIN_GAP_DAYS,
    created_date,
    created_gap_days,
    in_retained_window,
    log_redated,
    log_retained,
)
from govbr_scraper.scrapers.webscraper import (
    DEFAULT_HEADERS,
    SLEEP_TIME_INTERVAL,
    ScrapingError,
)

# Configurar logging (mesmo padrão do WebScraper)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class Plone6APIScraper:
    """
    Scraper para agências gov.br que usam Plone 6 com Volto (React SPA).

    Utiliza a REST API do Plone (++api++) em vez de parsing HTML, pois
    o conteúdo é renderizado client-side via JavaScript.
    """

    KNOWN_URL_STOP_THRESHOLD = 3

    def __init__(
        self,
        min_date: str,
        base_url: str,
        max_date: str | None = None,
        known_urls: set | None = None,
    ):
        """
        Inicializar scraper (interface compatível com WebScraper).

        :param min_date: Data mínima (formato: YYYY-MM-DD)
        :param base_url: URL base da página de notícias
        :param max_date: Data máxima (formato: YYYY-MM-DD)
        :param known_urls: Set de URLs já no banco (otimização)
        """
        self.base_url = base_url
        self.min_date = datetime.strptime(min_date, "%Y-%m-%d").date()
        self.max_date = datetime.strptime(max_date, "%Y-%m-%d").date() if max_date else None
        self.news_data = []
        self.agency = self.get_agency_name()
        self.known_urls = known_urls or set()
        self._consecutive_known = 0
        self.skipped_redated = 0

    def get_agency_name(self) -> str:
        """
        Extrair nome da agência da URL base.

        :return: Nome da agência
        """
        return self.base_url.split("/")[3]

    def scrape_news(self) -> list[dict[str, str]]:
        """
        Scraper notícias via API REST do Plone 6.

        :return: Lista de dicionários com dados das notícias
        :raises ScrapingError: Se houver falha na API ou rede
        """
        current_start = 0
        page_size = 25  # Itens por página

        while True:
            api_url = self._build_api_url(current_start, page_size)

            try:
                response_data = self._fetch_api_page(api_url)
            except ScrapingError:
                raise  # Propagar erro detectável
            except requests.exceptions.RequestException as e:
                raise ScrapingError(
                    f"Erro de rede ao acessar API de {self.agency}: {str(e)}"
                ) from e

            items = response_data.get("items", [])
            items_total = response_data.get("items_total", 0)

            if not items:
                logging.info(f"Nenhuma notícia encontrada para {self.agency}.")
                break

            logging.info(
                f"Processando {len(items)} notícias (offset {current_start}/{items_total})"
            )

            # Processar cada item
            for item in items:
                # Sleep entre requests (mesmo padrão do WebScraper)
                # Random sleep is not security-critical, just for rate limiting
                time.sleep(random.uniform(*SLEEP_TIME_INTERVAL))  # nosec B311

                should_continue = self._process_news_item(item)
                if not should_continue:
                    # Parar se data < min_date ou known URL fence
                    return self.news_data

            # Próxima página
            current_start += len(items)
            if current_start >= items_total:
                logging.info(f"Todas as notícias processadas para {self.agency}.")
                break

        return self.news_data

    def _build_api_url(self, b_start: int, b_size: int) -> str:
        """
        Construir URL da API REST do Plone a partir da URL base.

        Exemplo:
        Input:  https://www.gov.br/susep/pt-br/central-de-conteudos/noticias
        Output: https://www.gov.br/susep/++api++/pt-br/central-de-conteudos/noticias/@search?...

        :param b_start: Offset de paginação
        :param b_size: Itens por página
        :return: URL da API completa
        """
        parsed = urlparse(self.base_url)

        # Extrair partes: /AGENCY/PATH
        path_parts = parsed.path.strip("/").split("/", 1)
        agency = path_parts[0]  # Ex: "susep"
        rest_of_path = path_parts[1] if len(path_parts) > 1 else ""

        # Construir novo path: /AGENCY/++api++/PATH/@search
        api_path = f"/{agency}/++api++/{rest_of_path}/@search"

        # Query parameters
        params = [
            "portal_type=News Item",
            "sort_on=effective",
            "sort_order=descending",
            f"b_start={b_start}",
            f"b_size={b_size}",
            "fullobjects=1",  # Retornar objetos completos (com conteúdo)
            "metadata_fields=title",
            "metadata_fields=effective",
            "metadata_fields=modified",
            "metadata_fields=description",
        ]
        query_string = "&".join(params)

        # Reconstruir URL
        api_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                api_path,
                "",  # params
                query_string,
                "",  # fragment
            )
        )

        return api_url

    @retry_transient
    def _fetch_api_page(self, api_url: str) -> dict:
        """
        Fetch página da API, repetindo só falha transitória (política em ``http_policy``).

        :param api_url: URL da API
        :return: Dict com resposta JSON
        :raises PermanentHTTPError: Em 4xx permanente ou redirect para o login do Plone
        :raises ScrapingError: Se resposta não for JSON válido
        """
        logging.info(f"Fetching API: {api_url}")

        headers = DEFAULT_HEADERS.copy()
        headers["Accept"] = "application/json"

        response = requests.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
        raise_for_http_status(response)

        final_url = getattr(response, "url", None)
        if isinstance(final_url, str) and is_login_redirect(final_url):
            raise login_redirect_error(self.agency, api_url, final_url)

        try:
            data = response.json()
        except json.JSONDecodeError as e:
            raise ScrapingError(
                f"API de {self.agency} retornou resposta não-JSON. "
                f"Possível problema de autenticação ou mudança de endpoint. "
                f"URL: {api_url}"
            ) from e

        return data

    def _process_news_item(self, item: dict) -> bool:
        """
        Processar um item da API e adicionar ao news_data.

        Aplica filtros:
        - Data < min_date: parar
        - Data > max_date: pular
        - URL conhecida: known URL fence (parar após 3 consecutivas)

        :param item: Dict do JSON da API
        :return: True para continuar, False para parar
        """
        # 1. Extrair dados básicos
        url = item.get("@id", "")
        title = item.get("title", "No Title")

        # 2. Extrair e validar data
        effective_str = item.get("effective")
        if effective_str:
            try:
                published_dt = datetime.fromisoformat(effective_str)
                news_date = published_dt.date()
            except (ValueError, AttributeError):
                logging.warning(f"Data inválida em {url}: {effective_str}")
                news_date = None
                published_dt = None
        else:
            news_date = None
            published_dt = None

        # 3. Filtro de data (mesma lógica do WebScraper.extract_news_info)
        if news_date:
            if news_date < self.min_date:
                logging.info(f"Parando scrape. Notícia mais antiga que min_date: {news_date}")
                return False  # PARAR
            if self.max_date and news_date > self.max_date:
                logging.info(f"Pulando notícia de {news_date} (mais nova que max_date)")
                return True  # PULAR mas continuar

        # 3b. Conteúdo redatado: created muito anterior à publicação (republicação de item antigo)
        gap = created_gap_days(item.get("created"), published_dt)
        if gap is not None and gap >= REDATED_MIN_GAP_DAYS:
            detail = f"created={item.get('created')} effective={effective_str} gap_days={gap:.0f}"
            created_day = created_date(item.get("created"))
            if created_day is not None and in_retained_window(created_day):
                # conteúdo retido no defeso e publicado depois: primeira publicação, mantém
                log_retained(self.agency, url, "plone_created", detail)
            elif log_redated(self.agency, url, "plone_created", detail):
                self.skipped_redated += 1
                return True  # PULAR mas continuar

        # 4. Known URL fence (mesma lógica do WebScraper)
        if url in self.known_urls:
            self._consecutive_known += 1
            logging.info(
                f"Pulando artigo conhecido "
                f"({self._consecutive_known}/{self.KNOWN_URL_STOP_THRESHOLD}): {url}"
            )
            if self._consecutive_known >= self.KNOWN_URL_STOP_THRESHOLD:
                logging.info(
                    f"Known URL fence: {self.KNOWN_URL_STOP_THRESHOLD} "
                    f"artigos consecutivos conhecidos. Parando."
                )
                return False  # PARAR
            return True  # PULAR mas continuar

        # 5. Reset contador (artigo novo encontrado)
        self._consecutive_known = 0

        # 6. Transformar e adicionar
        news_item = self._transform_news_item(item, published_dt)

        logging.info(f"Artigo extraído: {news_date} - {url}\n")

        self.news_data.append(news_item)
        return True  # CONTINUAR

    def _transform_news_item(self, item: dict, published_dt: datetime | None) -> dict[str, str]:
        """
        Transformar item da API do Plone para formato interno.

        :param item: Dict do JSON da API
        :param published_dt: Datetime já parseado
        :return: Dict no formato esperado pelo ScrapeManager
        """
        # 1. URL
        url = item.get("@id", "")

        # 2. Título
        title = item.get("title", "No Title")

        # 3. Data de atualização
        modified_str = item.get("modified")
        if modified_str:
            try:
                updated_dt = datetime.fromisoformat(modified_str)
            except (ValueError, AttributeError):
                updated_dt = None
        else:
            updated_dt = None

        # 4. Subtítulo (description)
        subtitle = item.get("description", None)

        # 5. Conteúdo: Extrair de blocks (Plone 6 usa formato blocks/slate)
        content_md = ""

        # Tentar extrair de blocks
        blocks = item.get("blocks", {})
        if blocks:
            # Procurar por blocos do tipo 'slate' ou 'text'
            for block_id, block_data in blocks.items():
                if isinstance(block_data, dict):
                    block_type = block_data.get("@type", "")

                    # Slate block (formato Volto)
                    if block_type == "slate":
                        plaintext = block_data.get("plaintext", "")
                        if plaintext:
                            content_md += plaintext + "\n\n"

                    # Text block (formato alternativo)
                    elif block_type in ["text", "textBlock"]:
                        text = block_data.get("text", "")
                        if text:
                            # Se for HTML, converter para Markdown
                            if "<" in text and ">" in text:
                                content_md += md(text) + "\n\n"
                            else:
                                content_md += text + "\n\n"

        # Fallback: tentar campo 'text' (formato antigo)
        if not content_md:
            text_data = item.get("text", {})
            if isinstance(text_data, dict):
                content_html = text_data.get("data", "")
            else:
                content_html = str(text_data) if text_data else ""

            if content_html:
                content_md = md(content_html)

        # Limpeza básica
        if content_md:
            content_md = re.sub(r"\n{3,}", "\n\n", content_md).strip()

        # 6. Imagem
        image_url = None
        if "image" in item:
            image_data = item["image"]
            if isinstance(image_data, dict):
                download_path = image_data.get("download")
                if download_path:
                    # Se já for URL absoluta, usar diretamente; caso contrário,
                    # concatenar com a URL do artigo (não com o pai)
                    if download_path.startswith("http"):
                        image_url = download_path
                    else:
                        image_url = f"{url}/{download_path}"

        # 7. Categoria e tags (Subject no Plone)
        subject = item.get("Subject", [])
        if isinstance(subject, list):
            category = subject[0] if subject else "No Category"
            tags = subject  # Lista completa
        else:
            category = "No Category"
            tags = []

        # 8. Editorial lead (geralmente não disponível na API)
        editorial_lead = None

        # 9. Montar dict final
        return {
            "title": title,
            "url": url,
            "published_at": published_dt,
            "updated_datetime": updated_dt,
            "category": category,
            "tags": tags,
            "editorial_lead": editorial_lead,
            "subtitle": subtitle,
            "content": content_md,
            "image": image_url,
            "agency": self.agency,
            "extracted_at": datetime.now(UTC),
        }
