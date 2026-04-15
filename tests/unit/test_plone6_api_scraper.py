"""
Unit tests for Plone6APIScraper.

Tests cover: URL building, item transformation, date filtering,
known URL fence, fetch error handling.
"""
import json
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
import requests

from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.webscraper import ScrapingError

BASE_URL = "https://www.gov.br/susep/pt-br/central-de-conteudos/noticias"
TZ_BR = timezone(timedelta(hours=-3))


def _make_scraper(min_date="2026-01-01", max_date=None, known_urls=None):
    return Plone6APIScraper(
        min_date=min_date,
        base_url=BASE_URL,
        max_date=max_date,
        known_urls=known_urls,
    )


def _api_item(url=None, title="Título Teste", effective="2026-03-15T10:00:00-03:00",
              modified="2026-03-15T10:05:00-03:00", description="Resumo",
              content="<p>Conteúdo completo</p>"):
    return {
        "@id": url or f"https://www.gov.br/susep/pt-br/noticias/{title.lower().replace(' ', '-')}",
        "title": title,
        "effective": effective,
        "modified": modified,
        "description": description,
        "text": {"data": content, "content-type": "text/html"},
    }


def _api_response(items, total=None):
    return {"items": items, "items_total": total or len(items)}


class TestGetAgencyName:
    """Test agency name extraction from URL."""

    def test_extracts_agency_from_govbr_url(self):
        scraper = _make_scraper()
        assert scraper.agency == "susep"


class TestBuildApiUrl:
    """Test URL transformation from base URL to Plone REST API endpoint."""

    def test_inserts_api_segment(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 25)
        assert "++api++" in url

    def test_appends_search_endpoint(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 25)
        assert "/@search" in url

    def test_preserves_agency_path(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 25)
        assert "/susep/++api++" in url

    def test_includes_portal_type_filter(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 25)
        assert "portal_type=News" in url

    def test_pagination_b_start(self):
        scraper = _make_scraper()
        url0 = scraper._build_api_url(0, 25)
        url25 = scraper._build_api_url(25, 25)
        assert "b_start=0" in url0
        assert "b_start=25" in url25

    def test_pagination_b_size(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 10)
        assert "b_size=10" in url

    def test_includes_fullobjects(self):
        scraper = _make_scraper()
        url = scraper._build_api_url(0, 25)
        assert "fullobjects=1" in url


class TestTransformNewsItem:
    """Test JSON API item → internal dict transformation."""

    def test_maps_required_fields(self):
        scraper = _make_scraper()
        item = _api_item(url="https://www.gov.br/susep/pt-br/noticias/test")
        published_dt = datetime(2026, 3, 15, 10, 0, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)

        assert result["url"] == "https://www.gov.br/susep/pt-br/noticias/test"
        assert result["title"] == "Título Teste"
        assert result["published_at"] == published_dt
        assert result["agency"] == "susep"

    def test_extracts_subtitle_from_description(self):
        scraper = _make_scraper()
        item = _api_item(description="Resumo da notícia")
        published_dt = datetime(2026, 3, 15, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)
        assert result["subtitle"] == "Resumo da notícia"

    def test_converts_html_content_to_text(self):
        scraper = _make_scraper()
        item = _api_item(content="<p>Conteúdo importante aqui</p>")
        published_dt = datetime(2026, 3, 15, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)
        assert "Conteúdo importante aqui" in result["content"]

    def test_extracts_content_from_slate_blocks(self):
        scraper = _make_scraper()
        item = {
            "@id": "https://www.gov.br/susep/pt-br/noticias/test",
            "title": "Test",
            "effective": "2026-03-15T10:00:00-03:00",
            "blocks": {
                "block-1": {"@type": "slate", "plaintext": "Texto do bloco slate"},
                "block-2": {"@type": "image"},
            },
        }
        published_dt = datetime(2026, 3, 15, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)
        assert "Texto do bloco slate" in result["content"]

    def test_handles_text_field_as_plain_string(self):
        """When item['text'] is a raw string instead of a dict, use it directly."""
        scraper = _make_scraper()
        item = {
            "@id": "https://www.gov.br/susep/pt-br/noticias/test",
            "title": "Test",
            "text": "Conteúdo como string simples",
        }
        result = scraper._transform_news_item(item, None)
        assert "Conteúdo como string simples" in result["content"]

    def test_handles_missing_optional_fields(self):
        scraper = _make_scraper()
        item = {
            "@id": "https://www.gov.br/susep/pt-br/noticias/minimal",
            "title": "Mínimo",
        }
        published_dt = datetime(2026, 3, 15, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)
        assert result["subtitle"] is None
        assert result["content"] == ""
        assert result["updated_datetime"] is None
        assert result["image"] is None
        assert result["category"] == "No Category"
        assert result["tags"] == []

    def test_handles_none_published_dt(self):
        scraper = _make_scraper()
        item = _api_item()
        result = scraper._transform_news_item(item, None)
        assert result["published_at"] is None

    def test_includes_extracted_at(self):
        scraper = _make_scraper()
        item = _api_item()
        result = scraper._transform_news_item(item, None)
        assert result["extracted_at"] is not None
        assert isinstance(result["extracted_at"], datetime)
        assert result["extracted_at"].tzinfo is not None  # timezone-aware (UTC)

    def test_extracts_image_url_from_relative_path(self):
        scraper = _make_scraper()
        item = _api_item(url="https://www.gov.br/susep/pt-br/noticias/test")
        item["image"] = {"download": "@@images/image/large"}
        published_dt = datetime(2026, 3, 15, tzinfo=TZ_BR)
        result = scraper._transform_news_item(item, published_dt)
        assert result["image"] == "https://www.gov.br/susep/pt-br/noticias/test/@@images/image/large"

    def test_extracts_image_url_from_absolute_url(self):
        scraper = _make_scraper()
        abs_url = "https://www.gov.br/susep/pt-br/noticias/test/@@images/image/large"
        item = _api_item(url="https://www.gov.br/susep/pt-br/noticias/test")
        item["image"] = {"download": abs_url}
        result = scraper._transform_news_item(item, None)
        assert result["image"] == abs_url

    def test_extracts_category_and_tags_from_subject(self):
        scraper = _make_scraper()
        item = _api_item()
        item["Subject"] = ["Economia", "Regulação"]
        result = scraper._transform_news_item(item, None)
        assert result["category"] == "Economia"
        assert result["tags"] == ["Economia", "Regulação"]

    def test_defaults_to_no_category_when_subject_is_not_a_list(self):
        scraper = _make_scraper()
        item = _api_item()
        item["Subject"] = "Economia"  # string instead of list
        result = scraper._transform_news_item(item, None)
        assert result["category"] == "No Category"
        assert result["tags"] == []

    def test_extracts_content_from_text_block_type(self):
        scraper = _make_scraper()
        item = {
            "@id": "https://www.gov.br/susep/pt-br/noticias/test",
            "title": "Test",
            "blocks": {
                "b1": {"@type": "text", "text": "Conteúdo em bloco text"},
                "b2": {"@type": "textBlock", "text": "Conteúdo em bloco textBlock"},
            },
        }
        result = scraper._transform_news_item(item, None)
        assert "Conteúdo em bloco text" in result["content"]
        assert "Conteúdo em bloco textBlock" in result["content"]

    def test_converts_html_in_text_block_to_markdown(self):
        scraper = _make_scraper()
        item = {
            "@id": "https://www.gov.br/susep/pt-br/noticias/test",
            "title": "Test",
            "blocks": {
                "b1": {"@type": "text", "text": "<p>Texto em HTML</p>"},
            },
        }
        result = scraper._transform_news_item(item, None)
        assert "Texto em HTML" in result["content"]

    def test_invalid_modified_date_produces_none_updated_datetime(self):
        scraper = _make_scraper()
        item = _api_item(modified="not-a-date")
        result = scraper._transform_news_item(item, None)
        assert result["updated_datetime"] is None

    def test_all_12_fields_present(self):
        scraper = _make_scraper()
        item = _api_item()
        result = scraper._transform_news_item(item, datetime(2026, 3, 15, tzinfo=TZ_BR))
        expected_fields = {
            "title", "url", "published_at", "updated_datetime",
            "category", "tags", "editorial_lead", "subtitle",
            "content", "image", "agency", "extracted_at",
        }
        assert expected_fields.issubset(result.keys())


class TestDateFilters:
    """Test min_date and max_date filtering logic."""

    def test_item_before_min_date_stops_scraping(self):
        scraper = _make_scraper(min_date="2026-03-10")
        item = _api_item(effective="2026-03-05T10:00:00-03:00")
        result = scraper._process_news_item(item)
        assert result is False
        assert len(scraper.news_data) == 0

    def test_item_after_max_date_is_skipped_but_continues(self):
        scraper = _make_scraper(min_date="2026-01-01", max_date="2026-03-15")
        item = _api_item(effective="2026-03-20T10:00:00-03:00")
        result = scraper._process_news_item(item)
        assert result is True  # Continua
        assert len(scraper.news_data) == 0  # Mas não adiciona

    def test_item_within_range_is_added(self):
        scraper = _make_scraper(min_date="2026-03-01", max_date="2026-03-31")
        item = _api_item(effective="2026-03-15T10:00:00-03:00")
        result = scraper._process_news_item(item)
        assert result is True
        assert len(scraper.news_data) == 1

    def test_item_with_invalid_date_is_added(self):
        """Items with unparseable dates should still be added (not filtered)."""
        scraper = _make_scraper(min_date="2026-01-01")
        item = _api_item(effective="not-a-date")
        result = scraper._process_news_item(item)
        assert result is True
        assert len(scraper.news_data) == 1

    def test_item_without_effective_field_is_added(self):
        """Items with no effective field bypass date filtering and are added."""
        scraper = _make_scraper(min_date="2026-01-01")
        item = _api_item()
        del item["effective"]
        result = scraper._process_news_item(item)
        assert result is True
        assert len(scraper.news_data) == 1
        assert scraper.news_data[0]["published_at"] is None


class TestKnownUrlFence:
    """Test early-stop optimization when known URLs are encountered."""

    def test_known_url_is_skipped(self):
        known = {"https://www.gov.br/susep/pt-br/noticias/artigo-1"}
        scraper = _make_scraper(known_urls=known)
        item = _api_item(url="https://www.gov.br/susep/pt-br/noticias/artigo-1",
                         effective="2026-03-15T10:00:00-03:00")
        result = scraper._process_news_item(item)
        assert result is True  # Continua (não parou ainda)
        assert len(scraper.news_data) == 0

    def test_three_consecutive_known_urls_stop_scraping(self):
        urls = [
            "https://www.gov.br/susep/pt-br/noticias/artigo-1",
            "https://www.gov.br/susep/pt-br/noticias/artigo-2",
            "https://www.gov.br/susep/pt-br/noticias/artigo-3",
        ]
        scraper = _make_scraper(known_urls=set(urls))
        for url in urls[:2]:
            result = scraper._process_news_item(
                _api_item(url=url, effective="2026-03-15T10:00:00-03:00")
            )
            assert result is True  # Ainda continua

        result = scraper._process_news_item(
            _api_item(url=urls[2], effective="2026-03-15T10:00:00-03:00")
        )
        assert result is False  # Para no 3º consecutivo

    def test_new_url_resets_consecutive_counter(self):
        known = {
            "https://www.gov.br/susep/pt-br/noticias/artigo-1",
            "https://www.gov.br/susep/pt-br/noticias/artigo-2",
        }
        scraper = _make_scraper(known_urls=known)
        # 2 known
        for url in list(known):
            scraper._process_news_item(
                _api_item(url=url, effective="2026-03-15T10:00:00-03:00")
            )
        assert scraper._consecutive_known == 2
        # Novo artigo → reseta contador
        scraper._process_news_item(
            _api_item(url="https://www.gov.br/susep/pt-br/noticias/novo",
                      effective="2026-03-15T10:00:00-03:00")
        )
        assert scraper._consecutive_known == 0


class TestScrapeNewsLoop:
    """Test the main pagination loop in scrape_news()."""

    def test_returns_empty_when_api_returns_no_items(self):
        scraper = _make_scraper()
        with patch.object(scraper, "_fetch_api_page", return_value=_api_response([])):
            result = scraper.scrape_news()
        assert result == []

    def test_processes_single_page(self):
        scraper = _make_scraper(min_date="2026-03-01", max_date="2026-04-01")
        items = [_api_item(title=f"Notícia {i}", effective="2026-03-15T10:00:00-03:00")
                 for i in range(3)]
        with patch.object(scraper, "_fetch_api_page", return_value=_api_response(items)), \
             patch("time.sleep"):
            result = scraper.scrape_news()
        assert len(result) == 3

    def test_stops_pagination_when_all_items_processed(self):
        scraper = _make_scraper(min_date="2026-03-01")
        items = [_api_item(effective="2026-03-15T10:00:00-03:00")]
        mock_fetch = MagicMock(return_value=_api_response(items, total=1))
        with patch.object(scraper, "_fetch_api_page", mock_fetch), \
             patch("time.sleep"):
            scraper.scrape_news()
        assert mock_fetch.call_count == 1  # Só 1 página

    def test_stops_early_when_item_before_min_date(self):
        scraper = _make_scraper(min_date="2026-03-10")
        items = [_api_item(effective="2026-03-05T10:00:00-03:00")]  # before min_date
        with patch.object(scraper, "_fetch_api_page",
                          return_value=_api_response(items, total=100)), \
             patch("time.sleep"):
            result = scraper.scrape_news()
        assert result == []

    def test_paginates_across_multiple_pages(self):
        scraper = _make_scraper(min_date="2026-03-01", max_date="2026-04-01")
        page1 = [_api_item(title=f"Notícia {i}", effective="2026-03-15T10:00:00-03:00")
                 for i in range(3)]
        page2 = [_api_item(title=f"Notícia {i+3}", effective="2026-03-15T10:00:00-03:00")
                 for i in range(2)]
        mock_fetch = MagicMock(side_effect=[
            _api_response(page1, total=5),
            _api_response(page2, total=5),
        ])
        with patch.object(scraper, "_fetch_api_page", mock_fetch), \
             patch("time.sleep"):
            result = scraper.scrape_news()
        assert mock_fetch.call_count == 2
        assert len(result) == 5

    def test_propagates_scraping_error(self):
        scraper = _make_scraper()
        with patch.object(scraper, "_fetch_api_page",
                          side_effect=ScrapingError("Falha na API")):
            with pytest.raises(ScrapingError, match="Falha na API"):
                scraper.scrape_news()

    def test_wraps_request_exception_as_scraping_error(self):
        scraper = _make_scraper()
        with patch.object(scraper, "_fetch_api_page",
                          side_effect=requests.exceptions.ConnectionError("timeout")):
            with pytest.raises(ScrapingError, match="Erro de rede"):
                scraper.scrape_news()


class TestFetchApiPage:
    """Test HTTP fetch and JSON parsing."""

    def test_raises_scraping_error_on_invalid_json(self):
        scraper = _make_scraper()
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = json.JSONDecodeError("err", "doc", 0)
        with patch("requests.get", return_value=mock_response):
            with pytest.raises(ScrapingError, match="não-JSON"):
                scraper._fetch_api_page("https://api.example.com/search")

    def test_returns_parsed_json_on_success(self):
        scraper = _make_scraper()
        expected = {"items": [], "items_total": 0}
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = expected
        with patch("requests.get", return_value=mock_response):
            result = scraper._fetch_api_page("https://api.example.com/search")
        assert result == expected
