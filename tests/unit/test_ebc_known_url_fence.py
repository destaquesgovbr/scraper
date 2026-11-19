"""Cerca de URLs conhecidas na EBC (a mesma do gov.br, ver test_known_url_fence.py).

Sem ela, cada execução baixava todos os artigos da EBC desde 00:00 BRT de min_date, e a
Phase 1 regravava cada um (a maior parte da inflação de articles_saved). Agora:

- URL já gravada é pulada antes do sleep e do download;
- 3 conhecidas seguidas param a fonte (``KNOWN_URL_STOP_THRESHOLD``, o mesmo do gov.br);
- URL nova zera o contador;
- a cerca é desligada quando ``max_date`` é anterior a hoje (BRT), o caso do backfill: na
  EBC a data só aparece depois do download, e os primeiros itens da listagem (os mais
  novos, já conhecidos) parariam a fonte antes da janela.
"""

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
import requests

from govbr_scraper.scrapers import ebc_webscraper
from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
from govbr_scraper.scrapers.ebc_webscraper import (
    EBCWebScraper,
    known_url_fence_applies,
    today_brt,
)
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.webscraper import WebScraper
from govbr_scraper.storage.storage_adapter import InsertCounts

BASE_URL = "https://agenciabrasil.ebc.com.br/ultimas"
TODAY = date(2026, 11, 19)


def _url(slug: str) -> str:
    return f"https://agenciabrasil.ebc.com.br/geral/noticia/2026-11/{slug}"


KNOWN = {_url("antiga-1"), _url("antiga-2"), _url("antiga-3"), _url("antiga-4")}


def _article(url: str, date_text: str = "19/11/2026 - 10:00") -> dict:
    return {"title": "Notícia", "url": url, "date": date_text, "error": ""}


@pytest.fixture(autouse=True)
def frozen_today():
    with patch.object(ebc_webscraper, "today_brt", return_value=TODAY):
        yield


@pytest.fixture
def sleep():
    with patch("govbr_scraper.scrapers.ebc_webscraper.time.sleep") as mock_sleep:
        yield mock_sleep


def _scraper(max_date: str | None = "2026-11-19", known_urls=KNOWN) -> EBCWebScraper:
    scraper = EBCWebScraper("2026-11-18", BASE_URL, max_date=max_date, known_urls=known_urls)
    scraper.scrape_news_page = MagicMock(side_effect=lambda url: _article(url))
    return scraper


class TestKnownUrlSkip:
    def test_known_url_is_skipped_without_sleep_or_fetch(self, sleep):
        scraper = _scraper()

        assert scraper.process_news_urls([_url("antiga-1")]) is True

        scraper.scrape_news_page.assert_not_called()
        sleep.assert_not_called()
        assert scraper.news_data == []

    def test_new_url_is_fetched(self, sleep):
        scraper = _scraper()

        assert scraper.process_news_urls([_url("nova")]) is True

        scraper.scrape_news_page.assert_called_once_with(_url("nova"))
        assert sleep.call_count == 1
        assert [n["url"] for n in scraper.news_data] == [_url("nova")]


class TestKnownUrlFenceStop:
    def test_three_consecutive_known_urls_stop(self, sleep):
        scraper = _scraper()
        urls = [_url("antiga-1"), _url("antiga-2"), _url("antiga-3"), _url("nova")]

        assert scraper.process_news_urls(urls) is False

        scraper.scrape_news_page.assert_not_called()
        sleep.assert_not_called()

    def test_new_url_resets_counter(self, sleep):
        scraper = _scraper()
        urls = [
            _url("antiga-1"),
            _url("antiga-2"),
            _url("nova"),
            _url("antiga-3"),
            _url("antiga-4"),
        ]

        assert scraper.process_news_urls(urls) is True

        assert scraper._consecutive_known == 2
        scraper.scrape_news_page.assert_called_once_with(_url("nova"))

    def test_counter_persists_across_index_pages(self, sleep):
        """A cerca conta seguidas mesmo entre páginas do índice, como no gov.br."""
        scraper = _scraper()

        assert scraper.process_news_urls([_url("nova"), _url("antiga-1"), _url("antiga-2")])
        assert scraper.process_news_urls([_url("antiga-3")]) is False

    def test_radio_url_neither_counts_nor_resets(self, sleep):
        scraper = _scraper()
        radio = "https://radios.ebc.com.br/programa/2026-11/audio"
        urls = [_url("antiga-1"), radio, _url("antiga-2"), _url("antiga-3")]

        assert scraper.process_news_urls(urls) is False
        scraper.scrape_news_page.assert_not_called()

    def test_date_stop_still_works(self, sleep):
        scraper = _scraper()
        scraper.scrape_news_page = MagicMock(
            return_value=_article(_url("velha"), "17/11/2026 - 10:00")
        )

        assert scraper.process_news_urls([_url("velha")]) is False

    def test_threshold_is_the_same_as_govbr(self):
        assert EBCWebScraper.KNOWN_URL_STOP_THRESHOLD == WebScraper.KNOWN_URL_STOP_THRESHOLD
        assert EBCWebScraper.KNOWN_URL_STOP_THRESHOLD == Plone6APIScraper.KNOWN_URL_STOP_THRESHOLD
        assert EBCWebScraper.KNOWN_URL_STOP_THRESHOLD == 3


class TestFenceWindow:
    @pytest.mark.parametrize(
        ("max_date", "applies"),
        [
            (None, True),
            (date(2026, 11, 19), True),  # hoje
            (date(2026, 11, 20), True),  # futuro
            (date(2026, 11, 18), False),  # ontem: backfill
            (date(2026, 10, 1), False),
        ],
    )
    def test_known_url_fence_applies(self, max_date, applies):
        assert known_url_fence_applies(max_date, today=TODAY) is applies

    def test_known_url_fence_applies_uses_today_brt(self):
        assert known_url_fence_applies(date(2026, 11, 19)) is True
        assert known_url_fence_applies(date(2026, 11, 18)) is False

    def test_backfill_disables_fence(self, sleep):
        """max_date antes de hoje: as URLs conhecidas são baixadas (como antes)."""
        scraper = _scraper(max_date="2026-11-18")
        urls = [_url("antiga-1"), _url("antiga-2"), _url("antiga-3")]

        assert scraper.known_urls == set()
        assert scraper.process_news_urls(urls) is True
        assert scraper.scrape_news_page.call_count == 3

    def test_window_without_end_keeps_fence(self, sleep):
        scraper = _scraper(max_date=None)
        assert scraper.known_urls == KNOWN

    def test_without_known_urls_everything_is_fetched(self, sleep):
        scraper = _scraper(known_urls=None)
        urls = [_url("antiga-1"), _url("antiga-2"), _url("antiga-3")]

        assert scraper.process_news_urls(urls) is True
        assert scraper.scrape_news_page.call_count == 3


class TestTodayBrt:
    @pytest.mark.parametrize(
        ("utc_now", "expected"),
        [
            (datetime(2026, 11, 20, 2, 59, tzinfo=UTC), date(2026, 11, 19)),  # 23:59 BRT
            (datetime(2026, 11, 20, 3, 0, tzinfo=UTC), date(2026, 11, 20)),  # 00:00 BRT
        ],
    )
    def test_today_brt_uses_sao_paulo(self, utc_now, expected):
        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return utc_now.astimezone(tz)

        with patch.object(ebc_webscraper, "datetime", _FrozenDatetime):
            assert today_brt() == expected


class TestScrapeNewsStopsAtFence:
    def test_listing_of_known_urls_does_not_fetch_articles_or_next_page(self, sleep):
        listing = "".join(
            f'<a class="capa-noticia" href="{_url(slug)}">x</a>'
            for slug in ("antiga-1", "antiga-2", "antiga-3", "nova")
        )
        response = requests.Response()
        response.status_code = 200
        response._content = f"<html><body>{listing}</body></html>".encode()
        scraper = _scraper()

        with patch("requests.get", return_value=response) as mock_get:
            assert scraper.scrape_news() == []

        assert mock_get.call_count == 1  # só a página 0 do índice
        scraper.scrape_news_page.assert_not_called()


class TestEbcManagerKnownUrls:
    def _run(self, max_date: str, storage: MagicMock):
        scraper = MagicMock()
        scraper.scrape_news.return_value = []
        with patch(
            "govbr_scraper.scrapers.ebc_scrape_manager.EBCWebScraper", return_value=scraper
        ) as cls:
            result = EBCScrapeManager(storage).run_scraper(
                min_date="2026-11-18",
                max_date=max_date,
                sequential=True,
                agencies=["agencia_brasil"],
            )
        return result, cls

    @staticmethod
    def _storage() -> MagicMock:
        storage = MagicMock()
        storage.get_recent_urls.return_value = KNOWN
        storage.insert.return_value = InsertCounts(saved=0, new=0)
        return storage

    def test_window_until_today_loads_known_urls(self):
        storage = self._storage()

        _, cls = self._run("2026-11-19", storage)

        storage.get_recent_urls.assert_called_once_with("agencia_brasil")
        assert cls.call_args.kwargs["known_urls"] == KNOWN

    def test_backfill_does_not_query_known_urls(self):
        storage = self._storage()

        _, cls = self._run("2026-11-18", storage)

        storage.get_recent_urls.assert_not_called()
        assert cls.call_args.kwargs["known_urls"] == set()

    def test_query_failure_disables_fence(self):
        storage = self._storage()
        storage.get_recent_urls.side_effect = RuntimeError("connection refused")

        result, cls = self._run("2026-11-19", storage)

        assert cls.call_args.kwargs["known_urls"] == set()
        assert result["errors"] == []
        assert result["agencies_processed"] == ["agencia_brasil"]

    def test_all_sources_load_their_own_known_urls(self):
        storage = self._storage()
        scraper = MagicMock()
        scraper.scrape_news.return_value = []
        with patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCWebScraper", return_value=scraper):
            EBCScrapeManager(storage).run_scraper(
                min_date="2026-11-18", max_date="2026-11-19", sequential=True
            )

        called = sorted(c.args[0] for c in storage.get_recent_urls.call_args_list)
        assert called == ["agencia_brasil", "tvbrasil"]
