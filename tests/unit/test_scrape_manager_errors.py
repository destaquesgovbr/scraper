"""Contrato de erros e de articles_new dos scrape managers (gov.br e EBC).

Cada item de ``errors`` leva ``category`` (a mesma gravada em scrape_runs) e
``retryable``; agência fora do YAML ou inativa vira ``unknown_agency``, permanente.
"""

from unittest.mock import MagicMock, patch

import pytest

from govbr_scraper.scrapers.ebc_scrape_manager import EBCScrapeManager
from govbr_scraper.scrapers.scrape_manager import ScrapeManager
from govbr_scraper.scrapers.webscraper import ScrapingError
from govbr_scraper.storage.storage_adapter import InsertCounts


def _storage(counts=InsertCounts(saved=0, new=0)):
    storage = MagicMock()
    storage.get_recent_urls.return_value = set()
    storage.insert.return_value = counts
    return storage


class TestUnknownAgency:
    @pytest.mark.parametrize("agency", ["nao_existe_xyz", "cisc"], ids=["ausente", "inativa"])
    def test_govbr_unknown_or_inactive_agency_is_permanent(self, agency):
        storage = _storage()
        result = ScrapeManager(storage).run_scraper(
            agencies=[agency], min_date="2026-11-10", max_date="2026-11-10", sequential=True
        )

        assert result["agencies_processed"] == []
        (error,) = result["errors"]
        assert error["agency"] == agency
        assert error["category"] == "unknown_agency"
        assert error["retryable"] is False
        # Não há scraping: nenhuma linha em scrape_runs.
        storage.record_scrape_run.assert_not_called()

    @pytest.mark.parametrize(
        "agency", ["nao_existe_xyz", "memoria-ebc"], ids=["ausente", "inativa"]
    )
    def test_ebc_unknown_or_inactive_agency_is_permanent(self, agency):
        storage = _storage()
        result = EBCScrapeManager(storage).run_scraper(
            min_date="2026-11-10", max_date="2026-11-10", sequential=True, agencies=[agency]
        )

        (error,) = result["errors"]
        assert error["category"] == "unknown_agency"
        assert error["retryable"] is False
        storage.record_scrape_run.assert_not_called()


class TestGovbrErrorEntries:
    @pytest.fixture
    def run(self):
        def _run(exc):
            storage = _storage()
            scraper = MagicMock()
            scraper.scrape_news.side_effect = exc
            with (
                patch(
                    "govbr_scraper.scrapers.scrape_manager.load_urls_from_yaml",
                    return_value={
                        "mec": {
                            "url": "https://www.gov.br/mec/pt-br/assuntos/noticias",
                            "scraper_type": "plone6_api",
                            "active": True,
                        }
                    },
                ),
                patch(
                    "govbr_scraper.scrapers.scrape_manager.Plone6APIScraper",
                    return_value=scraper,
                ),
            ):
                result = ScrapeManager(storage).run_scraper(
                    agencies=["mec"], min_date="2026-11-10", max_date="2026-11-10", sequential=True
                )
            return result, storage.record_scrape_run.call_args[0][0]

        return _run

    @pytest.mark.parametrize(
        ("exc", "category", "retryable"),
        [
            (
                ScrapingError("Erro de rede ao acessar API de mec: 503 Server Error"),
                "network_error",
                True,
            ),
            (ScrapingError("API de mec retornou resposta não-JSON."), "unknown", True),
            (RuntimeError("could not connect to server: timeout expired"), "network_error", True),
            (ScrapingError("No articles found on first page of mec"), "html_changed", False),
        ],
    )
    def test_error_entry_matches_scrape_run(self, run, exc, category, retryable):
        result, scrape_run = run(exc)

        (error,) = result["errors"]
        assert error["category"] == category
        assert error["retryable"] is retryable
        assert str(scrape_run.error_category) == category


class TestEbcManager:
    def _run(self, scraper_side_effect=None, scraped=None, counts=InsertCounts(saved=0, new=0)):
        storage = _storage(counts)
        scraper = MagicMock()
        if scraper_side_effect is not None:
            scraper.scrape_news.side_effect = scraper_side_effect
        else:
            scraper.scrape_news.return_value = scraped or []
        with patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCWebScraper", return_value=scraper):
            result = EBCScrapeManager(storage).run_scraper(
                min_date="2026-11-10",
                max_date="2026-11-10",
                sequential=True,
                agencies=["agencia_brasil"],
            )
        return result, storage

    def test_error_entry_uses_same_category_as_scrape_run(self):
        result, storage = self._run(RuntimeError("Read timed out"))

        (error,) = result["errors"]
        assert error == {
            "agency": "agencia_brasil",
            "error": "Read timed out",
            "category": "network_error",
            "retryable": True,
        }
        run = storage.record_scrape_run.call_args[0][0]
        assert run.error_category == "network_error"

    def test_counts_new_separately_from_saved(self):
        item = {
            "title": "Notícia",
            "url": "https://agenciabrasil.ebc.com.br/x",
            "content": "Texto",
            "agency": "agencia_brasil",
            "published_datetime": "2026-11-10T10:00:00-03:00",
        }
        result, storage = self._run(scraped=[item], counts=InsertCounts(saved=5, new=2))

        assert (result["articles_saved"], result["articles_new"]) == (5, 2)
        run = storage.record_scrape_run.call_args[0][0]
        assert (run.articles_saved, run.articles_new) == (5, 2)

    def test_no_news_records_zero_new(self):
        result, storage = self._run(scraped=[])

        assert result["articles_new"] == 0
        run = storage.record_scrape_run.call_args[0][0]
        assert run.articles_new == 0
