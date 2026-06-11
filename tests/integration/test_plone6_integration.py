"""
Integration tests for Plone6APIScraper against real Plone 6 REST API.

Tests the Plone6APIScraper with real HTTP requests to validate that:
- Article extraction from JSON works correctly (fields, date parsing, content)
- Date filtering is applied properly
- Recently migrated agency URLs resolve to articles via the full scraper pipeline
"""

import pytest
import requests
from datetime import datetime, timedelta
from pathlib import Path
from govbr_scraper.scrapers.plone6_api_scraper import Plone6APIScraper
from govbr_scraper.scrapers.yaml_config import load_urls_from_yaml

# Choose 1 stable Plone6 agency for testing scraper behavior
# esporte is a good choice as it's a major government agency with consistent availability
PLONE6_AGENCY = "esporte"

# Agencies recently migrated to plone6_api or with corrected URLs.
# These tests validate the configured URL is correct, not scraper behavior.
RECENTLY_MIGRATED_AGENCIES = {
    "abc":               "https://www.gov.br/abc/pt-br/assuntos/noticias",
    "anatel":            "https://www.gov.br/anatel/pt-br/assuntos/noticias",
    "ansn":              "https://www.gov.br/ansn/pt-br/assuntos/noticias",
    "ibc":               "https://www.gov.br/ibc/pt-br/centrais-de-conteudos/noticias",
    "memoriasreveladas": "https://www.gov.br/memoriasreveladas/pt-br/centrais-de-conteudo/destaques",
    "planejamento":      "https://www.gov.br/planejamento/pt-br/assuntos/noticias",
}


@pytest.fixture(scope="module")
def plone6_agency_url():
    """Load Plone6 agency URL from YAML configuration."""
    config_dir = str(Path(__file__).parent.parent.parent / "src" / "govbr_scraper" / "scrapers" / "config")

    try:
        urls = load_urls_from_yaml(config_dir, "site_urls.yaml")
    except Exception as e:
        pytest.skip(f"Could not load site_urls.yaml: {e}")

    if PLONE6_AGENCY not in urls:
        pytest.skip(f"{PLONE6_AGENCY} not found in site_urls.yaml")

    agency_data = urls[PLONE6_AGENCY]

    if agency_data.get("scraper_type") != "plone6_api":
        pytest.skip(
            f"{PLONE6_AGENCY} is not a plone6_api agency "
            f"(type: {agency_data.get('scraper_type')})"
        )

    return agency_data["url"]


@pytest.mark.integration
class TestPlone6APIScraper:
    """Tests Plone6APIScraper behavior: field extraction, date parsing, date filtering."""

    def test_scrape_news_returns_articles(self, plone6_agency_url):
        """
        Plone6APIScraper.scrape_news() should return articles with all required fields.

        Validates: API accessibility, JSON parsing, field extraction (title, url,
        content, published_at), date filtering via min_date constraint.
        """
        min_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        scraper = Plone6APIScraper(
            base_url=plone6_agency_url,
            min_date=min_date,
        )

        try:
            articles = scraper.scrape_news()
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Failed to fetch from API (network issue): {e}")
        except Exception as e:
            pytest.fail(f"scrape_news() raised unexpected error: {e}")

        assert len(articles) > 0, (
            f"No articles found for {PLONE6_AGENCY} in last 30 days. "
            f"This may indicate API changes or the agency hasn't published recently."
        )

        first = articles[0]

        assert "title" in first, "title field is missing"
        assert first["title"], "title is empty"
        assert isinstance(first["title"], str), f"title should be str, got {type(first['title'])}"

        assert "url" in first, "url field is missing"
        assert first["url"], "url is empty"
        assert first["url"].startswith("https://"), f"url should be absolute HTTPS, got {first['url']}"

        assert "content" in first, "content field is missing"
        assert first["content"], "content is empty"
        assert isinstance(first["content"], str), f"content should be str, got {type(first['content'])}"

        assert "published_at" in first, "published_at field is missing"
        assert first["published_at"], "published_at is missing"
        assert isinstance(first["published_at"], datetime), (
            f"published_at should be datetime, got {type(first['published_at'])}"
        )

        # Note: published_at from Plone6 API is timezone-aware, so compare dates only
        min_date_dt = datetime.strptime(min_date, "%Y-%m-%d").date()
        assert first["published_at"].date() >= min_date_dt, (
            f"published_at {first['published_at'].date()} is before min_date {min_date_dt}"
        )

        assert "agency" in first, "agency field is missing"


@pytest.mark.integration
class TestMigratedAgencyUrlsWork:
    """Validates that recently migrated agencies return articles via the full scraper pipeline.

    Objective: prevent config regression (wrong URL or inactive agency after migration).
    Does NOT test scraper behavior — that is covered by TestPlone6APIScraper.
    """

    @pytest.mark.parametrize("agency_key,agency_url", RECENTLY_MIGRATED_AGENCIES.items())
    def test_scrape_news_returns_at_least_one_article(self, agency_key, agency_url):
        """scrape_news() must return at least one article for each recently migrated agency."""
        scraper = Plone6APIScraper(
            base_url=agency_url,
            min_date=(datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d"),
        )

        try:
            articles = scraper.scrape_news()
        except requests.exceptions.RequestException as e:
            pytest.skip(f"Network issue for {agency_key}: {e}")
        except Exception as e:
            pytest.fail(f"{agency_key}: scrape_news() raised unexpected error: {e}")

        assert len(articles) > 0, (
            f"{agency_key}: nenhum artigo encontrado nos últimos 180 dias. "
            f"URL pode estar errada ou agência parou de publicar."
        )
