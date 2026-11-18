"""Tests for Scraper API endpoints — HTTP status codes and response structure."""

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from govbr_scraper.api import app

client = TestClient(app)


# =============================================================================
# /health
# =============================================================================


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


# =============================================================================
# /scrape/agencies — HTTP status codes
# =============================================================================


AGENCIES_PAYLOAD = {"start_date": "2025-01-01", "agencies": ["mec"], "sequential": True}


def _mock_run_scraper(
    *, articles_scraped=0, articles_saved=0, articles_new=0, agencies_processed=None, errors=None
):
    """Helper to create a mock ScrapeManager.run_scraper return value."""
    return {
        "articles_scraped": articles_scraped,
        "articles_saved": articles_saved,
        "articles_new": articles_new,
        "agencies_processed": agencies_processed or [],
        "errors": errors or [],
    }


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_completed_returns_200(mock_storage_cls, mock_manager_cls):
    """Successful scraping should return HTTP 200."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        articles_scraped=10, articles_saved=8, articles_new=3, agencies_processed=["mec"]
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "completed"
    assert data["articles_scraped"] == 10
    assert data["articles_saved"] == 8
    assert data["articles_new"] == 3
    assert data["agencies_processed"] == ["mec"]
    assert data["errors"] == []


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_partial_returns_207(mock_storage_cls, mock_manager_cls):
    """Partial failure (some agencies OK, some failed) should return HTTP 207."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        articles_scraped=5,
        articles_saved=5,
        agencies_processed=["mec"],
        errors=[{"agency": "mds", "error": "Connection timeout"}],
    )
    mock_manager_cls.return_value = mock_manager

    payload = {**AGENCIES_PAYLOAD, "agencies": ["mec", "mds"]}
    response = client.post("/scrape/agencies", json=payload)

    assert response.status_code == 207
    data = response.json()
    assert data["status"] == "partial"
    assert len(data["errors"]) == 1
    assert data["errors"][0]["agency"] == "mds"


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_failed_returns_500(mock_storage_cls, mock_manager_cls):
    """Total failure (all agencies failed) should return HTTP 500."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        errors=[{"agency": "mec", "error": "Site down"}],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.status_code == 500
    data = response.json()
    assert data["status"] == "failed"
    assert data["message"] == "All agencies failed"


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_exception_returns_500(mock_storage_cls, mock_manager_cls):
    """Unhandled exception (e.g. storage init failure) should return HTTP 500."""
    mock_storage_cls.side_effect = RuntimeError("DB connection failed")

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.status_code == 500
    assert "DB connection failed" in response.json()["detail"]


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_no_articles_returns_200(mock_storage_cls, mock_manager_cls):
    """No articles found (but no errors) is still a success."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        agencies_processed=["mec"],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "completed"
    assert data["articles_scraped"] == 0


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_end_date_defaults_to_start(mock_storage_cls, mock_manager_cls):
    """When end_date is omitted, it should default to start_date."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(agencies_processed=["mec"])
    mock_manager_cls.return_value = mock_manager

    payload = {"start_date": "2025-06-15", "agencies": ["mec"]}
    response = client.post("/scrape/agencies", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["start_date"] == "2025-06-15"
    assert data["end_date"] == "2025-06-15"


# =============================================================================
# /scrape/ebc — HTTP status codes
# =============================================================================


EBC_PAYLOAD = {"start_date": "2025-01-01", "sequential": True}


@patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_completed_returns_200(mock_storage_cls, mock_manager_cls):
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        articles_scraped=20,
        articles_saved=18,
        articles_new=4,
        agencies_processed=["agencia_brasil", "tvbrasil"],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/ebc", json=EBC_PAYLOAD)

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["articles_new"] == 4


@patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_partial_returns_207(mock_storage_cls, mock_manager_cls):
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        articles_scraped=10,
        articles_saved=10,
        agencies_processed=["agencia_brasil"],
        errors=[{"agency": "tvbrasil", "error": "Timeout"}],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/ebc", json=EBC_PAYLOAD)

    assert response.status_code == 207
    assert response.json()["status"] == "partial"


@patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_failed_returns_500(mock_storage_cls, mock_manager_cls):
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        errors=[{"agency": "agencia_brasil", "error": "DNS resolution failed"}],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/ebc", json=EBC_PAYLOAD)

    assert response.status_code == 500
    data = response.json()
    assert data["status"] == "failed"
    assert "DNS resolution failed" in data["message"]


@patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_exception_returns_500(mock_storage_cls, mock_manager_cls):
    """Unhandled exception during EBC scraping should return HTTP 500."""
    mock_storage_cls.side_effect = RuntimeError("DB connection failed")

    response = client.post("/scrape/ebc", json=EBC_PAYLOAD)

    assert response.status_code == 500
    assert "DB connection failed" in response.json()["detail"]


# =============================================================================
# Contrato de errors[]: category e retryable (DAG decide se repete)
# =============================================================================


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_errors_carry_category_and_retryable(mock_storage_cls, mock_manager_cls):
    """O status HTTP não muda (500 se todas falharam); o corpo diz se vale repetir."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        errors=[
            {
                "agency": "mec",
                "error": "No articles found on first page of mec",
                "category": "html_changed",
                "retryable": False,
            }
        ],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.status_code == 500
    assert response.json()["errors"] == [
        {
            "agency": "mec",
            "error": "No articles found on first page of mec",
            "category": "html_changed",
            "retryable": False,
        }
    ]


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_error_without_category_defaults_to_retryable(
    mock_storage_cls, mock_manager_cls
):
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        errors=[{"agency": "mec", "error": "Site down"}],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    (error,) = response.json()["errors"]
    assert error["category"] is None
    assert error["retryable"] is True


@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_unknown_agency_is_permanent(mock_storage_cls):
    """ScrapeManager real com o site_urls.yaml real: agência fora do YAML."""
    payload = {**AGENCIES_PAYLOAD, "agencies": ["nao_existe_xyz"]}
    response = client.post("/scrape/agencies", json=payload)

    assert response.status_code == 500  # inalterado: compatível com as DAGs antigas
    data = response.json()
    assert data["status"] == "failed"
    assert data["articles_new"] == 0
    (error,) = data["errors"]
    assert error["agency"] == "nao_existe_xyz"
    assert error["category"] == "unknown_agency"
    assert error["retryable"] is False


@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_unknown_agency_is_permanent(mock_storage_cls):
    response = client.post("/scrape/ebc", json={**EBC_PAYLOAD, "agencies": ["memoria-ebc"]})

    assert response.status_code == 500
    (error,) = response.json()["errors"]
    assert error["category"] == "unknown_agency"
    assert error["retryable"] is False


@patch("govbr_scraper.scrapers.ebc_scrape_manager.EBCScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_ebc_partial_keeps_207_with_categories(mock_storage_cls, mock_manager_cls):
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = _mock_run_scraper(
        articles_scraped=10,
        articles_saved=10,
        articles_new=2,
        agencies_processed=["agencia_brasil"],
        errors=[
            {
                "agency": "tvbrasil",
                "error": "Read timed out",
                "category": "network_error",
                "retryable": True,
            }
        ],
    )
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/ebc", json=EBC_PAYLOAD)

    assert response.status_code == 207
    data = response.json()
    assert data["articles_new"] == 2
    assert data["errors"][0]["retryable"] is True
    assert data["errors"][0]["category"] == "network_error"


@patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
@patch("govbr_scraper.storage.StorageAdapter", autospec=True)
def test_scrape_agencies_old_manager_metrics_without_articles_new(
    mock_storage_cls, mock_manager_cls
):
    """articles_new ausente nas métricas vira 0 (resposta sempre tem o campo)."""
    mock_manager = MagicMock()
    mock_manager.run_scraper.return_value = {
        "articles_scraped": 1,
        "articles_saved": 1,
        "agencies_processed": ["mec"],
        "errors": [],
    }
    mock_manager_cls.return_value = mock_manager

    response = client.post("/scrape/agencies", json=AGENCIES_PAYLOAD)

    assert response.json()["articles_new"] == 0


# =============================================================================
# Payload Validation Tests
# =============================================================================


class TestPayloadValidation:
    """Tests for request payload validation."""

    def test_missing_start_date_returns_422(self):
        """Missing required start_date field should return HTTP 422."""
        response = client.post("/scrape/agencies", json={"agencies": ["mec"]})

        assert response.status_code == 422
        detail = response.json()["detail"]
        assert any("start_date" in str(err).lower() for err in detail)

    @patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
    @patch("govbr_scraper.storage.StorageAdapter", autospec=True)
    def test_invalid_start_date_format_returns_500(self, mock_storage_cls, mock_manager_cls):
        """Invalid date format passes Pydantic (start_date is str) but fails in the scraper, returning HTTP 500."""
        mock_manager = MagicMock()
        mock_manager.run_scraper.side_effect = ValueError("invalid date format: not-a-date")
        mock_manager_cls.return_value = mock_manager

        response = client.post(
            "/scrape/agencies", json={"start_date": "not-a-date", "agencies": ["mec"]}
        )

        assert response.status_code == 500

    def test_invalid_json_returns_422(self):
        """Malformed JSON should return HTTP 422."""
        response = client.post(
            "/scrape/agencies",
            data="this is not json",
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 422

    def test_empty_payload_returns_422(self):
        """Empty payload should return HTTP 422."""
        response = client.post("/scrape/agencies", json={})

        assert response.status_code == 422

    def test_null_start_date_returns_422(self):
        """Null start_date should return HTTP 422."""
        response = client.post("/scrape/agencies", json={"start_date": None, "agencies": ["mec"]})

        assert response.status_code == 422

    def test_agencies_as_string_instead_of_list_returns_422(self):
        """agencies as string instead of list should return HTTP 422."""
        response = client.post(
            "/scrape/agencies",
            json={
                "start_date": "2025-01-01",
                "agencies": "mec",  # Should be ["mec"]
            },
        )

        assert response.status_code == 422

    def test_invalid_field_type_returns_422(self):
        """Invalid field type that cannot be coerced should return HTTP 422."""
        response = client.post(
            "/scrape/agencies",
            json={
                "start_date": "2025-01-01",
                "agencies": ["mec"],
                "sequential": {"invalid": "object"},  # Invalid type that cannot be coerced to bool
            },
        )

        assert response.status_code == 422

    @patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
    @patch("govbr_scraper.storage.StorageAdapter", autospec=True)
    def test_extra_fields_are_ignored(self, mock_storage_cls, mock_manager_cls):
        """Extra fields in payload should be ignored gracefully."""
        mock_manager = MagicMock()
        mock_manager.run_scraper.return_value = _mock_run_scraper(agencies_processed=["mec"])
        mock_manager_cls.return_value = mock_manager

        response = client.post(
            "/scrape/agencies",
            json={
                "start_date": "2025-01-01",
                "agencies": ["mec"],
                "extra_field": "should_be_ignored",
                "another_extra": 12345,
            },
        )

        # Should succeed despite extra fields
        assert response.status_code == 200

    @patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
    @patch("govbr_scraper.storage.StorageAdapter", autospec=True)
    def test_future_start_date_is_accepted(self, mock_storage_cls, mock_manager_cls):
        """Future dates should be accepted (validation is up to business logic)."""
        mock_manager = MagicMock()
        mock_manager.run_scraper.return_value = _mock_run_scraper(
            articles_scraped=0, articles_saved=0, agencies_processed=["mec"]
        )
        mock_manager_cls.return_value = mock_manager

        response = client.post(
            "/scrape/agencies", json={"start_date": "2099-12-31", "agencies": ["mec"]}
        )

        # Should not fail at validation layer
        assert response.status_code == 200

    @patch("govbr_scraper.scrapers.scrape_manager.ScrapeManager", autospec=True)
    @patch("govbr_scraper.storage.StorageAdapter", autospec=True)
    def test_end_date_before_start_date_accepted_at_api_layer(
        self, mock_storage_cls, mock_manager_cls
    ):
        """end_date before start_date accepted at API layer (logic validation elsewhere)."""
        mock_manager = MagicMock()
        mock_manager.run_scraper.return_value = _mock_run_scraper(
            articles_scraped=0, articles_saved=0, agencies_processed=["mec"]
        )
        mock_manager_cls.return_value = mock_manager

        response = client.post(
            "/scrape/agencies",
            json={
                "start_date": "2025-12-31",
                "end_date": "2025-01-01",  # Before start_date
                "agencies": ["mec"],
            },
        )

        # API layer doesn't validate date logic, only types
        assert response.status_code == 200
