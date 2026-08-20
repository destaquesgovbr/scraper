from pathlib import Path

import pytest

from scripts.validate_site_urls import validate_agency, validate_document, validate_sync


def valid_agency(**overrides: object) -> dict[str, object]:
    agency: dict[str, object] = {
        "url": "https://www.gov.br/mec/pt-br",
        "active": True,
        "scraper_type": "html",
    }
    agency.update(overrides)
    return agency


def test_validate_document_accepts_valid_agency() -> None:
    errors, count = validate_document({"agencies": {"mec": valid_agency()}})

    assert errors == []
    assert count == 1


@pytest.mark.parametrize(
    ("data", "expected"),
    [
        (None, "Configuration must be a mapping"),
        ({}, "'agencies' must be a non-empty mapping"),
        ({"agencies": []}, "'agencies' must be a non-empty mapping"),
        ({"agencies": {}}, "'agencies' must be a non-empty mapping"),
    ],
)
def test_validate_document_rejects_invalid_structure(data: object, expected: str) -> None:
    errors, count = validate_document(data)

    assert expected in errors
    assert count == 0


def test_validate_agency_rejects_non_mapping() -> None:
    assert validate_agency("mec", "invalid") == ["Agency 'mec' must be a mapping"]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"url": 123}, "URL must be a string"),
        ({"url": "http://www.gov.br/mec"}, "URL must use https"),
        ({"url": "https://dados.gov.br/mec"}, "URL must use www.gov.br"),
        (
            {"url": "https://user:pass@www.gov.br/mec"},  # pragma: allowlist secret
            "must not contain credentials",
        ),
        ({"url": "https://www.gov.br:8080/mec"}, "must not use a non-HTTPS port"),
        ({"url": "https://[www.gov.br/mec"}, "has a malformed URL"),
        ({"active": "true"}, "'active' must be boolean"),
        ({"scraper_type": "unknown"}, "invalid scraper_type"),
    ],
)
def test_validate_agency_rejects_invalid_values(
    overrides: dict[str, object], expected: str
) -> None:
    errors = validate_agency("mec", valid_agency(**overrides))

    assert any(expected in error for error in errors)


def test_validate_agency_reports_missing_fields() -> None:
    errors = validate_agency("mec", {})

    assert "Agency 'mec' missing fields: ['active', 'url']" in errors


def test_validate_sync_accepts_identical_files(tmp_path: Path) -> None:
    source = tmp_path / "source.yaml"
    copy = tmp_path / "copy.yaml"
    source.write_text("agencies: {}\n", encoding="utf-8")
    copy.write_text("agencies: {}\n", encoding="utf-8")

    assert validate_sync(source, copy) == []


def test_validate_sync_rejects_different_files(tmp_path: Path) -> None:
    source = tmp_path / "source.yaml"
    copy = tmp_path / "copy.yaml"
    source.write_text("source\n", encoding="utf-8")
    copy.write_text("copy\n", encoding="utf-8")

    errors = validate_sync(source, copy)

    assert len(errors) == 1
    assert "out of sync" in errors[0]


def test_validate_sync_reports_missing_files(tmp_path: Path) -> None:
    errors = validate_sync(tmp_path / "source.yaml", tmp_path / "copy.yaml")

    assert len(errors) == 2
    assert all("not found" in error for error in errors)
