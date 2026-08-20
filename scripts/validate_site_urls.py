#!/usr/bin/env python3
"""Validate the site URL configuration and its Airflow copy."""

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_PATH = REPO_ROOT / "src/govbr_scraper/scrapers/config/site_urls.yaml"
DAGS_PATH = REPO_ROOT / "dags/config/site_urls.yaml"
REQUIRED_FIELDS = {"url", "active"}
VALID_SCRAPER_TYPES = {"html", "plone6_api"}


def validate_agency(key: str, agency: object) -> list[str]:
    """Validate a single agency definition."""
    if not isinstance(agency, Mapping):
        return [f"Agency '{key}' must be a mapping"]

    errors = []
    missing = REQUIRED_FIELDS - set(agency)
    if missing:
        errors.append(f"Agency '{key}' missing fields: {sorted(missing)}")

    if "url" in agency:
        url = agency["url"]
        if not isinstance(url, str):
            errors.append(f"Agency '{key}' URL must be a string")
        else:
            try:
                parsed = urlsplit(url)
                port = parsed.port
            except ValueError:
                errors.append(f"Agency '{key}' has a malformed URL: {url}")
            else:
                if parsed.scheme != "https":
                    errors.append(f"Agency '{key}' URL must use https: {url}")
                if parsed.hostname != "www.gov.br":
                    errors.append(f"Agency '{key}' URL must use www.gov.br: {url}")
                if parsed.username or parsed.password:
                    errors.append(f"Agency '{key}' URL must not contain credentials: {url}")
                if port not in (None, 443):
                    errors.append(f"Agency '{key}' URL must not use a non-HTTPS port: {url}")

    if "scraper_type" in agency and agency["scraper_type"] not in VALID_SCRAPER_TYPES:
        errors.append(f"Agency '{key}' has invalid scraper_type: {agency['scraper_type']}")

    if "active" in agency and not isinstance(agency["active"], bool):
        value_type = type(agency["active"]).__name__
        errors.append(f"Agency '{key}' 'active' must be boolean, got: {value_type}")

    return errors


def validate_document(data: Any) -> tuple[list[str], int]:
    """Validate the top-level configuration and all agencies."""
    if not isinstance(data, Mapping):
        return ["Configuration must be a mapping"], 0

    agencies = data.get("agencies")
    if not isinstance(agencies, Mapping) or not agencies:
        return ["'agencies' must be a non-empty mapping"], 0

    errors = []
    for key, agency in agencies.items():
        if not isinstance(key, str) or not key:
            errors.append(f"Agency key must be a non-empty string: {key!r}")
            continue
        errors.extend(validate_agency(key, agency))

    return errors, len(agencies)


def validate_sync(source_path: Path, copy_path: Path) -> list[str]:
    """Ensure the source configuration and Airflow copy are byte-identical."""
    missing = [str(path) for path in (source_path, copy_path) if not path.is_file()]
    if missing:
        return [f"Configuration file not found: {path}" for path in missing]

    if source_path.read_bytes() != copy_path.read_bytes():
        return [f"site_urls.yaml files are out of sync; copy {source_path} to {copy_path}"]
    return []


def main() -> int:
    """Validate the source configuration and its Airflow copy."""
    errors = validate_sync(SOURCE_PATH, DAGS_PATH)

    try:
        data = yaml.safe_load(SOURCE_PATH.read_text(encoding="utf-8"))
    except OSError as exc:
        errors.append(f"Could not read {SOURCE_PATH}: {exc}")
        data = None
    except yaml.YAMLError as exc:
        errors.append(f"Invalid YAML syntax in {SOURCE_PATH}: {exc}")
        data = None

    document_errors, agency_count = validate_document(data)
    errors.extend(document_errors)

    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors))
        return 1

    print(f"Validated {agency_count} agencies and synchronized Airflow configuration")
    return 0


if __name__ == "__main__":
    sys.exit(main())
