#!/usr/bin/env python3
"""Validate site_urls.yaml schema and URL format."""

import sys
from pathlib import Path
from typing import Any

import yaml

REQUIRED_FIELDS = {"url", "active"}
VALID_SCRAPER_TYPES = {"html", "plone6_api"}


def validate_agency(key: str, agency: dict[str, Any]) -> list[str]:
    """Validate a single agency definition."""
    errors = []

    # Check required fields
    missing = REQUIRED_FIELDS - set(agency.keys())
    if missing:
        errors.append(f"Agency '{key}' missing fields: {missing}")

    # Validate URL format
    if "url" in agency:
        url = agency["url"]
        if not url.startswith("https://"):
            errors.append(f"Agency '{key}' URL must start with https://: {url}")
        # Currently all 162 agencies use www.gov.br subdomain.
        # If future agencies use other subdomains (dados.gov.br, api.gov.br),
        # relax this check to pattern: https://[subdomain].gov.br/
        if not url.startswith("https://www.gov.br/"):
            errors.append(f"Agency '{key}' URL must be a gov.br domain: {url}")

    # Validate scraper_type if present
    if "scraper_type" in agency:
        if agency["scraper_type"] not in VALID_SCRAPER_TYPES:
            errors.append(f"Agency '{key}' has invalid scraper_type: {agency['scraper_type']}")

    # Validate active field type
    if "active" in agency and not isinstance(agency["active"], bool):
        errors.append(f"Agency '{key}' 'active' must be boolean, got: {type(agency['active'])}")

    return errors


def main() -> int:
    """Validate site_urls.yaml in src/."""
    file_path = Path("src/govbr_scraper/scrapers/config/site_urls.yaml")

    if not file_path.exists():
        print(f"ERROR: {file_path} not found")
        return 1

    try:
        with open(file_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        print(f"ERROR: Invalid YAML syntax: {e}")
        return 1

    if data is None or "agencies" not in data:
        print("ERROR: Missing 'agencies' key")
        return 1

    errors = []
    for key, agency in data["agencies"].items():
        errors.extend(validate_agency(key, agency))

    if errors:
        print("\n".join(errors))
        return 1

    print(f"✓ Validated {len(data['agencies'])} agencies")
    return 0


if __name__ == "__main__":
    sys.exit(main())
