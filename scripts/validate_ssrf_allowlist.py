#!/usr/bin/env python3
"""Validate that the API SSRF allowlist covers configured URL domains."""

import ast
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
API_PATH = REPO_ROOT / "src/govbr_scraper/api.py"
SITE_URLS_PATH = REPO_ROOT / "src/govbr_scraper/scrapers/config/site_urls.yaml"
EBC_URLS_PATH = REPO_ROOT / "src/govbr_scraper/scrapers/config/ebc_urls.yaml"

# These asset locations are valid API inputs but are not agency base URLs.
REQUIRED_ASSET_PREFIXES = {
    "https://imagens.ebc.com.br/",
    "https://live.staticflickr.com/",
    "https://storage.googleapis.com/destaquesgovbr-thumbnails/",
}


def extract_allowed_prefixes(api_path: Path) -> set[str]:
    """Extract a literal _ALLOWED_URL_PREFIXES collection from Python source."""
    tree = ast.parse(api_path.read_text(encoding="utf-8"), filename=str(api_path))

    for node in ast.walk(tree):
        value: ast.expr | None = None
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "_ALLOWED_URL_PREFIXES"
            for target in node.targets
        ):
            value = node.value
        elif (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "_ALLOWED_URL_PREFIXES"
        ):
            value = node.value

        if value is None:
            continue

        try:
            prefixes = ast.literal_eval(value)
        except (ValueError, SyntaxError) as exc:
            raise ValueError("_ALLOWED_URL_PREFIXES must contain only literals") from exc

        if not isinstance(prefixes, tuple | list | set) or not prefixes:
            raise ValueError("_ALLOWED_URL_PREFIXES must be a non-empty tuple, list, or set")
        if not all(isinstance(prefix, str) for prefix in prefixes):
            raise ValueError("_ALLOWED_URL_PREFIXES entries must all be strings")
        return set(prefixes)

    raise ValueError("Could not find _ALLOWED_URL_PREFIXES")


def validate_prefix(prefix: str) -> str | None:
    """Return an error when a prefix is too broad or is not a canonical HTTPS URL."""
    try:
        parsed = urlsplit(prefix)
        port = parsed.port
    except ValueError:
        return f"Malformed allowlist prefix: {prefix}"

    if parsed.scheme != "https" or not parsed.hostname:
        return f"Allowlist prefix must be an absolute HTTPS URL: {prefix}"
    if parsed.username or parsed.password:
        return f"Allowlist prefix must not contain credentials: {prefix}"
    if port not in (None, 443):
        return f"Allowlist prefix must not use a non-HTTPS port: {prefix}"
    if not prefix.endswith("/") or parsed.query or parsed.fragment:
        return f"Allowlist prefix must end with '/' and contain no query or fragment: {prefix}"
    return None


def extract_domains_from_yaml(yaml_path: Path) -> set[str]:
    """Extract canonical HTTPS origins from an agency configuration."""
    data: Any = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    if not isinstance(data, Mapping) or not isinstance(data.get("agencies"), Mapping):
        raise ValueError(f"{yaml_path} must contain an 'agencies' mapping")

    domains = set()
    for key, agency in data["agencies"].items():
        if not isinstance(agency, Mapping) or not isinstance(agency.get("url"), str):
            raise ValueError(f"Agency '{key}' in {yaml_path} must have a string URL")

        url = agency["url"]
        try:
            parsed = urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"Agency '{key}' has a malformed URL: {url}") from exc

        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError(f"Agency '{key}' must have an absolute HTTPS URL: {url}")
        if port not in (None, 443):
            raise ValueError(f"Agency '{key}' URL must not use a non-HTTPS port: {url}")

        domains.add(f"https://{parsed.hostname}/")
    return domains


def validate_coverage(allowed: set[str], domains: set[str]) -> list[str]:
    """Validate prefix safety and coverage of configured domains and asset URLs."""
    errors = [error for prefix in sorted(allowed) if (error := validate_prefix(prefix))]

    uncovered = {
        domain for domain in domains if not any(domain.startswith(prefix) for prefix in allowed)
    }
    errors.extend(f"Configured domain is not covered: {domain}" for domain in sorted(uncovered))

    missing_assets = REQUIRED_ASSET_PREFIXES - allowed
    errors.extend(
        f"Required asset prefix is missing: {prefix}" for prefix in sorted(missing_assets)
    )
    return errors


def main() -> int:
    """Validate SSRF allowlist safety and coverage."""
    try:
        allowed = extract_allowed_prefixes(API_PATH)
        domains = extract_domains_from_yaml(SITE_URLS_PATH) | extract_domains_from_yaml(
            EBC_URLS_PATH
        )
        errors = validate_coverage(allowed, domains)
    except (OSError, SyntaxError, yaml.YAMLError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1

    if errors:
        print("\n".join(f"ERROR: {error}" for error in errors))
        return 1

    print(f"Validated {len(domains)} domains against {len(allowed)} safe allowlist prefixes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
