#!/usr/bin/env python3
"""Validate that _ALLOWED_URL_PREFIXES in api.py covers domains from site_urls.yaml and ebc_urls.yaml."""

import ast
import sys
from pathlib import Path

import yaml


def extract_allowed_prefixes() -> set[str]:
    """Extract _ALLOWED_URL_PREFIXES from api.py."""
    api_path = Path("src/govbr_scraper/api.py")

    with open(api_path) as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "_ALLOWED_URL_PREFIXES":
                    if isinstance(node.value, (ast.Tuple, ast.List, ast.Set)):
                        return {
                            elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)
                        }
    return set()


def extract_domains_from_yaml(yaml_path: Path) -> set[str]:
    """Extract unique domains from YAML file."""
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    domains = set()
    for agency in data.get("agencies", {}).values():
        url = agency.get("url", "")
        if url and url.startswith("https://"):
            # Extract base domain (e.g., "https://www.gov.br/")
            parts = url.split("/")
            if len(parts) >= 3:
                domains.add(f"{parts[0]}//{parts[2]}/")
    return domains


def main() -> int:
    """Validate SSRF allowlist coverage."""
    allowed = extract_allowed_prefixes()

    if not allowed:
        print("ERROR: Could not extract _ALLOWED_URL_PREFIXES from api.py")
        return 1

    # Extract domains from both YAML files
    site_urls_path = Path("src/govbr_scraper/scrapers/config/site_urls.yaml")
    ebc_urls_path = Path("src/govbr_scraper/scrapers/config/ebc_urls.yaml")

    site_domains = extract_domains_from_yaml(site_urls_path)
    ebc_domains = extract_domains_from_yaml(ebc_urls_path)
    all_domains = site_domains | ebc_domains

    # Check coverage
    uncovered = []
    for domain in all_domains:
        if not any(domain.startswith(prefix) for prefix in allowed):
            uncovered.append(domain)

    # Mandatory domains (from CLAUDE.md)
    # Note: For storage.googleapis.com, we allow more specific paths (e.g., /destaquesgovbr-thumbnails/)
    mandatory = {
        "https://www.gov.br/",
        "https://agenciabrasil.ebc.com.br/",
        "https://tvbrasil.ebc.com.br/",
        "https://storage.googleapis.com/",
    }

    # Check if mandatory domains are covered (allowing more specific prefixes)
    # Note: Logic is inverted - we check if ANY allowlist prefix STARTS WITH the mandatory domain,
    # not if the domain starts with a prefix. This allows:
    #   - mandatory: "https://storage.googleapis.com/"
    #   - allowlist: "https://storage.googleapis.com/destaquesgovbr-thumbnails/" ✅ valid
    # This validates that the BROADER domain is represented, even if allowlist is more specific.
    missing_mandatory = set()
    for domain in mandatory:
        # Check if domain or any more specific prefix is in allowlist
        if not any(prefix.startswith(domain) for prefix in allowed):
            missing_mandatory.add(domain)

    if uncovered or missing_mandatory:
        if uncovered:
            print("ERROR: Domains in YAML files not covered by _ALLOWED_URL_PREFIXES:")
            for domain in sorted(uncovered):
                print(f"  - {domain}")

        if missing_mandatory:
            print("ERROR: Mandatory domains missing from _ALLOWED_URL_PREFIXES:")
            for domain in sorted(missing_mandatory):
                print(f"  - {domain}")

        print("\nCurrent _ALLOWED_URL_PREFIXES:")
        for prefix in sorted(allowed):
            print(f"  - {prefix}")

        return 1

    print(f"✓ All {len(all_domains)} domains covered by {len(allowed)} allowlist prefixes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
