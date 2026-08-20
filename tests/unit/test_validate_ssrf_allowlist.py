from pathlib import Path

import pytest

from scripts.validate_ssrf_allowlist import (
    REQUIRED_ASSET_PREFIXES,
    extract_allowed_prefixes,
    extract_domains_from_yaml,
    validate_coverage,
    validate_prefix,
)


@pytest.mark.parametrize(("opening", "closing"), [("(", ")"), ("[", "]"), ("{", "}")])
def test_extract_allowed_prefixes_accepts_literal_collections(
    tmp_path: Path, opening: str, closing: str
) -> None:
    api_path = tmp_path / "api.py"
    api_path.write_text(
        f'_ALLOWED_URL_PREFIXES = {opening}"https://www.gov.br/",{closing}\n',
        encoding="utf-8",
    )

    assert extract_allowed_prefixes(api_path) == {"https://www.gov.br/"}


def test_extract_allowed_prefixes_accepts_annotated_assignment(tmp_path: Path) -> None:
    api_path = tmp_path / "api.py"
    api_path.write_text(
        '_ALLOWED_URL_PREFIXES: tuple[str, ...] = ("https://www.gov.br/",)\n',
        encoding="utf-8",
    )

    assert extract_allowed_prefixes(api_path) == {"https://www.gov.br/"}


@pytest.mark.parametrize(
    "source",
    [
        "_ALLOWED_URL_PREFIXES = build_prefixes()\n",
        '_ALLOWED_URL_PREFIXES = ("https://www.gov.br/", 42)\n',
        "_ALLOWED_URL_PREFIXES = ()\n",
    ],
)
def test_extract_allowed_prefixes_rejects_invalid_values(tmp_path: Path, source: str) -> None:
    api_path = tmp_path / "api.py"
    api_path.write_text(source, encoding="utf-8")

    with pytest.raises(ValueError):
        extract_allowed_prefixes(api_path)


def test_extract_domains_from_yaml_returns_https_origins(tmp_path: Path) -> None:
    yaml_path = tmp_path / "urls.yaml"
    yaml_path.write_text(
        "agencies:\n  mec:\n    url: https://www.gov.br/mec/pt-br\n",
        encoding="utf-8",
    )

    assert extract_domains_from_yaml(yaml_path) == {"https://www.gov.br/"}


@pytest.mark.parametrize(
    "contents",
    [
        "[]\n",
        "agencies: []\n",
        "agencies:\n  mec: invalid\n",
        "agencies:\n  mec:\n    url: http://www.gov.br/mec\n",
        "agencies:\n  mec:\n    url: https://www.gov.br:8080/mec\n",
    ],
)
def test_extract_domains_from_yaml_rejects_invalid_configuration(
    tmp_path: Path, contents: str
) -> None:
    yaml_path = tmp_path / "urls.yaml"
    yaml_path.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError):
        extract_domains_from_yaml(yaml_path)


@pytest.mark.parametrize(
    "prefix",
    [
        "https://",
        "http://www.gov.br/",
        "https://user:pass@www.gov.br/",  # pragma: allowlist secret
        "https://www.gov.br:8080/",
        "https://www.gov.br",
        "https://www.gov.br/?page=1",
    ],
)
def test_validate_prefix_rejects_unsafe_or_noncanonical_prefix(prefix: str) -> None:
    assert validate_prefix(prefix) is not None


def test_validate_coverage_accepts_configured_domains_and_assets() -> None:
    allowed = REQUIRED_ASSET_PREFIXES | {"https://www.gov.br/"}

    assert validate_coverage(allowed, {"https://www.gov.br/"}) == []


def test_validate_coverage_reports_uncovered_domain_and_missing_assets() -> None:
    errors = validate_coverage({"https://www.gov.br/"}, {"https://example.gov.br/"})

    assert any("Configured domain is not covered" in error for error in errors)
    assert sum("Required asset prefix is missing" in error for error in errors) == 3
