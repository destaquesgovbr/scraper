"""
Shared utilities for loading and processing agency YAML configuration files.
"""
import logging
import os
from typing import Any, Dict

import yaml


def get_config_dir(module_file: str) -> str:
    """
    Get the config directory path relative to a module file.

    :param module_file: The __file__ of the calling module.
    :return: Absolute path to the config directory.
    """
    return os.path.join(os.path.dirname(os.path.abspath(module_file)), "config")


def load_urls_from_yaml(
    config_dir: str, file_name: str, agency: str = None
) -> Dict[str, str]:
    """
    Load URLs from a YAML file.

    Expected format:
        agencies:
          agency_key:
            url: str
            active: bool  # optional, defaults to True
            disabled_reason: str  # optional
            disabled_date: str  # optional

    :param config_dir: Directory containing the YAML file.
    :param file_name: The name of the YAML file.
    :param agency: Specific agency key to filter URLs. If None, load all active URLs.
    :return: A dict mapping agency_name to URL.
    :raises ValueError: If agency not found or is inactive.
    """
    file_path = os.path.join(config_dir, file_name)

    with open(file_path, "r") as f:
        agencies = yaml.safe_load(f)["agencies"]

    if agency:
        if agency not in agencies:
            raise ValueError(f"Agency '{agency}' not found in the YAML file.")
        agency_data = agencies[agency]
        if is_agency_inactive(agency, agency_data):
            raise ValueError(f"Agency '{agency}' is inactive.")
        return {agency: extract_url(agency_data)}

    # Load all active agencies
    agency_urls = {}
    inactive_agencies = []

    for agency_key, agency_data in agencies.items():
        if is_agency_inactive(agency_key, agency_data):
            inactive_agencies.append(agency_key)
            continue
        agency_urls[agency_key] = extract_url(agency_data)

    if inactive_agencies:
        logging.info(
            f"Filtered {len(inactive_agencies)} inactive agencies: "
            f"{', '.join(sorted(inactive_agencies))}"
        )

    return agency_urls


def extract_url(agency_data: Dict[str, Any]) -> str:
    """
    Extract URL from agency data.

    :param agency_data: Dict with 'url' key.
    :return: The URL string.
    """
    return str(agency_data["url"])


def is_agency_inactive(agency_key: str, agency_data: Dict[str, Any]) -> bool:
    """
    Check if agency is inactive.

    :param agency_key: Agency identifier for logging.
    :param agency_data: Dict with optional 'active' key.
    :return: True if agency should be skipped.
    """
    is_active = agency_data.get("active", True)

    if not is_active:
        reason = agency_data.get("disabled_reason", "No reason provided")
        logging.debug(f"Skipping inactive agency '{agency_key}': {reason}")

    return not is_active
