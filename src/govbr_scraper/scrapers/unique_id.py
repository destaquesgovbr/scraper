"""Generate readable unique IDs for news articles.

Replaces MD5 hashes with human-readable slugs + deterministic suffix.
Format: {slug}_{suffix}  (e.g., "governo-anuncia-programa_a3f2e1")
"""

import hashlib
import re
import unicodedata
from datetime import date


def slugify(text: str, max_length: int = 100) -> str:
    """Convert text to a URL-friendly slug.

    - Decomposes Unicode accents (é → e, ç → c, ã → a)
    - Lowercases
    - Replaces non-alphanumeric with dashes
    - Collapses consecutive dashes
    - Truncates at word boundary (last dash before max_length)
    """
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    if len(text) > max_length:
        truncated = text[:max_length]
        # Cut at last dash to avoid mid-word truncation
        if "-" in truncated:
            truncated = truncated.rsplit("-", 1)[0]
        text = truncated
    return text


def generate_suffix(agency: str, published_at_value, title: str) -> str:
    """Generate a deterministic 6-char hex suffix from article attributes.

    Uses the same hash inputs as the legacy MD5 algorithm (agency + date + title)
    to preserve uniqueness guarantees.
    """
    date_str = (
        published_at_value.isoformat()
        if isinstance(published_at_value, date)
        else str(published_at_value)
    )
    hash_input = f"{agency}_{date_str}_{title}".encode("utf-8")
    return hashlib.md5(hash_input).hexdigest()[:6]


def generate_readable_unique_id(agency: str, published_at_value, title: str) -> str:
    """Generate a readable unique ID in the format: {slug}_{suffix}.

    The slug is derived from the title (max 100 chars, truncated at word boundary).
    The suffix is a 6-char hex hash of agency + date + title for uniqueness.
    Total length never exceeds 120 characters.
    """
    slug = slugify(title)
    suffix = generate_suffix(agency, published_at_value, title)
    if slug:
        return f"{slug}_{suffix}"
    return f"sem-titulo_{suffix}"
