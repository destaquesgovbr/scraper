import hashlib
import re
import unicodedata


def normalize_text(text: str | None) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def compute_content_hash(title: str, content: str | None) -> str | None:
    norm_title = normalize_text(title)
    norm_content = normalize_text(content)
    if not norm_title and not norm_content:
        return None
    normalized = norm_title + "\n" + norm_content
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
