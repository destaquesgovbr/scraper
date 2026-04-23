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


def compute_content_hash(title: str, content: str | None) -> str:
    normalized = normalize_text(title) + "\n" + normalize_text(content)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]
