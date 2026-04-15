"""
Unit tests for StorageAdapter data conversion methods.

Tests cover:
1. _convert_to_news_insert() - OrderedDict to NewsInsert conversion
2. _parse_datetime() - Datetime parsing from various formats
3. _resolve_theme_id() - Theme code to ID resolution
"""

from collections import OrderedDict
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from govbr_scraper.models.news import Agency, Theme
from govbr_scraper.storage.storage_adapter import StorageAdapter


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_postgres():
    """Mock PostgresManager with agencies and themes cache."""
    pg = MagicMock()
    pg._agencies_by_key = {
        "mec": Agency(id=1, key="mec", name="Ministério da Educação"),
        "mds": Agency(id=2, key="mds", name="Ministério do Desenvolvimento Social"),
    }
    pg._themes_by_code = {
        "EDU": Theme(id=10, code="EDU", label="Educação", level=1),
        "EDU.BASICA": Theme(id=11, code="EDU.BASICA", label="Educação Básica", level=2),
        "SAU": Theme(id=20, code="SAU", label="Saúde", level=1),
    }
    return pg


@pytest.fixture
def adapter(mock_postgres):
    """StorageAdapter with mocked PostgresManager."""
    adapter = StorageAdapter(postgres_manager=mock_postgres)
    return adapter


# =============================================================================
# Tests for _convert_to_news_insert()
# =============================================================================


class TestConvertToNewsInsert:
    """Tests for OrderedDict to NewsInsert conversion."""

    def test_convert_valid_record(self, adapter):
        """Valid record should be converted to NewsInsert."""
        data = OrderedDict({
            "unique_id": ["mec-2026-01-15-noticia"],
            "title": ["Nova política educacional"],
            "url": ["https://www.gov.br/mec/noticia"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec"],
            "content": ["Conteúdo da notícia"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].unique_id == "mec-2026-01-15-noticia"
        assert result[0].title == "Nova política educacional"
        assert result[0].agency_id == 1
        assert result[0].agency_key == "mec"
        assert result[0].agency_name == "Ministério da Educação"

    def test_convert_skips_missing_published_at(self, adapter):
        """Record without published_at should be skipped."""
        data = OrderedDict({
            "unique_id": ["test-1", "test-2"],
            "title": ["Title 1", "Title 2"],
            "url": ["http://url1.com", "http://url2.com"],
            "published_at": [None, datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec", "mec"],
            "content": ["Content 1", "Content 2"],
        })

        result = adapter._convert_to_news_insert(data)

        # Only second record should be included
        assert len(result) == 1
        assert result[0].unique_id == "test-2"

    def test_convert_skips_unknown_agency(self, adapter):
        """Record with unknown agency should be skipped."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["unknown_agency"],
            "content": ["Content"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 0

    def test_convert_parses_string_datetime(self, adapter):
        """ISO string datetime should be parsed."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": ["2026-01-15T14:30:00+00:00"],
            "agency": ["mec"],
            "content": ["Content"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].published_at == datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)

    def test_convert_handles_string_with_z_suffix(self, adapter):
        """ISO string with Z suffix should be parsed."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": ["2026-01-15T14:30:00Z"],
            "agency": ["mec"],
            "content": ["Content"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].published_at == datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)

    def test_convert_resolves_theme_ids(self, adapter):
        """Theme codes should be resolved to IDs."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec"],
            "content": ["Content"],
            "theme_1_level_1_code": ["EDU"],
            "theme_1_level_2_code": ["EDU.BASICA"],
            "most_specific_theme_code": ["EDU.BASICA"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].theme_l1_id == 10
        assert result[0].theme_l2_id == 11
        assert result[0].most_specific_theme_id == 11

    def test_convert_handles_missing_theme_codes(self, adapter):
        """Missing theme codes should result in None IDs."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec"],
            "content": ["Content"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].theme_l1_id is None
        assert result[0].theme_l2_id is None
        assert result[0].most_specific_theme_id is None

    def test_convert_handles_optional_fields(self, adapter):
        """Optional fields should be handled gracefully."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec"],
            "content": ["Content"],
            "image": ["https://img.com/photo.jpg"],
            "video_url": ["https://video.com/v123"],
            "category": ["Educação"],
            "tags": [["educacao", "ensino"]],
            "editorial_lead": ["Especial"],
            "subtitle": ["Subtítulo da notícia"],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].image_url == "https://img.com/photo.jpg"
        assert result[0].video_url == "https://video.com/v123"
        assert result[0].category == "Educação"
        assert result[0].tags == ["educacao", "ensino"]
        assert result[0].editorial_lead == "Especial"
        assert result[0].subtitle == "Subtítulo da notícia"

    def test_convert_handles_empty_tags_as_none(self, adapter):
        """Empty or None tags should be converted to empty list."""
        data = OrderedDict({
            "unique_id": ["test-1"],
            "title": ["Title"],
            "url": ["http://url.com"],
            "published_at": [datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)],
            "agency": ["mec"],
            "content": ["Content"],
            "tags": [None],
        })

        result = adapter._convert_to_news_insert(data)

        assert len(result) == 1
        assert result[0].tags == []


# =============================================================================
# Tests for _resolve_theme_id()
# =============================================================================


class TestResolveThemeId:
    """Tests for theme code to ID resolution."""

    def test_resolve_theme_id_found(self, adapter):
        """Existing theme code should return ID."""
        result = adapter._resolve_theme_id("EDU")
        assert result == 10

    def test_resolve_theme_id_level_2(self, adapter):
        """Level 2 theme code should be resolved."""
        result = adapter._resolve_theme_id("EDU.BASICA")
        assert result == 11

    def test_resolve_theme_id_not_found(self, adapter):
        """Unknown theme code should return None."""
        result = adapter._resolve_theme_id("UNKNOWN")
        assert result is None

    def test_resolve_theme_id_none_input(self, adapter):
        """None input should return None."""
        result = adapter._resolve_theme_id(None)
        assert result is None

    def test_resolve_theme_id_empty_string(self, adapter):
        """Empty string should return None."""
        result = adapter._resolve_theme_id("")
        assert result is None


# =============================================================================
# Tests for _parse_datetime()
# =============================================================================


class TestParseDatetime:
    """Tests for datetime parsing from various formats."""

    def test_parse_datetime_from_string(self, adapter):
        """ISO format string should be parsed."""
        result = adapter._parse_datetime("2026-01-15T14:30:00+00:00")
        assert result == datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)

    def test_parse_datetime_from_string_with_z(self, adapter):
        """ISO format string with Z should be parsed."""
        result = adapter._parse_datetime("2026-01-15T14:30:00Z")
        assert result == datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)

    def test_parse_datetime_from_datetime_object(self, adapter):
        """Datetime object should be passed through."""
        dt = datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)
        result = adapter._parse_datetime(dt)
        assert result == dt

    def test_parse_datetime_none(self, adapter):
        """None input should return None."""
        result = adapter._parse_datetime(None)
        assert result is None

    def test_parse_datetime_invalid_string(self, adapter):
        """Invalid string format should return None."""
        result = adapter._parse_datetime("not a date")
        assert result is None

    def test_parse_datetime_empty_string(self, adapter):
        """Empty string should return None."""
        result = adapter._parse_datetime("")
        assert result is None

    def test_parse_datetime_duck_typed_to_pydatetime(self, adapter):
        """Object with to_pydatetime() method should be converted."""
        # Create a mock object that simulates pandas Timestamp behavior
        mock_timestamp = MagicMock()
        mock_timestamp.to_pydatetime.return_value = datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)

        result = adapter._parse_datetime(mock_timestamp)

        assert result == datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc)
        mock_timestamp.to_pydatetime.assert_called_once()
