"""
Pydantic models for news and related entities.
"""

from datetime import datetime

from pydantic import BaseModel, Field


class Agency(BaseModel):
    """Government agency model."""

    id: int | None = None
    key: str
    name: str
    type: str | None = None
    parent_key: str | None = None
    url: str | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class Theme(BaseModel):
    """Theme taxonomy model."""

    id: int | None = None
    code: str
    label: str
    full_name: str | None = None
    level: int = Field(..., ge=1, le=3)
    parent_code: str | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class News(BaseModel):
    """News article model."""

    # Primary key
    id: int | None = None
    unique_id: str

    # Foreign keys
    agency_id: int
    theme_l1_id: int | None = None
    theme_l2_id: int | None = None
    theme_l3_id: int | None = None
    most_specific_theme_id: int | None = None

    # Core content
    title: str
    url: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    tags: list[str] | None = None
    content: str | None = None
    editorial_lead: str | None = None
    subtitle: str | None = None

    # AI-generated
    summary: str | None = None

    # Timestamps
    published_at: datetime
    updated_datetime: datetime | None = None
    extracted_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    # Denormalized (performance)
    agency_key: str | None = None
    agency_name: str | None = None

    # Embeddings (Phase 4.7)
    content_embedding: list[float] | None = None  # 768-dimensional vector
    embedding_generated_at: datetime | None = None

    class Config:
        from_attributes = True


class NewsInsert(BaseModel):
    """News model for insert operations (without generated fields)."""

    unique_id: str
    agency_id: int
    theme_l1_id: int | None = None
    theme_l2_id: int | None = None
    theme_l3_id: int | None = None
    most_specific_theme_id: int | None = None
    title: str
    url: str | None = None
    image_url: str | None = None
    video_url: str | None = None
    category: str | None = None
    tags: list[str] | None = None
    content: str | None = None
    editorial_lead: str | None = None
    subtitle: str | None = None
    summary: str | None = None
    published_at: datetime
    updated_datetime: datetime | None = None
    extracted_at: datetime | None = None
    agency_key: str | None = None
    agency_name: str | None = None
    content_hash: str | None = None
    content_embedding: list[float] | None = None  # 768-dimensional vector (Phase 4.7)
    embedding_generated_at: datetime | None = None  # Phase 4.7
