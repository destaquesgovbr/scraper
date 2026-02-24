"""
Pydantic models for news and related entities.
"""

from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field


class Agency(BaseModel):
    """Government agency model."""

    id: Optional[int] = None
    key: str
    name: str
    type: Optional[str] = None
    parent_key: Optional[str] = None
    url: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class Theme(BaseModel):
    """Theme taxonomy model."""

    id: Optional[int] = None
    code: str
    label: str
    full_name: Optional[str] = None
    level: int = Field(..., ge=1, le=3)
    parent_code: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class News(BaseModel):
    """News article model."""

    # Primary key
    id: Optional[int] = None
    unique_id: str

    # Foreign keys
    agency_id: int
    theme_l1_id: Optional[int] = None
    theme_l2_id: Optional[int] = None
    theme_l3_id: Optional[int] = None
    most_specific_theme_id: Optional[int] = None

    # Core content
    title: str
    url: Optional[str] = None
    image_url: Optional[str] = None
    video_url: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    content: Optional[str] = None
    editorial_lead: Optional[str] = None
    subtitle: Optional[str] = None

    # AI-generated
    summary: Optional[str] = None

    # Timestamps
    published_at: datetime
    updated_datetime: Optional[datetime] = None
    extracted_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Denormalized (performance)
    agency_key: Optional[str] = None
    agency_name: Optional[str] = None

    # Embeddings (Phase 4.7)
    content_embedding: Optional[List[float]] = None  # 768-dimensional vector
    embedding_generated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class NewsInsert(BaseModel):
    """News model for insert operations (without generated fields)."""

    unique_id: str
    agency_id: int
    theme_l1_id: Optional[int] = None
    theme_l2_id: Optional[int] = None
    theme_l3_id: Optional[int] = None
    most_specific_theme_id: Optional[int] = None
    title: str
    url: Optional[str] = None
    image_url: Optional[str] = None
    video_url: Optional[str] = None
    category: Optional[str] = None
    tags: Optional[List[str]] = None
    content: Optional[str] = None
    editorial_lead: Optional[str] = None
    subtitle: Optional[str] = None
    summary: Optional[str] = None
    published_at: datetime
    updated_datetime: Optional[datetime] = None
    extracted_at: Optional[datetime] = None
    agency_key: Optional[str] = None
    agency_name: Optional[str] = None
    content_embedding: Optional[List[float]] = None  # 768-dimensional vector (Phase 4.7)
    embedding_generated_at: Optional[datetime] = None  # Phase 4.7
