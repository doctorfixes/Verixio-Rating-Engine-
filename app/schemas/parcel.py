from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel


# ── Parcel ────────────────────────────────────────────────────────────────────

class ParcelBase(BaseModel):
    parcel_id: str
    address: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    neighborhood: Optional[str] = None
    assessor_value: Optional[float] = None
    last_sale_date: Optional[date] = None
    last_sale_price: Optional[float] = None


class ParcelOut(ParcelBase):
    model_config = {"from_attributes": True}

    scores: Optional["ScoresOut"] = None


# ── Scores ────────────────────────────────────────────────────────────────────

class ScoresOut(BaseModel):
    model_config = {"from_attributes": True}

    parcel_id: str
    permit_score: Optional[int] = None
    zoning_score: Optional[int] = None
    friction_score: Optional[int] = None
    crime_score: Optional[int] = None
    environmental_score: Optional[int] = None
    nts: Optional[int] = None
    tcs: Optional[int] = None
    vgd: Optional[int] = None
    scored_at: Optional[datetime] = None


# ── Events ────────────────────────────────────────────────────────────────────

class EventOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    parcel_id: Optional[str] = None
    source: Optional[str] = None
    category: Optional[str] = None
    severity: Optional[int] = None
    event_date: Optional[date] = None
    created_at: Optional[datetime] = None


# ── Change Radar ──────────────────────────────────────────────────────────────

class ChangeOut(BaseModel):
    model_config = {"from_attributes": True}

    id: int
    parcel_id: Optional[str] = None
    change_type: Optional[str] = None
    description: Optional[str] = None
    event_date: Optional[date] = None
    created_at: Optional[datetime] = None


# Resolve forward reference
ParcelOut.model_rebuild()
