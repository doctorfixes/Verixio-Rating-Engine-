"""Parcel API router."""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.tables import ChangeRadar, EventNormalized, Parcel, ParcelScore
from app.schemas.parcel import ChangeOut, EventOut, ParcelOut, ScoresOut

router = APIRouter(prefix="/parcel", tags=["parcel"])


def _get_parcel_or_404(parcel_id: str, db: Session) -> Parcel:
    parcel = db.get(Parcel, parcel_id)
    if parcel is None:
        raise HTTPException(status_code=404, detail=f"Parcel '{parcel_id}' not found")
    return parcel


@router.get("/{parcel_id}", response_model=ParcelOut)
def get_parcel(parcel_id: str, db: Session = Depends(get_db)) -> Parcel:
    """Return parcel metadata together with its current scores."""
    return _get_parcel_or_404(parcel_id, db)


@router.get("/{parcel_id}/scores", response_model=ScoresOut)
def get_scores(parcel_id: str, db: Session = Depends(get_db)) -> ParcelScore:
    """Return NTS, TCS, VGD and all five input scores."""
    _get_parcel_or_404(parcel_id, db)
    scores = db.get(ParcelScore, parcel_id)
    if scores is None:
        raise HTTPException(status_code=404, detail=f"No scores found for parcel '{parcel_id}'")
    return scores


@router.get("/{parcel_id}/events", response_model=List[EventOut])
def get_events(parcel_id: str, db: Session = Depends(get_db)) -> list[EventNormalized]:
    """Return all normalised events for the parcel."""
    _get_parcel_or_404(parcel_id, db)
    return (
        db.query(EventNormalized)
        .filter(EventNormalized.parcel_id == parcel_id)
        .order_by(EventNormalized.event_date.desc())
        .all()
    )


@router.get("/{parcel_id}/changes", response_model=List[ChangeOut])
def get_changes(parcel_id: str, db: Session = Depends(get_db)) -> list[ChangeRadar]:
    """Return Change Radar alerts for the parcel."""
    _get_parcel_or_404(parcel_id, db)
    return (
        db.query(ChangeRadar)
        .filter(ChangeRadar.parcel_id == parcel_id)
        .order_by(ChangeRadar.created_at.desc())
        .all()
    )
