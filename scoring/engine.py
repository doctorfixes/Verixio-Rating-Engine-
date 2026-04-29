"""
Scoring engine — orchestrates data retrieval and score persistence.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.tables import (
    EventNormalized,
    Parcel,
    ParcelScore,
)
from scoring import formulas


def _count_events(
    db: Session,
    parcel_id: str,
    category: str,
    since: date,
) -> int:
    result = db.execute(
        select(func.count())
        .select_from(EventNormalized)
        .where(
            EventNormalized.parcel_id == parcel_id,
            EventNormalized.category == category,
            EventNormalized.event_date >= since,
        )
    )
    return result.scalar_one_or_none() or 0


def _max_citywide(db: Session, category: str, since: date) -> int:
    """Return the highest per-parcel event count for a given category."""
    sub = (
        select(func.count().label("cnt"))
        .select_from(EventNormalized)
        .where(
            EventNormalized.category == category,
            EventNormalized.event_date >= since,
        )
        .group_by(EventNormalized.parcel_id)
        .subquery()
    )
    result = db.execute(select(func.max(sub.c.cnt)))
    return result.scalar_one_or_none() or 1


def _market_value_percentile(db: Session, parcel_id: str) -> float:
    """
    Compute where this parcel's last_sale_price falls among all parcels
    as a 0–100 percentile.
    """
    parcel = db.get(Parcel, parcel_id)
    if parcel is None or parcel.last_sale_price is None:
        return 50.0

    total = db.execute(
        select(func.count()).select_from(Parcel).where(Parcel.last_sale_price.isnot(None))
    ).scalar_one_or_none() or 1

    below = db.execute(
        select(func.count())
        .select_from(Parcel)
        .where(Parcel.last_sale_price < parcel.last_sale_price)
    ).scalar_one_or_none() or 0

    return 100 * below / total


def score_parcel(db: Session, parcel_id: str) -> Optional[ParcelScore]:
    """
    Compute and persist scores for a single parcel.
    Returns the upserted ParcelScore row.
    """
    today = date.today()
    d12mo = today - timedelta(days=365)
    d90d = today - timedelta(days=90)
    d30d = today - timedelta(days=30)

    # ── Input scores ─────────────────────────────────────────────────────────
    permits_12mo = _count_events(db, parcel_id, "permit", d12mo)
    max_permits = _max_citywide(db, "permit", d12mo)
    ps = formulas.permit_score(permits_12mo, max_permits)

    # Zoning: derive from zoning_change events.
    # favorability = inverse of zoning-change frequency; changes = recent flag
    zoning_changes_12mo = _count_events(db, parcel_id, "zoning_change", d12mo)
    max_zoning = _max_citywide(db, "zoning_change", d12mo)
    zoning_favorability = 1.0 - (zoning_changes_12mo / max_zoning) if max_zoning else 1.0
    recent_zoning = 1.0 if zoning_changes_12mo > 0 else 0.0
    zs = formulas.zoning_score(zoning_favorability, recent_zoning)

    complaints_30d = _count_events(db, parcel_id, "311_complaint", d30d)
    max_311 = _max_citywide(db, "311_complaint", d30d)
    fs = formulas.friction_score(complaints_30d, max_311)

    crime_90d = _count_events(db, parcel_id, "crime", d90d)
    max_crime = _max_citywide(db, "crime", d90d)
    cs = formulas.crime_score(crime_90d, max_crime)

    env_12mo = _count_events(db, parcel_id, "environmental", d12mo)
    max_env = _max_citywide(db, "environmental", d12mo)
    es = formulas.environmental_score(env_12mo, max_env)

    # ── Fused scores ──────────────────────────────────────────────────────────
    nts_val = formulas.nts(ps, zs, fs)
    tcs_val = formulas.tcs(fs, cs, es)
    mvp = _market_value_percentile(db, parcel_id)
    vgd_val = formulas.vgd(nts_val, tcs_val, mvp)

    # ── Upsert ────────────────────────────────────────────────────────────────
    row = db.get(ParcelScore, parcel_id)
    if row is None:
        row = ParcelScore(parcel_id=parcel_id)
        db.add(row)

    row.permit_score = ps
    row.zoning_score = zs
    row.friction_score = fs
    row.crime_score = cs
    row.environmental_score = es
    row.nts = nts_val
    row.tcs = tcs_val
    row.vgd = vgd_val

    db.commit()
    db.refresh(row)
    return row


def score_all_parcels(db: Session) -> int:
    """Score every parcel in the database. Returns the number scored."""
    parcel_ids = db.execute(select(Parcel.parcel_id)).scalars().all()
    for pid in parcel_ids:
        score_parcel(db, pid)
    return len(parcel_ids)
