"""
Change Radar — detects meaningful parcel-level changes and writes alerts.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.tables import ChangeRadar, EventNormalized

logger = logging.getLogger(__name__)

# How far back to look for "new" events
LOOKBACK_DAYS = 1


def _since() -> date:
    return date.today() - timedelta(days=LOOKBACK_DAYS)


def _avg_30d(db: Session, parcel_id: str, category: str) -> float:
    """Average daily event count over the previous 30 days."""
    window_start = date.today() - timedelta(days=31)
    window_end = date.today() - timedelta(days=1)
    count = db.execute(
        select(func.count())
        .select_from(EventNormalized)
        .where(
            EventNormalized.parcel_id == parcel_id,
            EventNormalized.category == category,
            EventNormalized.event_date.between(window_start, window_end),
        )
    ).scalar_one_or_none() or 0
    return count / 30.0


def _new_events(
    db: Session,
    category: str,
    since: date,
) -> list[EventNormalized]:
    return (
        db.execute(
            select(EventNormalized).where(
                EventNormalized.category == category,
                EventNormalized.created_at >= datetime.combine(since, datetime.min.time()),
            )
        )
        .scalars()
        .all()
    )


def _already_alerted(db: Session, parcel_id: str, change_type: str, event_date: date) -> bool:
    count = db.execute(
        select(func.count())
        .select_from(ChangeRadar)
        .where(
            ChangeRadar.parcel_id == parcel_id,
            ChangeRadar.change_type == change_type,
            ChangeRadar.event_date == event_date,
        )
    ).scalar_one_or_none() or 0
    return count > 0


def _insert_alert(
    db: Session,
    parcel_id: str,
    change_type: str,
    description: str,
    event_date: date,
) -> None:
    if _already_alerted(db, parcel_id, change_type, event_date):
        return
    db.add(
        ChangeRadar(
            parcel_id=parcel_id,
            change_type=change_type,
            description=description,
            event_date=event_date,
        )
    )


def run_change_radar(db: Session) -> int:
    """
    Scan recent events and emit Change Radar alerts.
    Returns the number of new alerts inserted.
    """
    since = _since()
    inserted = 0

    # ── New permit ────────────────────────────────────────────────────────────
    for ev in _new_events(db, "permit", since):
        if ev.parcel_id:
            _insert_alert(
                db,
                ev.parcel_id,
                "new_permit",
                f"New permit recorded on {ev.event_date}",
                ev.event_date or date.today(),
            )
            inserted += 1

    # ── Zoning change ─────────────────────────────────────────────────────────
    for ev in _new_events(db, "zoning_change", since):
        if ev.parcel_id:
            _insert_alert(
                db,
                ev.parcel_id,
                "zoning_change",
                f"Zoning change recorded on {ev.event_date}",
                ev.event_date or date.today(),
            )
            inserted += 1

    # ── 311 spike (>2× 30-day avg) ────────────────────────────────────────────
    for ev in _new_events(db, "311_complaint", since):
        if ev.parcel_id:
            avg = _avg_30d(db, ev.parcel_id, "311_complaint")
            if avg > 0:
                # count today's complaints for this parcel
                today_count = db.execute(
                    select(func.count())
                    .select_from(EventNormalized)
                    .where(
                        EventNormalized.parcel_id == ev.parcel_id,
                        EventNormalized.category == "311_complaint",
                        EventNormalized.event_date == date.today(),
                    )
                ).scalar_one_or_none() or 0
                if today_count > 2 * avg:
                    _insert_alert(
                        db,
                        ev.parcel_id,
                        "311_spike",
                        (
                            f"311 spike: {today_count} complaints today "
                            f"vs {avg:.1f}/day 30-day avg"
                        ),
                        date.today(),
                    )
                    inserted += 1

    # ── New environmental notice ───────────────────────────────────────────────
    for ev in _new_events(db, "environmental", since):
        if ev.parcel_id:
            _insert_alert(
                db,
                ev.parcel_id,
                "environmental_notice",
                f"New environmental notice on {ev.event_date}",
                ev.event_date or date.today(),
            )
            inserted += 1

    db.commit()
    logger.info("Change Radar: %d new alerts inserted", inserted)
    return inserted
