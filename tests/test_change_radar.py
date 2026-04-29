"""Tests for Change Radar using an in-memory SQLite database."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.tables import ChangeRadar, EventNormalized, Parcel
from change_radar.radar import LOOKBACK_DAYS, run_change_radar

# ── Test DB setup ──────────────────────────────────────────────────────────────

SQLALCHEMY_TEST_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_TEST_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSession = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def db():
    session = TestingSession()
    try:
        yield session
    finally:
        session.close()


# ── Helpers ────────────────────────────────────────────────────────────────────

def _add_parcel(db, parcel_id: str = "P001") -> None:
    db.add(Parcel(parcel_id=parcel_id, address="123 Main St"))
    db.commit()


def _add_event(
    db,
    category: str,
    parcel_id: str = "P001",
    event_date: date | None = None,
    created_at: datetime | None = None,
) -> None:
    ev = EventNormalized(
        parcel_id=parcel_id,
        source="test",
        category=category,
        severity=30,
        event_date=event_date or date.today(),
    )
    if created_at is not None:
        ev.created_at = created_at
    db.add(ev)
    db.commit()


def _recent_created_at() -> datetime:
    """Return a created_at that falls within the LOOKBACK_DAYS window."""
    return datetime.combine(
        date.today() - timedelta(days=LOOKBACK_DAYS - 1),
        datetime.min.time(),
    )


def _old_created_at() -> datetime:
    """Return a created_at outside the LOOKBACK_DAYS window."""
    return datetime.combine(
        date.today() - timedelta(days=LOOKBACK_DAYS + 5),
        datetime.min.time(),
    )


# ── Tests ──────────────────────────────────────────────────────────────────────

def test_no_events_returns_zero(db):
    _add_parcel(db)
    assert run_change_radar(db) == 0


def test_new_permit_triggers_alert(db):
    _add_parcel(db)
    _add_event(db, "permit", created_at=_recent_created_at())

    count = run_change_radar(db)
    assert count == 1
    alert = db.query(ChangeRadar).first()
    assert alert.change_type == "new_permit"
    assert alert.parcel_id == "P001"


def test_new_permit_old_event_no_alert(db):
    _add_parcel(db)
    _add_event(db, "permit", created_at=_old_created_at())
    assert run_change_radar(db) == 0


def test_zoning_change_triggers_alert(db):
    _add_parcel(db)
    _add_event(db, "zoning_change", created_at=_recent_created_at())

    count = run_change_radar(db)
    assert count == 1
    alert = db.query(ChangeRadar).first()
    assert alert.change_type == "zoning_change"


def test_environmental_triggers_alert(db):
    _add_parcel(db)
    _add_event(db, "environmental", created_at=_recent_created_at())

    count = run_change_radar(db)
    assert count == 1
    alert = db.query(ChangeRadar).first()
    assert alert.change_type == "environmental_notice"


def test_duplicate_alert_not_inserted(db):
    _add_parcel(db)
    _add_event(db, "permit", created_at=_recent_created_at())

    run_change_radar(db)
    run_change_radar(db)  # second run — same event

    alerts = db.query(ChangeRadar).all()
    assert len(alerts) == 1


def test_no_alert_without_parcel_id(db):
    # EventNormalized with no parcel_id must not produce an alert
    ev = EventNormalized(
        parcel_id=None,
        source="test",
        category="permit",
        severity=10,
        event_date=date.today(),
        created_at=_recent_created_at(),
    )
    db.add(ev)
    db.commit()

    assert run_change_radar(db) == 0


def test_multiple_categories_create_multiple_alerts(db):
    _add_parcel(db)
    _add_event(db, "permit",        created_at=_recent_created_at())
    _add_event(db, "zoning_change", created_at=_recent_created_at())
    _add_event(db, "environmental", created_at=_recent_created_at())

    count = run_change_radar(db)
    assert count == 3
    types = {a.change_type for a in db.query(ChangeRadar).all()}
    assert types == {"new_permit", "zoning_change", "environmental_notice"}


# ── Tests: _avg_30d ────────────────────────────────────────────────────────────

def test_avg_30d_no_events_returns_zero(db):
    from change_radar.radar import _avg_30d
    _add_parcel(db)
    assert _avg_30d(db, "P001", "311_complaint") == 0.0


def test_avg_30d_with_events_in_window(db):
    from change_radar.radar import _avg_30d
    _add_parcel(db)
    # Add 30 events spread across the 30-day window (days -1 to -30)
    for days_ago in range(1, 31):
        _add_event(
            db,
            "311_complaint",
            event_date=date.today() - timedelta(days=days_ago),
        )
    result = _avg_30d(db, "P001", "311_complaint")
    assert result == 1.0  # 30 events / 30 days


def test_avg_30d_excludes_today(db):
    from change_radar.radar import _avg_30d
    _add_parcel(db)
    # Only an event from today — should NOT be in the window
    _add_event(db, "311_complaint", event_date=date.today())
    assert _avg_30d(db, "P001", "311_complaint") == 0.0


def test_avg_30d_excludes_old_events(db):
    from change_radar.radar import _avg_30d
    _add_parcel(db)
    # Event older than 31 days — falls outside the window
    _add_event(
        db,
        "311_complaint",
        event_date=date.today() - timedelta(days=32),
    )
    assert _avg_30d(db, "P001", "311_complaint") == 0.0


# ── Tests: 311 spike ───────────────────────────────────────────────────────────

def test_311_spike_triggers_alert(db):
    """A 311 spike (today_count > 2 × 30-day avg) should emit a 311_spike alert."""
    _add_parcel(db)
    # Establish a 30-day baseline: 1 event/day (avg = 1.0)
    for days_ago in range(1, 31):
        _add_event(
            db,
            "311_complaint",
            event_date=date.today() - timedelta(days=days_ago),
        )
    # Add 3 complaints today via recent created_at (today_count=3 > 2*1.0=2)
    for _ in range(3):
        _add_event(
            db,
            "311_complaint",
            event_date=date.today(),
            created_at=_recent_created_at(),
        )

    count = run_change_radar(db)
    assert count >= 1
    alert = db.query(ChangeRadar).filter_by(change_type="311_spike").first()
    assert alert is not None
    assert alert.parcel_id == "P001"


def test_311_no_spike_when_avg_is_zero(db):
    """Without a historical baseline, no spike alert should be generated."""
    _add_parcel(db)
    _add_event(
        db,
        "311_complaint",
        event_date=date.today(),
        created_at=_recent_created_at(),
    )
    count = run_change_radar(db)
    assert count == 0
    assert db.query(ChangeRadar).filter_by(change_type="311_spike").count() == 0


def test_311_no_spike_when_below_threshold(db):
    """today_count <= 2 * avg should NOT produce a spike alert."""
    _add_parcel(db)
    # Baseline: 30 events in 30 days → avg = 1.0
    for days_ago in range(1, 31):
        _add_event(
            db,
            "311_complaint",
            event_date=date.today() - timedelta(days=days_ago),
        )
    # Today: 2 events (exactly at threshold, not exceeding it)
    for _ in range(2):
        _add_event(
            db,
            "311_complaint",
            event_date=date.today(),
            created_at=_recent_created_at(),
        )
    run_change_radar(db)
    assert db.query(ChangeRadar).filter_by(change_type="311_spike").count() == 0
