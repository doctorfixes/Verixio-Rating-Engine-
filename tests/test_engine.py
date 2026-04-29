"""Tests for the scoring engine using an in-memory SQLite database."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.tables import EventNormalized, Parcel, ParcelScore
from scoring.engine import score_all_parcels, score_parcel

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

def _add_parcel(db, parcel_id: str = "P001", last_sale_price: float | None = None) -> None:
    db.add(Parcel(parcel_id=parcel_id, address="123 Main St", last_sale_price=last_sale_price))
    db.commit()


def _add_event(db, parcel_id: str, category: str, days_ago: int = 30) -> None:
    db.add(
        EventNormalized(
            parcel_id=parcel_id,
            source="test",
            category=category,
            severity=30,
            event_date=date.today() - timedelta(days=days_ago),
        )
    )
    db.commit()


# ── Tests: score_parcel ────────────────────────────────────────────────────────

def test_score_parcel_creates_row(db):
    _add_parcel(db)
    result = score_parcel(db, "P001")
    assert result is not None
    assert result.parcel_id == "P001"


def test_score_parcel_all_fields_populated(db):
    _add_parcel(db)
    result = score_parcel(db, "P001")
    for field in ("permit_score", "zoning_score", "friction_score", "crime_score",
                  "environmental_score", "nts", "tcs", "vgd"):
        assert getattr(result, field) is not None, f"{field} should not be None"


def test_score_parcel_range(db):
    """All 0–100 scores must be within bounds; VGD is −100 to 100."""
    _add_parcel(db)
    _add_event(db, "P001", "permit", days_ago=10)
    _add_event(db, "P001", "crime",  days_ago=5)
    result = score_parcel(db, "P001")

    for field in ("permit_score", "zoning_score", "friction_score",
                  "crime_score", "environmental_score", "nts", "tcs"):
        val = getattr(result, field)
        assert 0 <= val <= 100, f"{field}={val} out of [0, 100]"

    assert -100 <= result.vgd <= 100, f"vgd={result.vgd} out of [-100, 100]"


def test_score_parcel_upsert(db):
    """Calling score_parcel twice on the same parcel upserts the row."""
    _add_parcel(db)
    score_parcel(db, "P001")
    score_parcel(db, "P001")
    count = db.query(ParcelScore).filter_by(parcel_id="P001").count()
    assert count == 1


def test_score_parcel_with_no_events_defaults(db):
    """A parcel with no events should still get a complete score row."""
    _add_parcel(db)
    result = score_parcel(db, "P001")
    # No permits → permit_score == 0; no complaints/crime/env → inverse scores == 100
    assert result.permit_score == 0
    assert result.friction_score == 100
    assert result.crime_score == 100
    assert result.environmental_score == 100


def test_score_parcel_permit_events_increase_permit_score(db):
    """Permit events for a parcel should lift its permit_score above zero."""
    _add_parcel(db, "P001")
    _add_parcel(db, "P002")
    _add_event(db, "P001", "permit", days_ago=10)

    r1 = score_parcel(db, "P001")
    r2 = score_parcel(db, "P002")
    assert r1.permit_score > 0
    assert r2.permit_score == 0


def test_score_parcel_crime_reduces_crime_score(db):
    """Crime events should lower the crime_score below 100."""
    _add_parcel(db, "P001")
    _add_parcel(db, "P002")
    _add_event(db, "P001", "crime", days_ago=5)

    r1 = score_parcel(db, "P001")
    r2 = score_parcel(db, "P002")
    assert r1.crime_score < 100
    assert r2.crime_score == 100


# ── Tests: score_all_parcels ───────────────────────────────────────────────────

def test_score_all_returns_count(db):
    _add_parcel(db, "A1")
    _add_parcel(db, "A2")
    _add_parcel(db, "A3")
    assert score_all_parcels(db) == 3


def test_score_all_empty_db(db):
    assert score_all_parcels(db) == 0


def test_score_all_creates_rows_for_every_parcel(db):
    for pid in ["X1", "X2", "X3"]:
        _add_parcel(db, pid)
    score_all_parcels(db)
    count = db.query(ParcelScore).count()
    assert count == 3


# ── Tests: _market_value_percentile ───────────────────────────────────────────

def test_market_value_percentile_returns_50_when_no_sale_price(db):
    """When last_sale_price is None, _market_value_percentile returns 50.0."""
    from scoring.engine import _market_value_percentile
    _add_parcel(db, "P001", last_sale_price=None)
    assert _market_value_percentile(db, "P001") == 50.0


def test_market_value_percentile_returns_50_for_missing_parcel(db):
    """When the parcel doesn't exist, _market_value_percentile returns 50.0."""
    from scoring.engine import _market_value_percentile
    assert _market_value_percentile(db, "NONEXISTENT") == 50.0


def test_market_value_percentile_single_parcel_is_zero(db):
    """A single parcel has 0 parcels below it, so percentile = 0."""
    from scoring.engine import _market_value_percentile
    _add_parcel(db, "ONLY", last_sale_price=200_000)
    assert _market_value_percentile(db, "ONLY") == 0.0


def test_market_value_percentile_highest_price_scores_highest(db):
    """The parcel with the highest sale price has the most parcels below it."""
    from scoring.engine import _market_value_percentile
    _add_parcel(db, "LOW",  last_sale_price=100_000)
    _add_parcel(db, "MID",  last_sale_price=300_000)
    _add_parcel(db, "HIGH", last_sale_price=500_000)

    low_pct  = _market_value_percentile(db, "LOW")
    mid_pct  = _market_value_percentile(db, "MID")
    high_pct = _market_value_percentile(db, "HIGH")

    assert low_pct < mid_pct < high_pct


def test_score_parcel_vgd_higher_for_higher_sale_price(db):
    """Higher sale price → higher market percentile → higher VGD score."""
    _add_parcel(db, "LOW",  last_sale_price=100_000)
    _add_parcel(db, "HIGH", last_sale_price=500_000)

    low_result  = score_parcel(db, "LOW")
    high_result = score_parcel(db, "HIGH")

    assert high_result.vgd > low_result.vgd
