"""Integration tests for the FastAPI endpoints using an in-memory SQLite DB."""

from __future__ import annotations

from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base, get_db
from app.main import app
from app.models.tables import ChangeRadar, EventNormalized, Parcel, ParcelScore

# ── Test database setup ────────────────────────────────────────────────────────

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


def override_get_db():
    db = TestingSession()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _seed_parcel(parcel_id: str = "P001") -> None:
    db = TestingSession()
    db.add(
        Parcel(
            parcel_id=parcel_id,
            address="123 Main St",
            neighborhood="Capitol Hill",
            assessor_value=400000,
            last_sale_price=420000,
        )
    )
    db.commit()
    db.close()


def _seed_scores(parcel_id: str = "P001") -> None:
    db = TestingSession()
    db.add(
        ParcelScore(
            parcel_id=parcel_id,
            permit_score=70,
            zoning_score=80,
            friction_score=90,
            crime_score=60,
            environmental_score=85,
            nts=75,
            tcs=78,
            vgd=5,
        )
    )
    db.commit()
    db.close()


def _seed_event(parcel_id: str = "P001") -> None:
    db = TestingSession()
    db.add(
        EventNormalized(
            parcel_id=parcel_id,
            source="permits",
            category="permit",
            severity=10,
            event_date=date(2024, 1, 15),
        )
    )
    db.commit()
    db.close()


def _seed_change(parcel_id: str = "P001") -> None:
    db = TestingSession()
    db.add(
        ChangeRadar(
            parcel_id=parcel_id,
            change_type="new_permit",
            description="New permit recorded",
            event_date=date(2024, 1, 15),
        )
    )
    db.commit()
    db.close()


# ── Health endpoint ────────────────────────────────────────────────────────────

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ── GET /parcel/{parcel_id} ────────────────────────────────────────────────────

def test_get_parcel_not_found():
    resp = client.get("/parcel/MISSING")
    assert resp.status_code == 404


def test_get_parcel_no_scores():
    _seed_parcel()
    resp = client.get("/parcel/P001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["parcel_id"] == "P001"
    assert data["address"] == "123 Main St"
    assert data["scores"] is None


def test_get_parcel_with_scores():
    _seed_parcel()
    _seed_scores()
    resp = client.get("/parcel/P001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["scores"]["nts"] == 75
    assert data["scores"]["tcs"] == 78
    assert data["scores"]["vgd"] == 5


# ── GET /parcel/{parcel_id}/scores ─────────────────────────────────────────────

def test_get_scores_not_found():
    _seed_parcel()
    resp = client.get("/parcel/P001/scores")
    assert resp.status_code == 404


def test_get_scores():
    _seed_parcel()
    _seed_scores()
    resp = client.get("/parcel/P001/scores")
    assert resp.status_code == 200
    data = resp.json()
    assert data["permit_score"] == 70
    assert data["zoning_score"] == 80
    assert data["friction_score"] == 90
    assert data["crime_score"] == 60
    assert data["environmental_score"] == 85
    assert data["nts"] == 75
    assert data["tcs"] == 78
    assert data["vgd"] == 5


# ── GET /parcel/{parcel_id}/events ─────────────────────────────────────────────

def test_get_events_empty():
    _seed_parcel()
    resp = client.get("/parcel/P001/events")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_events():
    _seed_parcel()
    _seed_event()
    resp = client.get("/parcel/P001/events")
    assert resp.status_code == 200
    events = resp.json()
    assert len(events) == 1
    assert events[0]["category"] == "permit"
    assert events[0]["severity"] == 10


# ── GET /parcel/{parcel_id}/changes ────────────────────────────────────────────

def test_get_changes_empty():
    _seed_parcel()
    resp = client.get("/parcel/P001/changes")
    assert resp.status_code == 200
    assert resp.json() == []


def test_get_changes():
    _seed_parcel()
    _seed_change()
    resp = client.get("/parcel/P001/changes")
    assert resp.status_code == 200
    changes = resp.json()
    assert len(changes) == 1
    assert changes[0]["change_type"] == "new_permit"
