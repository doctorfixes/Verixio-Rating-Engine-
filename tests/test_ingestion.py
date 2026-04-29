"""Tests for the ingestion layer — BaseIngester, sub-ingesters, and ParcelSeeder."""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.database import Base
from app.models.tables import EventNormalized, EventRaw, Parcel
from ingestion.base import BaseIngester, SOCRATA_PAGE_SIZE
from ingestion.complaints import Complaints311Ingester
from ingestion.crime import CrimeIngester
from ingestion.environmental import EnvironmentalIngester
from ingestion.parcel_seed import ParcelSeeder, _parse, _to_float
from ingestion.permits import PermitsIngester

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


@pytest.fixture()
def db():
    session = TestingSession()
    try:
        yield session
    finally:
        session.close()


# ── Minimal concrete BaseIngester for testing ─────────────────────────────────

class _TestIngester(BaseIngester):
    source = "test"
    url = "https://example.com/test"

    def _address(self, record: dict[str, Any]) -> str | None:
        return record.get("address")

    def _category(self, record: dict[str, Any]) -> str:
        return "test_cat"

    def _severity(self, record: dict[str, Any]) -> int:
        return 25

    def _event_date(self, record: dict[str, Any]):
        return None


def _make_record(address: str | None = "123 MAIN ST") -> dict[str, Any]:
    return {"address": address, "some_field": "value"}


# ── BaseIngester.run() ─────────────────────────────────────────────────────────

def test_run_empty_response_returns_zero(db):
    ingester = _TestIngester(db)
    with patch("requests.get") as mock_get:
        mock_get.return_value = MagicMock(json=lambda: [], raise_for_status=lambda: None)
        result = ingester.run()
    assert result == 0
    assert db.query(EventRaw).count() == 0


def test_run_single_page_returns_count(db):
    records = [_make_record() for _ in range(5)]
    ingester = _TestIngester(db)
    with patch("requests.get") as mock_get:
        responses = [
            MagicMock(json=lambda r=records: r, raise_for_status=lambda: None),
            MagicMock(json=lambda: [], raise_for_status=lambda: None),
        ]
        mock_get.side_effect = responses
        result = ingester.run()
    assert result == 5


def test_run_stores_raw_records(db):
    records = [_make_record(), _make_record()]
    ingester = _TestIngester(db)
    with patch("requests.get") as mock_get:
        mock_get.return_value = MagicMock(
            json=lambda: records, raise_for_status=lambda: None
        )
        ingester.run()
    assert db.query(EventRaw).count() == 2


def test_run_normalises_matched_parcel(db):
    db.add(Parcel(parcel_id="P001", address="123 Main St"))
    db.commit()
    ingester = _TestIngester(db)
    records = [{"address": "123 Main St"}]
    with patch("requests.get") as mock_get:
        mock_get.return_value = MagicMock(
            json=lambda: records, raise_for_status=lambda: None
        )
        ingester.run()
    assert db.query(EventNormalized).filter_by(parcel_id="P001").count() == 1


def test_run_skips_normalise_for_unmatched_records(db):
    ingester = _TestIngester(db)
    records = [{"address": "999 Unknown Blvd"}]
    with patch("requests.get") as mock_get:
        mock_get.return_value = MagicMock(
            json=lambda: records, raise_for_status=lambda: None
        )
        ingester.run()
    assert db.query(EventNormalized).count() == 0
    assert db.query(EventRaw).count() == 1


def test_run_multi_page(db):
    page1 = [_make_record() for _ in range(SOCRATA_PAGE_SIZE)]
    page2 = [_make_record() for _ in range(3)]
    ingester = _TestIngester(db)
    with patch("requests.get") as mock_get:
        responses = [
            MagicMock(json=lambda p=page1: p, raise_for_status=lambda: None),
            MagicMock(json=lambda p=page2: p, raise_for_status=lambda: None),
        ]
        mock_get.side_effect = responses
        result = ingester.run()
    assert result == SOCRATA_PAGE_SIZE + 3


# ── BaseIngester._resolve_parcel_id ───────────────────────────────────────────

def test_resolve_parcel_id_no_address(db):
    ingester = _TestIngester(db)
    assert ingester._resolve_parcel_id({"address": None}) is None


def test_resolve_parcel_id_empty_address(db):
    ingester = _TestIngester(db)
    assert ingester._resolve_parcel_id({"address": ""}) is None


def test_resolve_parcel_id_not_found(db):
    ingester = _TestIngester(db)
    assert ingester._resolve_parcel_id({"address": "999 Unknown Rd"}) is None


def test_resolve_parcel_id_found(db):
    db.add(Parcel(parcel_id="P042", address="42 Test Ave"))
    db.commit()
    ingester = _TestIngester(db)
    result = ingester._resolve_parcel_id({"address": "42 Test Ave"})
    assert result == "P042"


def test_resolve_parcel_id_case_insensitive(db):
    db.add(Parcel(parcel_id="P007", address="7 Oak Street"))
    db.commit()
    ingester = _TestIngester(db)
    result = ingester._resolve_parcel_id({"address": "7 oak street"})
    assert result == "P007"


# ── PermitsIngester ────────────────────────────────────────────────────────────

@pytest.fixture()
def permits(db):
    return PermitsIngester(db)


def test_permits_address(permits):
    assert permits._address({"address": "100 Colfax"}) == "100 Colfax"


def test_permits_address_missing(permits):
    assert permits._address({}) is None


def test_permits_category(permits):
    assert permits._category({}) == "permit"


def test_permits_severity_issued(permits):
    assert permits._severity({"permit_status": "issued"}) == 10


def test_permits_severity_approved(permits):
    assert permits._severity({"permit_status": "approved"}) == 10


def test_permits_severity_expired(permits):
    assert permits._severity({"permit_status": "expired"}) == 60


def test_permits_severity_void(permits):
    assert permits._severity({"permit_status": "void"}) == 60


def test_permits_severity_cancelled(permits):
    assert permits._severity({"permit_status": "cancelled"}) == 60


def test_permits_severity_unknown_status(permits):
    assert permits._severity({"permit_status": "pending"}) == 30


def test_permits_severity_missing_status(permits):
    assert permits._severity({}) == 30


def test_permits_event_date_issue_date(permits):
    assert permits._event_date({"issue_date": "2024-03-15"}) == date(2024, 3, 15)


def test_permits_event_date_applied_date_fallback(permits):
    assert permits._event_date({"applied_date": "2024-06-01"}) == date(2024, 6, 1)


def test_permits_event_date_issue_date_with_time(permits):
    assert permits._event_date({"issue_date": "2024-03-15T10:30:00"}) == date(2024, 3, 15)


def test_permits_event_date_missing(permits):
    assert permits._event_date({}) is None


def test_permits_event_date_invalid(permits):
    assert permits._event_date({"issue_date": "not-a-date"}) is None


# ── Complaints311Ingester ──────────────────────────────────────────────────────

@pytest.fixture()
def complaints(db):
    return Complaints311Ingester(db)


def test_complaints_address_incident_address(complaints):
    assert complaints._address({"incident_address": "5th & Main"}) == "5th & Main"


def test_complaints_address_fallback(complaints):
    assert complaints._address({"address": "200 Larimer"}) == "200 Larimer"


def test_complaints_address_missing(complaints):
    assert complaints._address({}) is None


def test_complaints_category(complaints):
    assert complaints._category({}) == "311_complaint"


def test_complaints_severity_nuisance(complaints):
    assert complaints._severity({"agency_responsible": "Nuisance Abatement"}) == 70


def test_complaints_severity_code(complaints):
    assert complaints._severity({"agency_responsible": "Code Enforcement"}) == 70


def test_complaints_severity_unsafe(complaints):
    assert complaints._severity({"agency_responsible": "Unsafe Structures"}) == 70


def test_complaints_severity_other_agency(complaints):
    assert complaints._severity({"agency_responsible": "Parks Department"}) == 40


def test_complaints_severity_missing_agency(complaints):
    assert complaints._severity({}) == 40


def test_complaints_event_date_service_request_date(complaints):
    assert complaints._event_date({"service_request_date": "2024-05-20"}) == date(2024, 5, 20)


def test_complaints_event_date_created_fallback(complaints):
    assert complaints._event_date({"date_created": "2024-07-04"}) == date(2024, 7, 4)


def test_complaints_event_date_missing(complaints):
    assert complaints._event_date({}) is None


def test_complaints_event_date_invalid(complaints):
    assert complaints._event_date({"service_request_date": "bad-date"}) is None


# ── CrimeIngester ──────────────────────────────────────────────────────────────

@pytest.fixture()
def crime(db):
    return CrimeIngester(db)


def test_crime_address_incident_address(crime):
    assert crime._address({"incident_address": "900 Lincoln St"}) == "900 Lincoln St"


def test_crime_address_fallback(crime):
    assert crime._address({"address": "10 Blake St"}) == "10 Blake St"


def test_crime_address_missing(crime):
    assert crime._address({}) is None


def test_crime_category(crime):
    assert crime._category({}) == "crime"


@pytest.mark.parametrize("offense", ["murder", "robbery", "assault", "burglary", "arson", "kidnapping"])
def test_crime_severity_high(crime, offense):
    assert crime._severity({"offense_category_id": offense}) == 90


def test_crime_severity_high_offense_contains_keyword(crime):
    assert crime._severity({"offense_category_id": "aggravated-assault"}) == 90


def test_crime_severity_low_offense(crime):
    assert crime._severity({"offense_category_id": "theft-from-vehicle"}) == 50


def test_crime_severity_offense_type_id_fallback(crime):
    assert crime._severity({"offense_type_id": "murder-first-degree"}) == 90


def test_crime_severity_missing(crime):
    assert crime._severity({}) == 50


def test_crime_event_date_first_occurrence(crime):
    assert crime._event_date({"first_occurrence_date": "2024-01-10"}) == date(2024, 1, 10)


def test_crime_event_date_reported_fallback(crime):
    assert crime._event_date({"reported_date": "2024-02-14"}) == date(2024, 2, 14)


def test_crime_event_date_with_time(crime):
    assert crime._event_date({"first_occurrence_date": "2024-01-10T22:00:00"}) == date(2024, 1, 10)


def test_crime_event_date_missing(crime):
    assert crime._event_date({}) is None


def test_crime_event_date_invalid(crime):
    assert crime._event_date({"first_occurrence_date": "invalid"}) is None


# ── EnvironmentalIngester ──────────────────────────────────────────────────────

@pytest.fixture()
def environmental(db):
    return EnvironmentalIngester(db)


def test_environmental_address(environmental):
    assert environmental._address({"address": "300 Cherry Creek"}) == "300 Cherry Creek"


def test_environmental_address_site_address_fallback(environmental):
    assert environmental._address({"site_address": "55 Water St"}) == "55 Water St"


def test_environmental_address_missing(environmental):
    assert environmental._address({}) is None


def test_environmental_category(environmental):
    assert environmental._category({}) == "environmental"


def test_environmental_severity_active(environmental):
    assert environmental._severity({"status": "active"}) == 80


def test_environmental_severity_open(environmental):
    assert environmental._severity({"status": "open investigation"}) == 80


def test_environmental_severity_closed(environmental):
    assert environmental._severity({"status": "closed"}) == 40


def test_environmental_severity_missing(environmental):
    assert environmental._severity({}) == 40


def test_environmental_event_date_open_date(environmental):
    assert environmental._event_date({"open_date": "2023-11-01"}) == date(2023, 11, 1)


def test_environmental_event_date_reported_fallback(environmental):
    assert environmental._event_date({"reported_date": "2023-12-15"}) == date(2023, 12, 15)


def test_environmental_event_date_created_fallback(environmental):
    assert environmental._event_date({"date_created": "2024-01-20"}) == date(2024, 1, 20)


def test_environmental_event_date_missing(environmental):
    assert environmental._event_date({}) is None


def test_environmental_event_date_invalid(environmental):
    assert environmental._event_date({"open_date": "not-valid"}) is None


# ── parcel_seed._to_float ──────────────────────────────────────────────────────

def test_to_float_none():
    assert _to_float(None) is None


def test_to_float_int():
    assert _to_float(42) == 42.0


def test_to_float_string():
    assert _to_float("3.14") == 3.14


def test_to_float_invalid_string():
    assert _to_float("not-a-number") is None


def test_to_float_zero():
    assert _to_float(0) == 0.0


# ── parcel_seed._parse ─────────────────────────────────────────────────────────

def test_parse_full_record():
    record = {
        "pin": " P001 ",
        "property_address": "100 Main St",
        "situs_full_nbhd_description": "Capitol Hill",
        "total_actual_value": "500000",
        "lat": "39.7",
        "long": "-104.9",
    }
    result = _parse(record)
    assert result is not None
    assert result["parcel_id"] == "P001"
    assert result["address"] == "100 Main St"
    assert result["neighborhood"] == "Capitol Hill"
    assert result["assessor_value"] == 500000.0
    assert result["lat"] == 39.7
    assert result["lon"] == -104.9


def test_parse_missing_pin_returns_none():
    assert _parse({}) is None


def test_parse_empty_pin_returns_none():
    assert _parse({"pin": "   "}) is None


def test_parse_address_fallback_field(dummy=None):
    record = {"pin": "P002", "situs_street_address": "200 Oak Ave"}
    result = _parse(record)
    assert result is not None
    assert result["address"] == "200 Oak Ave"


def test_parse_neighborhood_fallback_to_nbhd_code():
    record = {"pin": "P003", "nbhd_code": "NB42"}
    result = _parse(record)
    assert result is not None
    assert result["neighborhood"] == "NB42"


def test_parse_lat_lon_latitude_longitude_fallback():
    record = {"pin": "P004", "latitude": "39.8", "longitude": "-105.0"}
    result = _parse(record)
    assert result is not None
    assert result["lat"] == 39.8
    assert result["lon"] == -105.0


def test_parse_assessor_value_actual_value_fallback():
    record = {"pin": "P005", "actual_value": "250000"}
    result = _parse(record)
    assert result is not None
    assert result["assessor_value"] == 250000.0


def test_parse_optional_fields_none_when_missing():
    record = {"pin": "P006"}
    result = _parse(record)
    assert result is not None
    assert result["address"] is None
    assert result["neighborhood"] is None
    assert result["lat"] is None
    assert result["lon"] is None
    assert result["assessor_value"] is None


# ── ParcelSeeder.run() (mocked _fetch_page + _bulk_upsert) ────────────────────

def test_parcel_seeder_run_empty(db):
    seeder = ParcelSeeder(db)
    with patch.object(seeder, "_fetch_page", return_value=[]):
        result = seeder.run()
    assert result == 0


def test_parcel_seeder_run_single_page(db):
    records = [{"pin": f"P{i:03d}", "property_address": f"{i} Main St"} for i in range(5)]
    seeder = ParcelSeeder(db)
    call_count = 0

    def fake_fetch(offset):
        nonlocal call_count
        call_count += 1
        return records if call_count == 1 else []

    with patch.object(seeder, "_fetch_page", side_effect=fake_fetch):
        with patch.object(seeder, "_bulk_upsert"):
            result = seeder.run()
    assert result == 5


def test_parcel_seeder_run_multi_page(db):
    page1 = [{"pin": f"P{i:04d}"} for i in range(SOCRATA_PAGE_SIZE)]
    page2 = [{"pin": f"P{i:04d}"} for i in range(SOCRATA_PAGE_SIZE, SOCRATA_PAGE_SIZE + 7)]
    seeder = ParcelSeeder(db)
    pages = [page1, page2]

    def fake_fetch(offset):
        return pages.pop(0) if pages else []

    with patch.object(seeder, "_fetch_page", side_effect=fake_fetch):
        with patch.object(seeder, "_bulk_upsert"):
            result = seeder.run()
    assert result == SOCRATA_PAGE_SIZE + 7


def test_parcel_seeder_run_skips_records_without_pin(db):
    records = [{"pin": "P001"}, {"pin": ""}, {}]
    seeder = ParcelSeeder(db)

    upserted_rows = []

    def fake_upsert(rows):
        upserted_rows.extend(rows)

    with patch.object(seeder, "_fetch_page", side_effect=[records, []]):
        with patch.object(seeder, "_bulk_upsert", side_effect=fake_upsert):
            seeder.run()

    assert len(upserted_rows) == 1
    assert upserted_rows[0]["parcel_id"] == "P001"


# ── App token header injection ─────────────────────────────────────────────────

def test_base_ingester_sets_app_token_header(db):
    """When denver_open_data_app_token is set, BaseIngester adds the header."""
    from unittest.mock import patch as _patch
    import ingestion.base as base_module

    with _patch.object(base_module.settings, "denver_open_data_app_token", "MY_TOKEN"):
        ingester = _TestIngester(db)

    assert ingester._headers.get("X-App-Token") == "MY_TOKEN"


def test_parcel_seeder_sets_app_token_header(db):
    """When denver_open_data_app_token is set, ParcelSeeder adds the header."""
    from unittest.mock import patch as _patch
    import ingestion.parcel_seed as seed_module

    with _patch.object(seed_module.settings, "denver_open_data_app_token", "SEED_TOKEN"):
        seeder = ParcelSeeder(db)

    assert seeder._headers.get("X-App-Token") == "SEED_TOKEN"


# ── ParcelSeeder._fetch_page ───────────────────────────────────────────────────

def test_parcel_seeder_fetch_page(db):
    """_fetch_page calls requests.get with the correct params and returns JSON."""
    expected_records = [{"pin": "P001"}]
    seeder = ParcelSeeder(db)

    with patch("requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.json.return_value = expected_records
        mock_get.return_value = mock_resp

        result = seeder._fetch_page(offset=0)

    assert result == expected_records
    call_kwargs = mock_get.call_args
    params = call_kwargs[1]["params"] if "params" in call_kwargs[1] else call_kwargs[0][1]
    assert params["$limit"] == SOCRATA_PAGE_SIZE
    assert params["$offset"] == 0
    assert "$where" in params
