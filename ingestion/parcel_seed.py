"""
Parcel seeder — populates the parcels table from Denver Real Property Valuations.

Denver Socrata dataset: Real Property Valuations
Endpoint: https://data.denvergov.org/resource/dn8v-f35q.json

Field mapping
─────────────
  Socrata field               → parcels column
  pin                         → parcel_id
  property_address            → address
  situs_full_nbhd_description → neighborhood  (falls back to nbhd_code)
  total_actual_value          → assessor_value
  lat                         → lat
  long                        → lon

Rows are upserted so the command is idempotent.
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.config import settings
from app.models.tables import Parcel
from ingestion.base import SOCRATA_PAGE_SIZE

logger = logging.getLogger(__name__)


class ParcelSeeder:
    """Fetch Denver Real Property Valuations and upsert into the parcels table."""

    def __init__(self, db: Session) -> None:
        self.db = db
        self._headers: dict[str, str] = {}
        if settings.denver_open_data_app_token:
            self._headers["X-App-Token"] = settings.denver_open_data_app_token

    # ── public entry point ────────────────────────────────────────────────────

    def run(self) -> int:
        """Upsert all pages of parcel records. Returns total rows processed."""
        total = 0
        offset = 0
        while True:
            records = self._fetch_page(offset)
            if not records:
                break
            rows = [r for r in (_parse(rec) for rec in records) if r is not None]
            if rows:
                self._bulk_upsert(rows)
                self.db.commit()
            total += len(records)
            logger.info("ParcelSeeder: upserted %d records (offset=%d)", len(rows), offset)
            if len(records) < SOCRATA_PAGE_SIZE:
                break
            offset += SOCRATA_PAGE_SIZE
        return total

    # ── internal helpers ──────────────────────────────────────────────────────

    def _fetch_page(self, offset: int) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "$limit": SOCRATA_PAGE_SIZE,
            "$offset": offset,
            "$order": ":id",
            # Only fetch rows that have a PIN (parcel identifier)
            "$where": "pin IS NOT NULL",
        }
        resp = requests.get(
            settings.parcel_seed_url,
            headers=self._headers,
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _bulk_upsert(self, rows: list[dict[str, Any]]) -> None:
        """
        Upsert a batch of parcel dicts in a single statement.

        On conflict (same parcel_id) NULL columns in the existing row are filled
        with the incoming value; non-NULL values are preserved, so re-running
        seed is safe.
        """
        from sqlalchemy import func

        stmt = pg_insert(Parcel).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["parcel_id"],
            set_={
                "address":        func.coalesce(Parcel.address,        stmt.excluded.address),
                "lat":            func.coalesce(Parcel.lat,            stmt.excluded.lat),
                "lon":            func.coalesce(Parcel.lon,            stmt.excluded.lon),
                "neighborhood":   func.coalesce(Parcel.neighborhood,   stmt.excluded.neighborhood),
                "assessor_value": func.coalesce(Parcel.assessor_value, stmt.excluded.assessor_value),
            },
        )
        self.db.execute(stmt)


# ── utility ────────────────────────────────────────────────────────────────────

def _parse(record: dict[str, Any]) -> dict[str, Any] | None:
    """Extract and type-coerce fields from a raw Socrata record."""
    parcel_id = (record.get("pin") or "").strip()
    if not parcel_id:
        return None

    address = (
        record.get("property_address")
        or record.get("situs_street_address")
        or ""
    ).strip() or None

    neighborhood = (
        record.get("situs_full_nbhd_description")
        or record.get("nbhd_code")
        or ""
    ).strip() or None

    return {
        "parcel_id":      parcel_id,
        "address":        address,
        "lat":            _to_float(record.get("lat") or record.get("latitude")),
        "lon":            _to_float(record.get("long") or record.get("longitude")),
        "neighborhood":   neighborhood,
        "assessor_value": _to_float(
            record.get("total_actual_value") or record.get("actual_value")
        ),
    }


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
