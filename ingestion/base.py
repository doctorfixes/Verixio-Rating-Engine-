"""
Base class for Denver Open Data ingestion.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.config import settings
from app.models.tables import EventNormalized, EventRaw, Parcel

logger = logging.getLogger(__name__)

SOCRATA_PAGE_SIZE = 1000


class BaseIngester(ABC):
    """Fetch records from a Socrata endpoint, store raw + normalised rows."""

    source: str  # subclasses must set this
    url: str     # subclasses must set this

    def __init__(self, db: Session) -> None:
        self.db = db
        self._headers: dict[str, str] = {}
        if settings.denver_open_data_app_token:
            self._headers["X-App-Token"] = settings.denver_open_data_app_token

    # ── public entry point ────────────────────────────────────────────────────

    def run(self) -> int:
        """Ingest all pages. Returns total rows ingested."""
        total = 0
        offset = 0
        while True:
            records = self._fetch_page(offset)
            if not records:
                break
            for record in records:
                self._store_raw(record)
                self._normalise_and_store(record)
            self.db.commit()
            total += len(records)
            logger.info("%s: ingested %d records (offset=%d)", self.source, len(records), offset)
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
        }
        resp = requests.get(self.url, headers=self._headers, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _store_raw(self, record: dict[str, Any]) -> None:
        raw = EventRaw(source=self.source, raw_payload=record)
        self.db.add(raw)

    def _normalise_and_store(self, record: dict[str, Any]) -> None:
        parcel_id = self._resolve_parcel_id(record)
        if parcel_id is None:
            return
        event = EventNormalized(
            parcel_id=parcel_id,
            source=self.source,
            category=self._category(record),
            severity=self._severity(record),
            event_date=self._event_date(record),
        )
        self.db.add(event)

    def _resolve_parcel_id(self, record: dict[str, Any]) -> str | None:
        """
        Attempt to match a record to a parcel via address.
        Falls back to None (record is still stored in events_raw).
        """
        address = self._address(record)
        if not address:
            return None
        address_upper = address.upper().strip()
        # Simple substring match — good enough for MVP
        result = (
            self.db.query(Parcel)
            .filter(Parcel.address.ilike(f"%{address_upper}%"))
            .first()
        )
        return result.parcel_id if result else None

    # ── abstract interface ────────────────────────────────────────────────────

    @abstractmethod
    def _address(self, record: dict[str, Any]) -> str | None:
        """Extract a street address from the raw record."""

    @abstractmethod
    def _category(self, record: dict[str, Any]) -> str:
        """Return the normalised category string."""

    @abstractmethod
    def _severity(self, record: dict[str, Any]) -> int:
        """Return severity 0–100."""

    @abstractmethod
    def _event_date(self, record: dict[str, Any]):
        """Return a date object (or None)."""
