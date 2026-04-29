"""Denver Crime Incidents ingestion."""

from __future__ import annotations

from datetime import date
from typing import Any

from app.config import settings
from ingestion.base import BaseIngester

# Serious offenses that elevate severity
HIGH_SEVERITY_OFFENSES = {
    "murder",
    "robbery",
    "assault",
    "burglary",
    "arson",
    "kidnapping",
}


class CrimeIngester(BaseIngester):
    source = "crime"
    url = settings.crime_url

    def _address(self, record: dict[str, Any]) -> str | None:
        return record.get("incident_address") or record.get("address")

    def _category(self, record: dict[str, Any]) -> str:
        return "crime"

    def _severity(self, record: dict[str, Any]) -> int:
        offense = (record.get("offense_category_id") or record.get("offense_type_id") or "").lower()
        if any(kw in offense for kw in HIGH_SEVERITY_OFFENSES):
            return 90
        return 50

    def _event_date(self, record: dict[str, Any]) -> date | None:
        raw = record.get("first_occurrence_date") or record.get("reported_date")
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
