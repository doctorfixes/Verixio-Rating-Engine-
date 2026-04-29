"""Denver 311 Service Requests ingestion."""

from __future__ import annotations

from datetime import date
from typing import Any

from app.config import settings
from ingestion.base import BaseIngester


class Complaints311Ingester(BaseIngester):
    source = "311_complaints"
    url = settings.complaints_311_url

    def _address(self, record: dict[str, Any]) -> str | None:
        return record.get("incident_address") or record.get("address")

    def _category(self, record: dict[str, Any]) -> str:
        return "311_complaint"

    def _severity(self, record: dict[str, Any]) -> int:
        agency = (record.get("agency_responsible") or "").lower()
        # Nuisance/unsafe-structure complaints are higher severity
        if any(kw in agency for kw in ["nuisance", "code", "unsafe"]):
            return 70
        return 40

    def _event_date(self, record: dict[str, Any]) -> date | None:
        raw = record.get("service_request_date") or record.get("date_created")
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
