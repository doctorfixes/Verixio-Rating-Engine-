"""Denver Environmental / Hazmat Notices ingestion."""

from __future__ import annotations

from datetime import date
from typing import Any

from app.config import settings
from ingestion.base import BaseIngester


class EnvironmentalIngester(BaseIngester):
    source = "environmental"
    url = settings.environmental_url

    def _address(self, record: dict[str, Any]) -> str | None:
        return record.get("address") or record.get("site_address")

    def _category(self, record: dict[str, Any]) -> str:
        return "environmental"

    def _severity(self, record: dict[str, Any]) -> int:
        status = (record.get("status") or "").lower()
        if "active" in status or "open" in status:
            return 80
        return 40

    def _event_date(self, record: dict[str, Any]) -> date | None:
        raw = (
            record.get("open_date")
            or record.get("reported_date")
            or record.get("date_created")
        )
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
