"""Denver Building Permits ingestion."""

from __future__ import annotations

from datetime import date
from typing import Any

from app.config import settings
from ingestion.base import BaseIngester


class PermitsIngester(BaseIngester):
    source = "permits"
    url = settings.permits_url

    def _address(self, record: dict[str, Any]) -> str | None:
        return record.get("address")

    def _category(self, record: dict[str, Any]) -> str:
        return "permit"

    def _severity(self, record: dict[str, Any]) -> int:
        # Active / issued permits are positive signals → low severity score
        status = (record.get("permit_status") or "").lower()
        if status in {"issued", "approved"}:
            return 10
        if status in {"expired", "void", "cancelled"}:
            return 60
        return 30

    def _event_date(self, record: dict[str, Any]) -> date | None:
        raw = record.get("issue_date") or record.get("applied_date")
        if not raw:
            return None
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            return None
