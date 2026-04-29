from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Double,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Parcel(Base):
    __tablename__ = "parcels"

    parcel_id: Mapped[str] = mapped_column(Text, primary_key=True)
    address: Mapped[Optional[str]] = mapped_column(Text)
    lat: Mapped[Optional[float]] = mapped_column(Double)
    lon: Mapped[Optional[float]] = mapped_column(Double)
    neighborhood: Mapped[Optional[str]] = mapped_column(Text)
    assessor_value: Mapped[Optional[float]] = mapped_column(Numeric)
    last_sale_date: Mapped[Optional[date]] = mapped_column(Date)
    last_sale_price: Mapped[Optional[float]] = mapped_column(Numeric)

    scores: Mapped[Optional["ParcelScore"]] = relationship(
        back_populates="parcel", uselist=False
    )
    events: Mapped[list["EventNormalized"]] = relationship(
        back_populates="parcel"
    )
    changes: Mapped[list["ChangeRadar"]] = relationship(
        back_populates="parcel"
    )


class EventRaw(Base):
    __tablename__ = "events_raw"

    # Use Integer (SQLite-compatible) — migration uses BIGSERIAL for Postgres
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[Optional[str]] = mapped_column(Text)
    raw_payload: Mapped[Optional[dict]] = mapped_column(JSON)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )


class EventNormalized(Base):
    __tablename__ = "events_normalized"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[Optional[str]] = mapped_column(
        Text, ForeignKey("parcels.parcel_id")
    )
    source: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[Optional[str]] = mapped_column(Text)
    severity: Mapped[Optional[int]] = mapped_column(Integer)
    event_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    parcel: Mapped[Optional["Parcel"]] = relationship(
        back_populates="events"
    )


class ParcelScore(Base):
    __tablename__ = "parcel_scores"

    parcel_id: Mapped[str] = mapped_column(
        Text, ForeignKey("parcels.parcel_id"), primary_key=True
    )
    permit_score: Mapped[Optional[int]] = mapped_column(Integer)
    zoning_score: Mapped[Optional[int]] = mapped_column(Integer)
    friction_score: Mapped[Optional[int]] = mapped_column(Integer)
    crime_score: Mapped[Optional[int]] = mapped_column(Integer)
    environmental_score: Mapped[Optional[int]] = mapped_column(Integer)
    nts: Mapped[Optional[int]] = mapped_column(Integer)
    tcs: Mapped[Optional[int]] = mapped_column(Integer)
    vgd: Mapped[Optional[int]] = mapped_column(Integer)
    scored_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    parcel: Mapped["Parcel"] = relationship(back_populates="scores")


class ChangeRadar(Base):
    __tablename__ = "change_radar"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parcel_id: Mapped[Optional[str]] = mapped_column(
        Text, ForeignKey("parcels.parcel_id")
    )
    change_type: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    event_date: Mapped[Optional[date]] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    parcel: Mapped[Optional["Parcel"]] = relationship(
        back_populates="changes"
    )
