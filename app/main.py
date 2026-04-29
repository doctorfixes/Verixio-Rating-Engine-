"""Verixio Rating Engine — FastAPI application."""

from fastapi import FastAPI

from app.routers import parcels

app = FastAPI(
    title="Verixio Rating Engine",
    description=(
        "Deterministic parcel scoring system producing NTS, TCS, and VGD scores "
        "for every parcel in Denver."
    ),
    version="0.1.0",
)

app.include_router(parcels.router)


@app.get("/health", tags=["health"])
def health() -> dict:
    return {"status": "ok"}
