"""Verixio Rating Engine — FastAPI application."""

import pathlib

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.routers import parcels

_UI_DIR = pathlib.Path(__file__).parent.parent / "ui"

app = FastAPI(
    title="Verixio Rating Engine",
    description=(
        "Deterministic parcel scoring system producing NTS, TCS, and VGD scores "
        "for every parcel in Denver."
    ),
    version="0.1.0",
)

app.include_router(parcels.router)

# Serve the single-page UI at /ui/
app.mount("/ui", StaticFiles(directory=str(_UI_DIR), html=True), name="ui")


@app.get("/", include_in_schema=False)
def root() -> FileResponse:
    """Redirect root to the parcel lookup UI."""
    return FileResponse(str(_UI_DIR / "index.html"))


@app.get("/health", tags=["health"])
def health() -> dict:
    return {"status": "ok"}
