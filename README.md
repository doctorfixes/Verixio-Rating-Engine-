# Verixio Rating Engine (VRE)

A deterministic parcel-intelligence platform that ingests real Denver open data,
scores every parcel on three composite dimensions, and surfaces meaningful changes
via a Change Radar.

---

## Scores

| Score | Full Name | Formula |
|-------|-----------|---------|
| **NTS** | Neighborhood Trajectory Score | `0.45·PS + 0.35·ZS + 0.20·FS` |
| **TCS** | Threat & Compliance Score | `0.40·FS + 0.35·CS + 0.25·ES` |
| **VGD** | Value–Growth Divergence | `market_value_percentile − (0.55·NTS + 0.45·TCS)` |

**Input scores** (all 0–100):

| Symbol | Name | Source |
|--------|------|--------|
| PS | Permit Score | Denver building-permit activity |
| ZS | Zoning Score | Zoning-change frequency |
| FS | Friction Score | 311 service-request complaints |
| CS | Crime Score | Denver crime incidents |
| ES | Environmental Score | Hazmat / environmental notices |

---

## Project Layout

```
.
├── app/
│   ├── main.py          # FastAPI app + static UI mount
│   ├── config.py        # Pydantic settings (reads .env)
│   ├── database.py      # SQLAlchemy engine + session
│   ├── models/
│   │   └── tables.py    # ORM models
│   ├── routers/
│   │   └── parcels.py   # /parcel/* endpoints
│   └── schemas/
│       └── parcel.py    # Pydantic response schemas
├── change_radar/
│   └── radar.py         # Change Radar detection logic
├── ingestion/
│   ├── base.py          # BaseIngester (Socrata pagination)
│   ├── permits.py       # Denver building permits
│   ├── complaints.py    # Denver 311 service requests
│   ├── crime.py         # Denver crime incidents
│   └── environmental.py # Denver hazmat / env notices
├── migrations/
│   └── 001_init.sql     # Full schema + indexes
├── scoring/
│   ├── formulas.py      # Pure scoring functions
│   └── engine.py        # DB-backed score orchestrator
├── tests/               # pytest test suite
├── ui/
│   └── index.html       # Single-page parcel lookup UI
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run.py               # CLI: ingest / score / radar / all
```

---

## Quick Start

### 1 — Configure environment

```bash
cp .env.example .env
# Edit DATABASE_URL and optionally DENVER_OPEN_DATA_APP_TOKEN
```

### 2 — Start with Docker Compose (recommended)

```bash
docker compose up --build
```

This starts a local Postgres instance (port 5432), applies the schema
automatically via the Docker entrypoint, and launches the API on port 8000.

### 3 — Or run locally with a Postgres/Supabase URL

```bash
pip install -r requirements.txt
# Apply the schema once:
psql "$DATABASE_URL" -f migrations/001_init.sql
# Start the API:
uvicorn app.main:app --reload
```

---

## CLI — Ingestion, Scoring & Radar

```bash
# Ingest all four Denver datasets
python run.py ingest

# Ingest a single source
python run.py ingest permits
python run.py ingest complaints
python run.py ingest crime
python run.py ingest environmental

# Score every parcel
python run.py score

# Run Change Radar (detect new events → emit alerts)
python run.py radar

# Full pipeline: ingest → score → radar
python run.py all
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Parcel lookup UI |
| `GET` | `/health` | Health check |
| `GET` | `/parcel/{id}` | Parcel metadata + current scores |
| `GET` | `/parcel/{id}/scores` | NTS, TCS, VGD + all five input scores |
| `GET` | `/parcel/{id}/events` | Normalised events (newest first) |
| `GET` | `/parcel/{id}/changes` | Change Radar alerts (newest first) |
| `GET` | `/docs` | Interactive API docs (Swagger UI) |

---

## UI

Open `http://localhost:8000/` in a browser. Enter a parcel ID and the page
fetches all four API endpoints in parallel, rendering:

- NTS, TCS, VGD and all five input scores
- Recent normalised events with category badges
- Change Radar alerts

---

## Change Radar

`run_change_radar()` scans events ingested within the last day and emits
typed alerts into the `change_radar` table:

| `change_type` | Trigger |
|---------------|---------|
| `new_permit` | Any new permit event |
| `zoning_change` | Any new zoning-change event |
| `311_spike` | Today's complaints > 2 × 30-day daily average |
| `environmental_notice` | Any new environmental event |

Duplicate alerts (same parcel + type + date) are suppressed.

---

## Database Schema

```sql
parcels          -- parcel master table
events_raw       -- raw JSON payloads from Socrata
events_normalized -- cleaned, categorised events
parcel_scores    -- PS/ZS/FS/CS/ES/NTS/TCS/VGD per parcel
change_radar     -- Change Radar alerts
```

Full DDL: [`migrations/001_init.sql`](migrations/001_init.sql)

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql://user:password@localhost:5432/verixio` | Postgres connection string |
| `DENVER_OPEN_DATA_APP_TOKEN` | *(empty)* | Optional Socrata app token for higher rate limits |

---

## Tests

```bash
python -m pytest tests/ -v
```

The suite uses an in-memory SQLite database and covers:
- All scoring formulas (`test_scoring.py`)
- All four FastAPI endpoints (`test_api.py`)
- Change Radar detection logic (`test_change_radar.py`)
- Scoring engine + batch pipeline (`test_engine.py`)

---

## Deployment

### Railway / Render / Fly.io

1. Push the repo and link it in your hosting dashboard.
2. Set the `DATABASE_URL` environment variable to your managed Postgres instance.
3. Apply the schema once: `psql "$DATABASE_URL" -f migrations/001_init.sql`
4. The service starts with: `uvicorn app.main:app --host 0.0.0.0 --port 8000`

### Docker

```bash
docker build -t verixio-vre .
docker run -e DATABASE_URL="..." -p 8000:8000 verixio-vre
```

