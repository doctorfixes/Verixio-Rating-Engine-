-- Verixio Rating Engine — initial schema
-- Run this against your Postgres / Supabase database

CREATE TABLE IF NOT EXISTS parcels (
    parcel_id       TEXT PRIMARY KEY,
    address         TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    neighborhood    TEXT,
    assessor_value  NUMERIC,
    last_sale_date  DATE,
    last_sale_price NUMERIC
);

CREATE TABLE IF NOT EXISTS events_raw (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT,
    raw_payload   JSONB,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS events_normalized (
    id          BIGSERIAL PRIMARY KEY,
    parcel_id   TEXT REFERENCES parcels(parcel_id),
    source      TEXT,
    category    TEXT,
    severity    INTEGER,
    event_date  DATE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS parcel_scores (
    parcel_id           TEXT PRIMARY KEY REFERENCES parcels(parcel_id),
    permit_score        INTEGER,
    zoning_score        INTEGER,
    friction_score      INTEGER,
    crime_score         INTEGER,
    environmental_score INTEGER,
    nts                 INTEGER,
    tcs                 INTEGER,
    vgd                 INTEGER,
    scored_at           TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS change_radar (
    id          BIGSERIAL PRIMARY KEY,
    parcel_id   TEXT REFERENCES parcels(parcel_id),
    change_type TEXT,
    description TEXT,
    event_date  DATE,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_events_normalized_parcel_id  ON events_normalized(parcel_id);
CREATE INDEX IF NOT EXISTS idx_events_normalized_event_date ON events_normalized(event_date);
CREATE INDEX IF NOT EXISTS idx_change_radar_parcel_id       ON change_radar(parcel_id);
CREATE INDEX IF NOT EXISTS idx_change_radar_created_at      ON change_radar(created_at);
