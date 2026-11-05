-- create_tables.sql — Real-Time Data Intelligence Platform (Track A)
-- Third normal form (3NF) model for earthquake and weather data.
-- Written in portable standard SQL: it runs on PostgreSQL and on SQLite
-- (used by src/load.py for the local demo). In Databricks, the same
-- tables are created as Delta Lake tables in the Silver layer.
--
-- Design (3NF):
--   places               — one row per distinct place (normalised out of the event)
--   earthquake_events    — one row per USGS event (natural key: the USGS event id)
--   weather_observations — one row per weather reading at an event location
--   ingestion_runs       — one row per ingest.py run (audit / idempotency)
--
-- Every non-key attribute depends only on its table's primary key; there
-- are no transitive dependencies. The USGS "place" text is the only
-- attribute that repeats across events, so it lives in its own table.

CREATE TABLE IF NOT EXISTS places (
    place_name TEXT PRIMARY KEY,
    longitude  REAL NOT NULL,
    latitude   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS earthquake_events (
    event_id       TEXT PRIMARY KEY,
    place_name     TEXT NOT NULL REFERENCES places (place_name),
    occurred_at    TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    magnitude      REAL,
    magnitude_type TEXT,
    depth_km       REAL,
    status         TEXT,
    tsunami        INTEGER NOT NULL DEFAULT 0,
    significance   INTEGER,
    event_type     TEXT NOT NULL,
    event_url      TEXT,
    source_network TEXT
);

CREATE TABLE IF NOT EXISTS weather_observations (
    event_id                      TEXT NOT NULL REFERENCES earthquake_events (event_id),
    observed_at                   TEXT NOT NULL,
    temperature_c                 REAL,
    humidity_pct                  REAL,
    wind_speed_kmh                REAL,
    precipitation_probability_pct REAL,
    weather_code                  INTEGER,
    timezone_name                 TEXT,
    fetched_at                    TEXT NOT NULL,
    PRIMARY KEY (event_id, observed_at)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT NOT NULL,
    finished_at     TEXT,
    source_feed     TEXT NOT NULL,
    events_fetched  INTEGER NOT NULL DEFAULT 0,
    weather_fetched INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON earthquake_events (occurred_at);
CREATE INDEX IF NOT EXISTS idx_events_magnitude ON earthquake_events (magnitude);
CREATE INDEX IF NOT EXISTS idx_weather_event ON weather_observations (event_id);
