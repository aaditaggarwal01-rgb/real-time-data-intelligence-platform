"""
load.py — Real-Time Data Intelligence Platform (Track A: Earthquake & Weather Alerts)

Week 4 deliverable for the Saras course project
"Data Pipelines with Databricks and Airflow".

Loads the raw JSON files in the landing zone (produced by src/ingest.py)
into the normalised 3NF tables defined in sql/create_tables.sql:

    places, earthquake_events, weather_observations, ingestion_runs

The loader is idempotent: re-running it over the same files changes
nothing. Earthquake rows are keyed by event_id (upsert, only replacing
an existing row when the incoming record is newer — the USGS `updated`
field) and weather rows are keyed by (event_id, observed_at).

Usage:
    python src/load.py [--landing-dir landing_zone] [--db project.db] [--run-id RUN]

The loader uses the Python standard library only (sqlite3). The SQL is
portable standard SQL, so the same statements work against PostgreSQL or
a Delta Lake Silver layer in Databricks with only the connection changing.
"""

import argparse
import glob
import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone

WEATHER_FILE_RE = re.compile(r"weather_(.+)_(\d{8}T\d{6}Z)\.json$")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load")


def iso_from_epoch_ms(epoch_ms):
    """Convert an epoch-milliseconds value to an ISO 8601 UTC timestamp."""
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def init_db(db_path):
    """Create (or open) the database and apply sql/create_tables.sql."""
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "sql", "create_tables.sql")
    with open(sql_path, encoding="utf-8") as fh:
        ddl = fh.read()
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(ddl)
    conn.commit()
    return conn


def load_earthquake_file(conn, path):
    """Load one USGS GeoJSON file; returns the number of events seen."""
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    features = data.get("features", [])
    for feature in features:
        props = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") or []
        longitude = coords[0] if len(coords) > 0 else None
        latitude = coords[1] if len(coords) > 1 else None
        depth_km = coords[2] if len(coords) > 2 else None
        place_name = props.get("place") or "Unknown"
        conn.execute(
            "INSERT INTO places (place_name, longitude, latitude) VALUES (?, ?, ?) "
            "ON CONFLICT(place_name) DO NOTHING",
            (place_name, longitude, latitude),
        )
        conn.execute(
            """
            INSERT INTO earthquake_events (
                event_id, place_name, occurred_at, updated_at, magnitude,
                magnitude_type, depth_km, longitude, latitude, status,
                tsunami, significance, event_type, event_url, source_network
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                place_name = excluded.place_name,
                occurred_at = excluded.occurred_at,
                updated_at = excluded.updated_at,
                magnitude = excluded.magnitude,
                magnitude_type = excluded.magnitude_type,
                depth_km = excluded.depth_km,
                longitude = excluded.longitude,
                latitude = excluded.latitude,
                status = excluded.status,
                tsunami = excluded.tsunami,
                significance = excluded.significance,
                event_type = excluded.event_type,
                event_url = excluded.event_url,
                source_network = excluded.source_network
            WHERE excluded.updated_at > earthquake_events.updated_at
            """,
            (
                feature.get("id"),
                place_name,
                iso_from_epoch_ms(props.get("time")),
                iso_from_epoch_ms(props.get("updated")),
                props.get("mag"),
                props.get("magType"),
                depth_km,
                longitude,
                latitude,
                props.get("status"),
                1 if props.get("tsunami") else 0,
                props.get("sig"),
                props.get("type"),
                props.get("url"),
                props.get("net"),
            ),
        )
    conn.commit()
    logger.info("Loaded %d events from %s", len(features), path)
    return len(features)


def load_weather_file(conn, path):
    """Load one Open-Meteo weather file; returns 1 if loaded, else 0."""
    match = WEATHER_FILE_RE.search(os.path.basename(path))
    if not match:
        logger.warning("Skipping %s: filename does not carry an event id", path)
        return 0
    event_id = match.group(1)
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    current = data.get("current") or {}
    conn.execute(
        """
        INSERT INTO weather_observations (
            event_id, observed_at, temperature_c, humidity_pct,
            wind_speed_kmh, precipitation_probability_pct, weather_code,
            timezone_name, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id, observed_at) DO NOTHING
        """,
        (
            event_id,
            current.get("time"),
            current.get("temperature_2m"),
            current.get("relative_humidity_2m"),
            current.get("wind_speed_10m"),
            current.get("precipitation_probability"),
            current.get("weather_code"),
            data.get("timezone"),
            datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        ),
    )
    conn.commit()
    logger.info("Loaded weather for %s from %s", event_id, path)
    return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load landing-zone JSON into the 3NF tables.")
    parser.add_argument("--landing-dir", default="landing_zone", help="Landing zone folder of raw files.")
    parser.add_argument("--db", default="project.db", help="SQLite database file to load into.")
    parser.add_argument("--run-id", default=None, help="Run id to record (default: current UTC timestamp).")
    args = parser.parse_args(argv)

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = init_db(args.db)

    eq_files = sorted(glob.glob(os.path.join(args.landing_dir, "usgs", "**", "*.geojson"), recursive=True))
    weather_files = sorted(glob.glob(os.path.join(args.landing_dir, "open_meteo", "**", "*.json"), recursive=True))
    events = sum(load_earthquake_file(conn, path) for path in eq_files)
    weather = sum(load_weather_file(conn, path) for path in weather_files)

    finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO ingestion_runs (run_id, started_at, finished_at, source_feed, events_fetched, weather_fetched, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, started_at, finished_at, "all_hour", events, weather, "success"),
    )
    conn.commit()
    logger.info("Run %s complete: %d events, %d weather rows", run_id, events, weather)
    return 0


if __name__ == "__main__":
    sys.exit(main())
