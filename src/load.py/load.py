"""
load.py — load raw landing-zone JSON into the 3NF tables (sql/create_tables.sql).

Week 4 deliverable. Reads the GeoJSON files saved by src/ingest.py
(landing_zone/usgs/<date>/*.geojson) and the weather files
(landing_zone/open_meteo/<date>/*.json) and upserts them into the
places, earthquake_events, weather_observations and ingestion_runs tables.

Runs against SQLite for the local demo (python src/load.py --db project.db);
the same DDL and upsert statements run unchanged on PostgreSQL and, with
Delta Lake MERGE, on Databricks (Silver layer, Week 5).

Deduplication (see docs/Data_Quality_Checks.md):
- earthquake_events: upsert on the USGS event id; a row is only updated
  when the incoming `updated` timestamp is newer than the stored one.
- weather_observations: upsert on (event_id, observed_at).
- ingestion_runs: one row per run, keyed by run id; re-running the loader
  for the same files is a no-op for unchanged rows.
"""

import argparse
import glob
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load")

WEATHER_FILE_RE = re.compile(r"weather_(.+)_(\d{8}T\d{6}Z)\.json$")


def iso_from_epoch_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def init_db(conn):
    """Create the 3NF tables from sql/create_tables.sql."""
    ddl_path = os.path.join(os.path.dirname(__file__), "..", "sql", "create_tables.sql")
    with open(ddl_path, encoding="utf-8") as fh:
        conn.executescript(fh.read())
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def load_earthquake_file(conn, path):
    """Upsert one USGS GeoJSON file. Returns the number of events seen."""
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    features = data.get("features", [])
    for feature in features:
        props = feature.get("properties", {})
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") or [None, None, None]
        longitude, latitude, depth = coords[0], coords[1], coords[2]
        place = props.get("place") or "Unknown"
        conn.execute(
            "INSERT INTO places (place_name, longitude, latitude) VALUES (?, ?, ?) "
            "ON CONFLICT (place_name) DO NOTHING",
            (place, longitude, latitude),
        )
        conn.execute(
            "INSERT INTO earthquake_events "
            "(event_id, place_name, occurred_at, updated_at, magnitude, magnitude_type, "
            " depth_km, longitude, latitude, status, tsunami, significance, "
            " event_type, event_url, source_network) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (event_id) DO UPDATE SET "
            " place_name = excluded.place_name, occurred_at = excluded.occurred_at, "
            " updated_at = excluded.updated_at, magnitude = excluded.magnitude, "
            " magnitude_type = excluded.magnitude_type, depth_km = excluded.depth_km, "
            " longitude = excluded.longitude, latitude = excluded.latitude, "
            " status = excluded.status, tsunami = excluded.tsunami, "
            " significance = excluded.significance, event_type = excluded.event_type, "
            " event_url = excluded.event_url, source_network = excluded.source_network "
            "WHERE excluded.updated_at > earthquake_events.updated_at",
            (
                feature.get("id"),
                place,
                iso_from_epoch_ms(props["time"]) if props.get("time") else None,
                iso_from_epoch_ms(props["updated"]) if props.get("updated") else None,
                props.get("mag"),
                props.get("magType"),
                depth,
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
    logger.info("Loaded %d events from %s", len(features), path)
    return len(features)


def load_weather_file(conn, path):
    """Upsert one Open-Meteo weather file. Returns 1 on success, 0 if skipped."""
    match = WEATHER_FILE_RE.search(os.path.basename(path))
    if not match:
        logger.warning("Skipping %s: filename does not carry an event id", path)
        return 0
    event_id = match.group(1)
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    current = data.get("current") or {}
    conn.execute(
        "INSERT INTO weather_observations "
        "(event_id, observed_at, temperature_c, humidity_pct, wind_speed_kmh, "
        " precipitation_probability_pct, weather_code, timezone_name, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (event_id, observed_at) DO UPDATE SET "
        " temperature_c = excluded.temperature_c, humidity_pct = excluded.humidity_pct, "
        " wind_speed_kmh = excluded.wind_speed_kmh, "
        " precipitation_probability_pct = excluded.precipitation_probability_pct, "
        " weather_code = excluded.weather_code, timezone_name = excluded.timezone_name, "
        " fetched_at = excluded.fetched_at",
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
    logger.info("Loaded weather for event %s from %s", event_id, path)
    return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load landing-zone JSON into the 3NF tables.")
    parser.add_argument("--landing-dir", default="landing_zone", help="Landing zone folder.")
    parser.add_argument("--db", default="project.db", help="SQLite database file for the demo.")
    parser.add_argument("--run-id", default=None, help="Run id for ingestion_runs (default: now).")
    args = parser.parse_args(argv)

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = sqlite3.connect(args.db)
    try:
        init_db(conn)
        eq_files = sorted(glob.glob(os.path.join(args.landing_dir, "usgs", "**", "*.geojson"), recursive=True))
        wx_files = sorted(glob.glob(os.path.join(args.landing_dir, "open_meteo", "**", "*.json"), recursive=True))
        events = sum(load_earthquake_file(conn, path) for path in eq_files)
        weather = sum(load_weather_file(conn, path) for path in wx_files)
        conn.execute(
            "INSERT INTO ingestion_runs "
            "(run_id, started_at, finished_at, source_feed, events_fetched, weather_fetched, status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (run_id) DO UPDATE SET finished_at = excluded.finished_at, "
            "events_fetched = excluded.events_fetched, weather_fetched = excluded.weather_fetched, "
            "status = excluded.status",
            (run_id, started, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "all_hour", events, weather, "complete"),
        )
        conn.commit()
        logger.info("Load complete: %d events, %d weather rows (run %s)", events, weather, run_id)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
