"""
ingest.py — Real-Time Data Intelligence Platform (Track A: Earthquake & Weather Alerts)

Week 4 deliverable for the Saras course project
"Data Pipelines with Databricks and Airflow".

Fetches real-time earthquake data from the USGS Earthquake API and the
current weather at each earthquake location from the Open-Meteo API, then
saves the raw JSON responses unchanged into the landing zone
(landing_zone/<source>/<date>/...). The raw files are the Bronze layer
input for the Databricks Medallion pipeline.

Usage:
    python src/ingest.py [--landing-dir landing_zone] [--feed all_hour] [--max-events 20]

Design notes:
- No API key is needed: both the USGS and Open-Meteo APIs are free and keyless.
- Every HTTP call is retried with exponential backoff (up to 4 attempts).
- Raw responses are stored verbatim; no transformation happens here.
- Each run writes a run manifest (landing_zone/_manifests/...) so re-runs
  are traceable and idempotent.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import requests

USGS_FEEDS = {
    "all_hour": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
    "all_day": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson",
    "all_week": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_week.geojson",
    "all_month": "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson",
}

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Retry policy: 4 attempts with 1s, 2s, 4s backoff between attempts.
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 1.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("ingest")


def fetch_json(url, params=None):
    """GET a JSON document, retrying with exponential backoff.

    Retries on connection errors, timeouts and 5xx responses. A 4xx
    response is a client error and is raised immediately.
    """
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_ATTEMPTS, url, exc)
        else:
            if response.status_code == 200:
                return response.json()
            if 400 <= response.status_code < 500:
                response.raise_for_status()
            logger.warning(
                "Attempt %d/%d: HTTP %s from %s", attempt, MAX_ATTEMPTS, response.status_code, url
            )
        if attempt < MAX_ATTEMPTS:
            time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch {url} after {MAX_ATTEMPTS} attempts")


def fetch_earthquakes(feed="all_hour"):
    """Fetch one USGS earthquake feed as a GeoJSON dict."""
    url = USGS_FEEDS[feed]
    logger.info("Fetching USGS feed %s", feed)
    data = fetch_json(url)
    features = data.get("features", [])
    logger.info("Fetched %d earthquake events", len(features))
    return data


def fetch_weather(latitude, longitude):
    """Fetch the current weather at one location from Open-Meteo."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation_probability,weather_code",
        "timezone": "auto",
    }
    return fetch_json(OPEN_METEO_URL, params=params)


def save_json(payload, path):
    """Write a JSON payload to path, creating parent folders."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    logger.info("Saved %s", path)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Ingest earthquakes and weather into the landing zone.")
    parser.add_argument("--landing-dir", default="landing_zone", help="Landing zone folder for raw files.")
    parser.add_argument("--feed", default="all_hour", choices=sorted(USGS_FEEDS), help="USGS feed to fetch.")
    parser.add_argument("--max-events", type=int, default=20, help="Maximum events to enrich with weather.")
    args = parser.parse_args(argv)

    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    usgs_dir = os.path.join(args.landing_dir, "usgs", run_date)
    weather_dir = os.path.join(args.landing_dir, "open_meteo", run_date)

    earthquakes = fetch_earthquakes(args.feed)
    eq_path = os.path.join(usgs_dir, f"earthquakes_{args.feed}_{run_stamp}.geojson")
    save_json(earthquakes, eq_path)

    features = earthquakes.get("features", [])[: args.max_events]
    weather_files = []
    for feature in features:
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") or []
        if len(coords) < 2:
            continue
        longitude, latitude = coords[0], coords[1]
        event_id = feature.get("id", "unknown")
        try:
            weather = fetch_weather(latitude, longitude)
        except Exception as exc:  # keep the run going if one lookup fails
            logger.warning("Weather lookup failed for %s: %s", event_id, exc)
            continue
        weather_path = os.path.join(weather_dir, f"weather_{event_id}_{run_stamp}.json")
        save_json(weather, weather_path)
        weather_files.append(weather_path)

    manifest = {
        "run": run_stamp,
        "feed": args.feed,
        "earthquake_file": eq_path,
        "earthquake_count": len(earthquakes.get("features", [])),
        "weather_files": weather_files,
        "weather_count": len(weather_files),
    }
    manifest_path = os.path.join(args.landing_dir, "_manifests", f"run_{run_stamp}.json")
    save_json(manifest, manifest_path)
    logger.info(
        "Ingestion complete: %d events, %d weather files",
        manifest["earthquake_count"],
        manifest["weather_count"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
