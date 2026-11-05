# Real-Time Data Intelligence Platform

Course project for the Saras course **"Data Pipelines with Databricks and Airflow"** (Weeks 4 & 5).

**Track A — Earthquake & Weather Alerts.** A real-time data pipeline that
ingests earthquake events from the USGS Earthquake API, enriches each
event with the current weather at its location from the Open-Meteo API,
and loads the result into a normalised 3NF model, ready for a Medallion
(Bronze / Silver / Gold) pipeline in Databricks with Delta Lake and
PySpark.

## Architecture

| Layer | Where | What |
| --- | --- | --- |
| Bronze | `landing_zone/` | Raw JSON exactly as returned by the APIs, one file per fetch, plus a run manifest |
| Silver | `sql/create_tables.sql` | Normalised 3NF tables (`places`, `earthquake_events`, `weather_observations`, `ingestion_runs`) |
| Gold | `sql/star_schema.sql` | Star schema (`dim_time`, `dim_location`, `dim_magnitude_band`, `dim_weather_condition`, `fact_earthquake_events`) — built in Week 5 |

## Repository structure

```
.
├── README.md
├── requirements.txt
├── src/
│   ├── ingest.py          # Fetches USGS + Open-Meteo into the landing zone
│   └── load.py            # Loads landing-zone JSON into the 3NF tables
├── sql/
│   ├── create_tables.sql  # 3NF Silver DDL (portable standard SQL)
│   └── star_schema.sql    # Week 5 Gold star schema
├── docs/
│   ├── ER_Diagram.md          # ER diagram (Mermaid) and 3NF notes
│   └── Data_Quality_Checks.md # Deduplication strategy and DQ checks
└── landing_zone/
    ├── usgs/2026-11-05/earthquakes_all_hour_sample.geojson
    └── open_meteo/2026-11-05/weather_nc75462742_sample.json
```

## Week 4 deliverables

- [x] `src/ingest.py` — fetches from the USGS and Open-Meteo APIs with
  error handling and retries with exponential backoff; saves raw
  JSON/GeoJSON into the landing zone.
- [x] `requirements.txt` — `requests>=2.31.0`.
- [x] Sample ingested JSON — see `landing_zone/` (trimmed extract of
  the live run of 5 November 2026; see below).
- [x] ER diagram in 3NF — `docs/ER_Diagram.md`.
- [x] `sql/create_tables.sql` — the 3NF DDL.
- [x] `src/load.py` — loads the raw JSON into the 3NF tables.
- [x] Deduplication strategy — `docs/Data_Quality_Checks.md`.

## How to run

```bash
pip install -r requirements.txt

# Ingest the past hour of earthquakes, with weather for up to 20 events
python src/ingest.py --landing-dir landing_zone --feed all_hour --max-events 20

# Load the landing zone into the 3NF tables (SQLite by default)
python src/load.py --landing-dir landing_zone --db project.db
```

No API key is needed: both the USGS and Open-Meteo APIs are free and
keyless.

## Live run evidence — 5 November 2026, 13:15 UTC

The ingestion was run for real in a Databricks Free Edition notebook
(`Project_Ingest_Run`) on 5 November 2026 at 13:15 UTC:

- **5 earthquake events** in the USGS `all_hour` feed.
- Sample events:
  - `nc75462742` — magnitude 0.75, "6 km WNW of The Geysers, CA",
    38.8057° N, 122.8148° W, depth 3.25 km, 12:46 UTC.
  - `hv75068896` — magnitude 1.7, "22 km N of Pāhala, Hawaii",
    19.4045° N, 155.4535° W, depth 5.77 km, 12:25 UTC.
- Weather fetched for `nc75462742`: **9.3 °C, humidity 50%, wind
  13.9 km/h, precipitation probability 0%, weather code 0 (Clear sky)**,
  observed 05:15 local time (America/Los_Angeles).
- The `requests` library is available in the Databricks serverless
  runtime, so `ingest.py` runs there unchanged.

Notes on the sample files in `landing_zone/`:

- They are a **trimmed extract** of that run: 2 of the 5 events are
  shown, and the weather file is for `nc75462742`. The feed metadata
  still reports the full count of 5.
- Open-Meteo returns the weather for the **nearest grid point**
  (38.795242, −122.81644), a few hundred metres from the event's own
  coordinates (38.8056678771973, −122.814834594727). Both are kept, so
  the offset is visible.

## Environment notes

- **Databricks Free Edition** (the Community Edition the lab refers to
  no longer exists). The Free Edition workspace has no classic cluster
  creation — compute is serverless — and the legacy DBFS paths
  `/FileStore` and `/tmp` are disabled in this Unity Catalog workspace.
  The ingestion run therefore saved its landing-zone files under the
  existing volume `/Volumes/workspace/default/iris_data/project_landing`
  (a new top-level volume cannot be created from a notebook; volumes
  are created through the Catalog UI).
- The same code runs locally with Python 3.11+ and `requests`.

## Week 5 plan

- Transformation scripts that populate the Gold star schema from the
  3NF Silver tables.
- Implement the pipeline in Databricks: Medallion layers with Delta
  Lake tables and PySpark transforms.
- Dashboard on the fact table and the 5-minute video demo of the
  end-to-end pipeline.
