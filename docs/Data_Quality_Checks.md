# Data Quality Checks & Deduplication Strategy — Real-Time Data Intelligence Platform (Track A)

Week 4 deliverable for the Saras course project "Data Pipelines with Databricks and Airflow".

This document covers two things: how duplicate records are prevented
(deduplication strategy) and the checks that prove the loaded data is
correct (data quality checks). The checks are written as standard SQL
against the 3NF tables in `sql/create_tables.sql`, so they run unchanged
in SQLite, PostgreSQL or a Databricks SQL warehouse.

## Deduplication strategy

### Natural keys, not surrogate ids

- `earthquake_events` is keyed by `event_id`, the USGS event id
  (for example `nc75462742`). It is stable across feeds and re-runs, so
  the same event fetched twice is the same row.
- `weather_observations` is keyed by the pair (`event_id`,
  `observed_at`): one weather reading per event per observation time.
- `places` is keyed by `place_name`; a place that appears for many
  events is stored once.
- `ingestion_runs` is keyed by `run_id`, so every run is recorded once.

### Upserts (insert or update, never duplicate)

`src/load.py` uses upserts for every table:

- `places`: `INSERT ... ON CONFLICT(place_name) DO NOTHING` — the first
  coordinates seen for a place are kept.
- `earthquake_events`: `INSERT ... ON CONFLICT(event_id) DO UPDATE ...
  WHERE excluded.updated_at > earthquake_events.updated_at` — an event
  is only overwritten when the incoming record is newer. The USGS
  revises events after first publication (magnitude or location
  corrections), and `updated` is the field that tells us which version
  is newer.
- `weather_observations`: `INSERT ... ON CONFLICT(event_id, observed_at)
  DO NOTHING` — a weather reading for an event and time is loaded once.

### Idempotent re-runs

Re-running `src/ingest.py` and then `src/load.py` over the same landing
zone changes nothing in the tables: the same event ids upsert to the
same rows, and the same weather keys are skipped. Each run writes a
manifest (`landing_zone/_manifests/run_<stamp>.json`) and an
`ingestion_runs` row, so a re-run is visible in the audit trail even
though the data does not change.

### What can still go wrong

- Two different events can share a place name (for example repeated
  aftershocks near "6 km WNW of The Geysers, CA"). That is fine: the
  place is a lookup, and each event keeps its own observed coordinates.
- Open-Meteo returns the weather for the nearest grid point, which can
  be a few hundred metres from the event's own coordinates. The event
  coordinates and the weather coordinates are both kept, so the offset
  is visible rather than hidden.

## Data quality checks

Run these against the loaded database after `src/load.py`. Every check
states the expected result.

### 1. Row counts: landing zone vs loaded tables

The number of events loaded must equal the number of features in the
GeoJSON files, and the number of weather rows must equal the number of
weather files.

```sql
-- Events loaded
SELECT COUNT(*) AS events_loaded FROM earthquake_events;
-- Weather rows loaded
SELECT COUNT(*) AS weather_rows_loaded FROM weather_observations;
-- Runs recorded
SELECT run_id, events_fetched, weather_fetched, status FROM ingestion_runs;
```

Expected: `events_loaded` equals the sum of `features` across the
`landing_zone/usgs/**/*.geojson` files; `weather_rows_loaded` equals the
number of `landing_zone/open_meteo/**/*.json` files.

### 2. No duplicate event ids

```sql
SELECT event_id, COUNT(*) AS n
FROM earthquake_events
GROUP BY event_id
HAVING COUNT(*) > 1;
```

Expected: zero rows.

### 3. No nulls in required columns

Magnitude is nullable (the USGS sometimes reports an event before a
magnitude is assigned), but the identity and time of every event must
be present.

```sql
SELECT COUNT(*) AS null_event_id FROM earthquake_events WHERE event_id IS NULL;
SELECT COUNT(*) AS null_place FROM earthquake_events WHERE place_name IS NULL;
SELECT COUNT(*) AS null_occurred_at FROM earthquake_events WHERE occurred_at IS NULL;
SELECT COUNT(*) AS null_updated_at FROM earthquake_events WHERE updated_at IS NULL;
```

Expected: 0 for every count.

### 4. Range checks

```sql
SELECT COUNT(*) AS bad_magnitude FROM earthquake_events WHERE magnitude IS NOT NULL AND (magnitude < -1 OR magnitude > 10);
SELECT COUNT(*) AS bad_depth FROM earthquake_events WHERE depth_km IS NOT NULL AND (depth_km < -10 OR depth_km > 700);
SELECT COUNT(*) AS bad_latitude FROM earthquake_events WHERE latitude IS NOT NULL AND (latitude < -90 OR latitude > 90);
SELECT COUNT(*) AS bad_longitude FROM earthquake_events WHERE longitude IS NOT NULL AND (longitude < -180 OR longitude > 180);
SELECT COUNT(*) AS bad_humidity FROM weather_observations WHERE humidity_pct IS NOT NULL AND (humidity_pct < 0 OR humidity_pct > 100);
SELECT COUNT(*) AS bad_wind FROM weather_observations WHERE wind_speed_kmh IS NOT NULL AND wind_speed_kmh < 0;
SELECT COUNT(*) AS bad_precipitation FROM weather_observations WHERE precipitation_probability_pct IS NOT NULL AND (precipitation_probability_pct < 0 OR precipitation_probability_pct > 100);
```

Expected: 0 for every count.

### 5. Freshness

The feed metadata `generated` time and the event `occurred_at` times
must be close to the run time for a real-time feed.

```sql
SELECT MAX(occurred_at) AS latest_event, MIN(occurred_at) AS earliest_event FROM earthquake_events;
```

Expected: the latest event is within the feed window (one hour for
`all_hour`) of the run time.

### 6. Referential integrity

Every event must have a place row, and every weather row must belong to
a loaded event.

```sql
SELECT COUNT(*) AS events_without_place
FROM earthquake_events e
LEFT JOIN places p ON p.place_name = e.place_name
WHERE p.place_name IS NULL;

SELECT COUNT(*) AS weather_without_event
FROM weather_observations w
LEFT JOIN earthquake_events e ON e.event_id = w.event_id
WHERE e.event_id IS NULL;
```

Expected: 0 for both counts. (Foreign keys are declared in
`sql/create_tables.sql` and enforced with `PRAGMA foreign_keys = ON`
in `src/load.py`, so these counts are a second line of defence.)

### 7. Weather coverage

Every event enriched with weather should have exactly one weather row
for the observation time fetched.

```sql
SELECT COUNT(*) AS events_with_weather
FROM earthquake_events e
WHERE EXISTS (SELECT 1 FROM weather_observations w WHERE w.event_id = e.event_id);
```

Expected: equal to the number of weather files written by the run
(`weather_count` in the run manifest).

## Results on the sample run (5 November 2026)

The live run on 5 November 2026 (13:15 UTC) fetched 5 events from the
USGS `all_hour` feed and weather for the first event
(`nc75462742` — 9.3 °C, humidity 50%, wind 13.9 km/h, precipitation
probability 0%, weather code 0, Clear sky). The sample files in
`landing_zone/` are a trimmed extract of that run (2 of the 5 events),
so the checks above run against the trimmed sample give 2 events and
1 weather row.
