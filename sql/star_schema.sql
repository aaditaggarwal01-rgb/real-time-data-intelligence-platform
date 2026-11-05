-- star_schema.sql — Week 5 star schema for analytics (Gold layer, Delta Lake)
-- Built from the 3NF Silver tables in create_tables.sql. This is the
-- Week 5 deliverable: one fact table (one row per earthquake event,
-- enriched with the weather at the time) and four dimensions.
-- The fact is populated by the Week 5 transform step from
-- earthquake_events + weather_observations.

CREATE TABLE IF NOT EXISTS dim_time (
    time_id     TEXT PRIMARY KEY,
    event_date  TEXT NOT NULL,
    event_hour  INTEGER NOT NULL,
    day_of_week TEXT NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    place_name TEXT PRIMARY KEY,
    longitude  REAL NOT NULL,
    latitude   REAL NOT NULL,
    region     TEXT
);

CREATE TABLE IF NOT EXISTS dim_magnitude_band (
    band_id       TEXT PRIMARY KEY,
    band_label    TEXT NOT NULL,
    min_magnitude REAL NOT NULL,
    max_magnitude REAL
);

INSERT INTO dim_magnitude_band (band_id, band_label, min_magnitude, max_magnitude) VALUES
    ('micro',    'Micro (< 2.0)',      -10.0, 2.0),
    ('minor',    'Minor (2.0-3.9)',     2.0,  4.0),
    ('light',    'Light (4.0-4.9)',     4.0,  5.0),
    ('moderate', 'Moderate (5.0-5.9)', 5.0,  6.0),
    ('strong',   'Strong (6.0-6.9)',   6.0,  7.0),
    ('major',    'Major (7.0+)',       7.0,  NULL)
ON CONFLICT (band_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_weather_condition (
    weather_code INTEGER PRIMARY KEY,
    description  TEXT NOT NULL
);

INSERT INTO dim_weather_condition (weather_code, description) VALUES
    (0, 'Clear sky'),
    (1, 'Mainly clear'), (2, 'Partly cloudy'), (3, 'Overcast'),
    (45, 'Fog'), (48, 'Depositing rime fog'),
    (51, 'Light drizzle'), (53, 'Moderate drizzle'), (55, 'Dense drizzle'),
    (61, 'Slight rain'), (63, 'Moderate rain'), (65, 'Heavy rain'),
    (71, 'Slight snow'), (73, 'Moderate snow'), (75, 'Heavy snow'),
    (80, 'Slight rain showers'), (81, 'Moderate rain showers'), (82, 'Violent rain showers'),
    (95, 'Thunderstorm'), (96, 'Thunderstorm with slight hail'), (99, 'Thunderstorm with heavy hail')
ON CONFLICT (weather_code) DO NOTHING;

CREATE TABLE IF NOT EXISTS fact_earthquake_events (
    event_id                      TEXT PRIMARY KEY,
    time_id                       TEXT NOT NULL REFERENCES dim_time (time_id),
    place_name                    TEXT NOT NULL REFERENCES dim_location (place_name),
    magnitude_band_id             TEXT NOT NULL REFERENCES dim_magnitude_band (band_id),
    weather_condition_code        INTEGER REFERENCES dim_weather_condition (weather_code),
    magnitude                     REAL,
    depth_km                      REAL,
    temperature_c                 REAL,
    wind_speed_kmh                REAL,
    precipitation_probability_pct REAL,
    tsunami                       INTEGER NOT NULL DEFAULT 0,
    significance                  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_fact_time ON fact_earthquake_events (time_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON fact_earthquake_events (place_name);
CREATE INDEX IF NOT EXISTS idx_fact_band ON fact_earthquake_events (magnitude_band_id);
