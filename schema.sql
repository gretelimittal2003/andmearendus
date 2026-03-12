PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_capital (
    capital_id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL,
    capital_name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    population INTEGER,
    area REAL,
    UNIQUE(country_name, capital_name)
);

CREATE TABLE IF NOT EXISTS raw_weather_api (
    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL,
    capital_name TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    extracted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_daily_weather (
    weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_name TEXT NOT NULL,
    capital_name TEXT NOT NULL,
    weather_date TEXT NOT NULL,
    temperature_max_c REAL NOT NULL,
    temperature_min_c REAL NOT NULL,
    temperature_avg_c REAL NOT NULL,
    precipitation_sum_mm REAL NOT NULL,
    wind_speed_max_kmh REAL NOT NULL,
    sunshine_duration_hours REAL NOT NULL,
    UNIQUE(country_name, capital_name, weather_date)
);

DROP VIEW IF EXISTS vw_capitals_avg_temperature;
CREATE VIEW vw_capitals_avg_temperature AS
SELECT
    capital_name,
    country_name,
    ROUND(AVG(temperature_avg_c), 2) AS avg_temperature_c
FROM fact_daily_weather
GROUP BY capital_name, country_name
ORDER BY avg_temperature_c DESC;

DROP VIEW IF EXISTS vw_countries_most_rainfall;
CREATE VIEW vw_countries_most_rainfall AS
SELECT
    country_name,
    capital_name,
    ROUND(SUM(precipitation_sum_mm), 2) AS total_rainfall_mm
FROM fact_daily_weather
GROUP BY country_name, capital_name
ORDER BY total_rainfall_mm DESC;

DROP VIEW IF EXISTS vw_country_30_day_summary;
CREATE VIEW vw_country_30_day_summary AS
SELECT
    country_name,
    capital_name,
    COUNT(*) AS measured_days,
    ROUND(AVG(temperature_max_c), 2) AS avg_max_temp_c,
    ROUND(AVG(temperature_min_c), 2) AS avg_min_temp_c,
    ROUND(AVG(temperature_avg_c), 2) AS avg_temp_c,
    ROUND(SUM(precipitation_sum_mm), 2) AS total_rainfall_mm,
    ROUND(MAX(wind_speed_max_kmh), 2) AS peak_wind_kmh,
    ROUND(SUM(sunshine_duration_hours), 2) AS total_sunshine_hours
FROM fact_daily_weather
GROUP BY country_name, capital_name
ORDER BY country_name;
