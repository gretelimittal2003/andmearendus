# Telia Data Engineer Internship – Weather ETL

A compact but production-minded Python ETL pipeline for the Telia home assignment.

## What it does

1. **Extracts** European countries and capital coordinates from RestCountries.
2. **Extracts** the last 30 full days of weather data for each capital from Open-Meteo Historical Weather API.
3. **Transforms** the raw response into clean daily facts.
4. **Loads** both raw JSON and cleaned rows into SQLite.
5. **Builds analytical SQL views** for ranking and summary reporting.
6. **Verifies** that the loaded data looks sane.

## Project structure

```text
.
├── etl.py            # Full ETL pipeline
├── schema.sql        # Database schema + analytical views
├── requirements.txt  # Python dependencies
├── .gitignore
└── README.md
```

## Why this solution is strong

- clean separation between **raw** and **clean** layers
- idempotent loads via `ON CONFLICT ... DO UPDATE`
- structured logging
- simple validation step after load
- explicit field selection for external APIs
- readable, internship-appropriate code without overengineering

## Setup

```bash
python -m venv .venv
source .venv/bin/activate   # macOS / Linux
# .venv\Scripts\activate    # Windows
pip install -r requirements.txt
```

## Run

```bash
python etl.py
```

This creates a local SQLite database:

```text
weather.db
```

## Main tables

### `dim_capital`
Reference data for countries and capitals.

### `raw_weather_api`
Stores the raw Open-Meteo JSON payload for traceability and debugging.

### `fact_daily_weather`
Cleaned daily weather facts by capital and date.

## Analytical views

### `vw_capitals_avg_temperature`
Capitals ranked by average 30-day temperature.

### `vw_countries_most_rainfall`
Countries/capitals ranked by total rainfall.

### `vw_country_30_day_summary`
One summary row per capital for the 30-day period.

## Example verification queries

```sql
SELECT COUNT(*) FROM dim_capital;
SELECT COUNT(*) FROM fact_daily_weather;
SELECT * FROM vw_capitals_avg_temperature LIMIT 10;
SELECT * FROM vw_countries_most_rainfall LIMIT 10;
SELECT * FROM vw_country_30_day_summary LIMIT 10;
```

## APIs used

- RestCountries region endpoint with filtered fields: `v3.1/region/europe?fields=...`
- Open-Meteo Historical Weather API `/v1/archive`

## Notes

- Sunshine duration is converted from **seconds to hours** during transform.
- The pipeline intentionally loads the **last 30 full days**, excluding today.
- SQLite was chosen because it is simple, local, and perfect for an internship assignment.

## Suggested GitHub repo title

`telia-weather-etl`
