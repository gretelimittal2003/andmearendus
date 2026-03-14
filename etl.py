from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Iterable

import requests

RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/region/europe"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
DB_PATH = "weather.db"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_CALLS = 0.2

DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "sunshine_duration",
]


@dataclass(frozen=True)
class CapitalRecord:
    country_name: str
    capital_name: str
    latitude: float
    longitude: float
    population: int | None
    area: float | None


class ETLError(Exception):
    """Raised when the ETL pipeline cannot continue safely."""


class WeatherETL:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "telia-weather-etl/1.0"})

    def run(self) -> None:
        logging.info("Starting ETL pipeline")
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            self._create_schema(conn)
            capitals = self.extract_capitals()
            self.load_capitals(conn, capitals)

            start_date = date.today() - timedelta(days=30)
            end_date = date.today() - timedelta(days=1)
            logging.info("Fetching weather from %s to %s", start_date, end_date)

            for capital in capitals:
                weather_json = self.extract_weather(capital, start_date.isoformat(), end_date.isoformat())
                self.load_raw_weather(conn, capital, weather_json)
                cleaned_rows = self.transform_weather(capital, weather_json)
                self.load_clean_weather(conn, cleaned_rows)
                time.sleep(SLEEP_BETWEEN_CALLS)

            self.create_views(conn)
            self.verify(conn)
            logging.info("ETL pipeline finished successfully")
        finally:
            conn.close()
            self.session.close()

    def extract_capitals(self) -> list[CapitalRecord]:
        params = {
            "fields": "name,capital,capitalInfo,population,area",
        }
        response = self._get(RESTCOUNTRIES_URL, params=params)
        payload = response.json()
        capitals: list[CapitalRecord] = []

        for country in payload:
            capital_list = country.get("capital") or []
            coords = (country.get("capitalInfo") or {}).get("latlng")
            if not capital_list or not coords or len(coords) != 2:
                logging.warning("Skipping country with missing capital data: %s", country.get("name", {}))
                continue

            capitals.append(
                CapitalRecord(
                    country_name=country["name"]["common"],
                    capital_name=capital_list[0],
                    latitude=float(coords[0]),
                    longitude=float(coords[1]),
                    population=country.get("population"),
                    area=country.get("area"),
                )
            )

        capitals.sort(key=lambda x: x.country_name)
        logging.info("Extracted %s European capitals", len(capitals))
        return capitals

    def extract_weather(self, capital: CapitalRecord, start_date: str, end_date: str) -> dict[str, Any]:
        params = {
            "latitude": capital.latitude,
            "longitude": capital.longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(DAILY_FIELDS),
            "timezone": "auto",
            "wind_speed_unit": "kmh",
            "temperature_unit": "celsius",
            "precipitation_unit": "mm",
        }
        response = self._get(OPEN_METEO_URL, params=params)
        return response.json()

    def transform_weather(self, capital: CapitalRecord, weather_json: dict[str, Any]) -> list[tuple[Any, ...]]:
        daily = weather_json.get("daily")
        if not daily:
            raise ETLError(f"Missing 'daily' section for {capital.capital_name}")

        times = daily.get("time", [])
        max_temps = daily.get("temperature_2m_max", [])
        min_temps = daily.get("temperature_2m_min", [])
        precipitation = daily.get("precipitation_sum", [])
        wind = daily.get("wind_speed_10m_max", [])
        sunshine = daily.get("sunshine_duration", [])

        lengths = {len(times), len(max_temps), len(min_temps), len(precipitation), len(wind), len(sunshine)}
        if len(lengths) != 1:
            raise ETLError(f"Mismatched daily array lengths for {capital.capital_name}")

        cleaned_rows: list[tuple[Any, ...]] = []
        for day, t_max, t_min, rain, wind_max, sun_seconds in zip(
            times, max_temps, min_temps, precipitation, wind, sunshine, strict=True
        ):
            if None in (t_max, t_min, rain, wind_max, sun_seconds):
                logging.warning("Skipping incomplete weather row for %s on %s", capital.capital_name, day)
                continue

            cleaned_rows.append(
                (
                    capital.country_name,
                    capital.capital_name,
                    day,
                    round(float(t_max), 2),
                    round(float(t_min), 2),
                    round((float(t_max) + float(t_min)) / 2, 2),
                    round(float(rain), 2),
                    round(float(wind_max), 2),
                    round(float(sun_seconds) / 3600, 2),
                )
            )

        return cleaned_rows

    def load_capitals(self, conn: sqlite3.Connection, capitals: Iterable[CapitalRecord]) -> None:
        conn.executemany(
            """
            INSERT INTO dim_capital (country_name, capital_name, latitude, longitude, population, area)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(country_name, capital_name) DO UPDATE SET
                latitude=excluded.latitude,
                longitude=excluded.longitude,
                population=excluded.population,
                area=excluded.area
            """,
            [
                (c.country_name, c.capital_name, c.latitude, c.longitude, c.population, c.area)
                for c in capitals
            ],
        )
        conn.commit()

    def load_raw_weather(self, conn: sqlite3.Connection, capital: CapitalRecord, weather_json: dict[str, Any]) -> None:
        conn.execute(
            """
            INSERT INTO raw_weather_api (country_name, capital_name, payload_json, extracted_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (capital.country_name, capital.capital_name, json.dumps(weather_json)),
        )
        conn.commit()

    def load_clean_weather(self, conn: sqlite3.Connection, cleaned_rows: list[tuple[Any, ...]]) -> None:
        conn.executemany(
            """
            INSERT INTO fact_daily_weather (
                country_name,
                capital_name,
                weather_date,
                temperature_max_c,
                temperature_min_c,
                temperature_avg_c,
                precipitation_sum_mm,
                wind_speed_max_kmh,
                sunshine_duration_hours
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(country_name, capital_name, weather_date) DO UPDATE SET
                temperature_max_c=excluded.temperature_max_c,
                temperature_min_c=excluded.temperature_min_c,
                temperature_avg_c=excluded.temperature_avg_c,
                precipitation_sum_mm=excluded.precipitation_sum_mm,
                wind_speed_max_kmh=excluded.wind_speed_max_kmh,
                sunshine_duration_hours=excluded.sunshine_duration_hours
            """,
            cleaned_rows,
        )
        conn.commit()

    def create_views(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
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
            """
        )
        conn.commit()

    def verify(self, conn: sqlite3.Connection) -> None:
        capital_count = conn.execute("SELECT COUNT(*) AS count FROM dim_capital").fetchone()["count"]
        weather_count = conn.execute("SELECT COUNT(*) AS count FROM fact_daily_weather").fetchone()["count"]
        missing = conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM fact_daily_weather
            WHERE temperature_max_c < temperature_min_c
            """
        ).fetchone()["count"]

        logging.info("Verification: %s capitals loaded", capital_count)
        logging.info("Verification: %s clean weather rows loaded", weather_count)
        if missing:
            raise ETLError(f"Verification failed: found {missing} rows where max temp < min temp")

    def _create_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
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
            """
        )
        conn.commit()

    def _get(self, url: str, params: dict[str, Any]) -> requests.Response:
        try:
            response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            raise ETLError(f"Request failed for {url}: {exc}") from exc


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    WeatherETL().run()


if __name__ == "__main__":
    main()
