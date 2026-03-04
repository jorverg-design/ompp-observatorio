import os
import sqlite3
from datetime import datetime, timezone
import requests

DB_PATH = os.getenv("DB_PATH", "ompp.sqlite")

EXCHANGE_URL = "https://open.er-api.com/v6/latest/USD"


def condb():
    return sqlite3.connect(DB_PATH)


def ensure_tables():
    con = condb()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS external_series (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        series TEXT NOT NULL,
        obs_date TEXT NOT NULL,
        value REAL,
        source TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_external_series
    ON external_series(series, obs_date)
    """)

    con.commit()
    con.close()


def upsert_series(series: str, obs_date: str, value: float, source: str):
    con = condb()
    cur = con.cursor()

    cur.execute(
        "DELETE FROM external_series WHERE series=? AND obs_date=?",
        (series, obs_date)
    )

    cur.execute("""
    INSERT INTO external_series(created_at, series, obs_date, value, source)
    VALUES(?,?,?,?,?)
    """, (
        datetime.now(timezone.utc).isoformat(),
        series,
        obs_date,
        value,
        source
    ))

    con.commit()
    con.close()


def fetch_usd_pyg():
    r = requests.get(EXCHANGE_URL, timeout=30)
    r.raise_for_status()

    data = r.json()

    pyg = float(data["rates"]["PYG"])

    obs_date = datetime.now(timezone.utc).date().isoformat()

    upsert_series(
        "USD_PYG",
        obs_date,
        pyg,
        "open.er-api.com"
    )


def energy_fx_main():
    ensure_tables()
    fetch_usd_pyg()
    print("USD updated successfully")


if __name__ == "__main__":
    energy_fx_main()
