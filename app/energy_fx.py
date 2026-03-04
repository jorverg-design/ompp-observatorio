import os
import sqlite3
from datetime import datetime, timezone
import requests

DB_PATH = os.getenv("DB_PATH", "ompp.sqlite")

# USD -> PYG
EXCHANGE_URL = "https://open.er-api.com/v6/latest/USD"

# Stooq CSV endpoint (commodities)
# formato: Symbol,Date,Time,Open,High,Low,Close,Volume
STOOQ_CSV = "https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"


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
        float(value),
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


def fetch_stooq_close(sym: str):
    url = STOOQ_CSV.format(sym=sym)
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    lines = [ln.strip() for ln in r.text.splitlines() if ln.strip()]

    if len(lines) < 2:
        raise RuntimeError(f"Stooq sin datos para {sym}")

    cols = lines[1].split(",")

    if len(cols) < 7:
        raise RuntimeError(f"CSV inesperado para {sym}")

    return float(cols[6])


def safe_fetch(series_name, symbol):
    try:
        value = fetch_stooq_close(symbol)
        obs_date = datetime.now(timezone.utc).date().isoformat()

        upsert_series(
            series_name,
            obs_date,
            value,
            "stooq.com"
        )

        print(f"{series_name} updated")

    except Exception as e:
        print(f"[WARN] {series_name} failed: {e}")


def energy_fx_main():

    print("ENERGY FX GLOBAL UPDATE")

    ensure_tables()

    # Tipo de cambio
    fetch_usd_pyg()

    # Energía
    safe_fetch("BRENT_USD", "CB.F")
    safe_fetch("DIESEL_USD", "HO.F")
    safe_fetch("GASOLINE_USD", "RB.F")

    # Granos
    safe_fetch("WHEAT_USD", "ZW.F")
    safe_fetch("CORN_USD", "ZC.F")

    print("External indicators updated successfully")


if __name__ == "__main__":
    energy_fx_main()
