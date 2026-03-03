import os
import sqlite3

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = os.getenv("DB_PATH", "ompp.sqlite")

def is_postgres() -> bool:
    return DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

def connect():
    if is_postgres():
        import psycopg2
        # Railway suele requerir SSL
        return psycopg2.connect(DATABASE_URL, sslmode="require")
    return sqlite3.connect(DB_PATH)

def q(sql: str) -> str:
    """Convierte placeholders SQLite (?) a Postgres (%s) si hace falta."""
    if is_postgres():
        return sql.replace("?", "%s")
    return sql

def execute(sql: str, params=()):
    con = connect()
    cur = con.cursor()
    cur.execute(q(sql), params)
    con.commit()
    cur.close()
    con.close()

def fetchone(sql: str, params=()):
    con = connect()
    cur = con.cursor()
    cur.execute(q(sql), params)
    row = cur.fetchone()
    cur.close()
    con.close()
    return row

def fetchall(sql: str, params=()):
    con = connect()
    cur = con.cursor()
    cur.execute(q(sql), params)
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows
