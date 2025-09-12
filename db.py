
import sqlite3, os
from contextlib import contextmanager
from pathlib import Path

DB_PATH = str(Path(__file__).resolve().parent / "timeseries.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS timeseries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    ts TEXT NOT NULL,
    consumption_kWh REAL,
    generation_kWh REAL,
    surplus_kWh REAL,
    price REAL,
    avg_consumption_kWh REAL,
    final_bid_ok TEXT,
    UNIQUE(site, ts)
);
CREATE INDEX IF NOT EXISTS idx_site_ts ON timeseries(site, ts);
"""

@contextmanager
def get_conn(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def init_db(db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        cur.execute("PRAGMA table_info(timeseries)")
        cols = [row[1] for row in cur.fetchall()]
        if "final_bid_ok" not in cols:
            cur.execute("ALTER TABLE timeseries ADD COLUMN final_bid_ok TEXT")

def reset_db(db_path: str = DB_PATH):
    if os.path.exists(db_path):
        os.remove(db_path)
    init_db(db_path)

def upsert_timeseries(rows, db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        cur.executemany(
            """
            INSERT INTO timeseries (site, ts, consumption_kWh, generation_kWh, surplus_kWh, price, avg_consumption_kWh, final_bid_ok)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site, ts) DO UPDATE SET
              consumption_kWh=excluded.consumption_kWh,
              generation_kWh=excluded.generation_kWh,
              surplus_kWh=excluded.surplus_kWh,
              price=excluded.price,
              avg_consumption_kWh=excluded.avg_consumption_kWh,
              final_bid_ok=excluded.final_bid_ok
            """,
            rows
        )

def query_timeseries(site: str, start_ts: str, end_ts: str, db_path: str = DB_PATH):
    with get_conn(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT ts, consumption_kWh, generation_kWh, surplus_kWh, price, avg_consumption_kWh, final_bid_ok
            FROM timeseries
            WHERE site=? AND ts >= ? AND ts < ?
            ORDER BY ts ASC
            """,
            (site, start_ts, end_ts)
        )
        rows = cur.fetchall()
    return rows
