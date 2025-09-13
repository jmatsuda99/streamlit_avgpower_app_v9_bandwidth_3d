
import sqlite3, os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, List, Tuple, Optional

APP_DIR = Path(__file__).resolve().parent
DB_PATH = str(APP_DIR / "timeseries_all.db")

SCHEMA = '''
CREATE TABLE IF NOT EXISTS timeseries(
  ts TEXT NOT NULL,
  site TEXT NOT NULL,
  consumption_kWh REAL,
  generation_kWh REAL,
  surplus_kWh REAL,
  price REAL,
  avg_consumption_kWh REAL,
  final_bid_ok INTEGER
);
CREATE INDEX IF NOT EXISTS idx_ts ON timeseries(ts);
CREATE INDEX IF NOT EXISTS idx_site_ts ON timeseries(site, ts);
'''

@contextmanager
def get_conn(db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def init_db(db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.executescript(SCHEMA)

def reset_db(db_path: str = DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS timeseries")
    init_db(db_path)

def insert_rows(rows: Iterable[Tuple], db_path: str = DB_PATH) -> int:
    rows = list(rows)
    if not rows:
        return 0
    with get_conn(db_path) as conn:
        conn.executemany(
            "INSERT INTO timeseries(ts, site, consumption_kWh, generation_kWh, surplus_kWh, price, avg_consumption_kWh, final_bid_ok) VALUES(?,?,?,?,?,?,?,?)",
            rows
        )
    return len(rows)

def list_sites(db_path: str = DB_PATH) -> List[str]:
    with get_conn(db_path) as conn:
        cur = conn.execute("SELECT DISTINCT site FROM timeseries ORDER BY site")
        return [r[0] for r in cur.fetchall()]

def query_timeseries(site: str, start_ts: str, end_ts: str, db_path: str = DB_PATH):
    sql = '''
    SELECT ts, site, consumption_kWh, generation_kWh, surplus_kWh, price, avg_consumption_kWh, final_bid_ok
    FROM timeseries
    WHERE site = ? AND ts BETWEEN ? AND ?
    ORDER BY ts ASC
    '''
    with get_conn(db_path) as conn:
        cur = conn.execute(sql, (site, start_ts, end_ts))
        return cur.fetchall()
