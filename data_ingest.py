
from __future__ import annotations
from typing import List, Tuple
import pandas as pd
import re
from pathlib import Path
from datetime import datetime
import sqlite3

import db as _db

APP_DIR = Path(__file__).resolve().parent

def sheet_to_site(sheet_name: str) -> str:
    s = sheet_name.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z_\-]", "", s)
    return s.lower()

def _detect_header_row(xlsx_path: str, sheet: str, max_rows: int = 8) -> int:
    df0 = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, nrows=max_rows)
    for i in range(min(max_rows, len(df0))):
        row = df0.iloc[i].astype(str).str.lower().tolist()
        if "ts" in row or "timestamp" in row or "日時" in row:
            return i
    return 0

def parse_timeseries_from_sheet(file_path: str, sheet_name: str, header_row: int | None = None) -> pd.DataFrame:
    if header_row is None:
        header_row = _detect_header_row(file_path, sheet_name)
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    cols = {c:str(c).strip().lower() for c in df.columns}
    df.columns = cols.values()
    # timestamp
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    elif {"date","time"}.issubset(df.columns):
        df["ts"] = pd.to_datetime(df["date"].astype(str) + " " + df["time"].astype(str), errors="coerce")
    else:
        first = next(iter(df.columns))
        df["ts"] = pd.to_datetime(df[first], errors="coerce")
    # numeric candidates
    for c in ["consumption_kwh","generation_kwh","surplus_kwh","price","avg_consumption_kwh"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = pd.NA
    if "final_bid_ok" not in df.columns:
        df["final_bid_ok"] = pd.NA
    keep = ["ts","consumption_kwh","generation_kwh","surplus_kwh","price","avg_consumption_kwh","final_bid_ok"]
    out = df[keep].dropna(subset=["ts"]).copy()
    out["ts"] = out["ts"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out

def to_rows_for_db(df: pd.DataFrame, site: str) -> List[Tuple]:
    rows = []
    for r in df.itertuples(index=False, name=None):
        ts, cons, gen, surp, price, avgc, ok = r
        rows.append((ts, sheet_to_site(site), cons, gen, surp, price, avgc, int(ok) if pd.notna(ok) else None))
    return rows

def ingest_all_sheets(file_path: str, db_path: str | None = None) -> int:
    if db_path is None:
        db_path = str(APP_DIR / "timeseries_all.db")
    _db.init_db(db_path)
    xls = pd.ExcelFile(file_path)
    total = 0
    for sh in xls.sheet_names:
        df = parse_timeseries_from_sheet(file_path, sh)
        rows = to_rows_for_db(df, sh)
        total += _db.insert_rows(rows, db_path=db_path)
    return total

def ingest_excel_to_separate_dbs(file_path: str) -> int:
    xls = pd.ExcelFile(file_path)
    total = 0
    for sh in xls.sheet_names:
        site = sheet_to_site(sh)
        db_path = str(APP_DIR / f"timeseries_{site}.db")
        _db.init_db(db_path)
        df = parse_timeseries_from_sheet(file_path, sh)
        rows = to_rows_for_db(df, sh)
        total += _db.insert_rows(rows, db_path=db_path)
    return total
