
import pandas as pd
from typing import List, Tuple
from db import upsert_timeseries

AVG_COL_RAW = "一時調整力\n（３時間消費量30分平均値）"

def parse_timeseries_from_sheet(file_path: str, sheet_name: str, header_row: int = 18) -> pd.DataFrame:
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
    except Exception:
        return pd.DataFrame()

    if df.shape[1] < 2:
        return pd.DataFrame()

    date_col = df.columns[0]
    time_col = df.columns[1]

    try:
        df["date_filled"] = pd.to_datetime(df[date_col]).ffill()
        df["timestamp"] = pd.to_datetime(
            df["date_filled"].astype(str) + " " + df[time_col].astype(str),
            errors="coerce"
        )
    except Exception:
        return pd.DataFrame()

    col_map = {
        "消費電気量": "consumption_kWh",
        "発電量": "generation_kWh",
        "余剰": "surplus_kWh",
        "電力価格": "price",
        AVG_COL_RAW: "avg_consumption_kWh"
    }
    for src, dst in col_map.items():
        if src in df.columns:
            df[dst] = pd.to_numeric(df[src], errors="coerce")
        else:
            df[dst] = pd.NA

    final_cols = [c for c in df.columns if "最終入札可否" in str(c)]
    if final_cols:
        final_col = final_cols[0]
        df["final_bid_ok"] = df[final_col].ffill().astype(str)
    else:
        df["final_bid_ok"] = pd.NA

    out = df[["timestamp", "consumption_kWh", "generation_kWh", "surplus_kWh", "price", "avg_consumption_kWh", "final_bid_ok"]]
    out = out.dropna(subset=["timestamp"])
    return out

def to_rows_for_db(site: str, df: pd.DataFrame) -> List[Tuple]:
    rows = []
    for _, r in df.iterrows():
        ts_iso = pd.to_datetime(r["timestamp"]).isoformat()
        rows.append((
            site,
            ts_iso,
            (None if pd.isna(r["consumption_kWh"]) else float(r["consumption_kWh"])),
            (None if pd.isna(r["generation_kWh"]) else float(r["generation_kWh"])),
            (None if pd.isna(r["surplus_kWh"]) else float(r["surplus_kWh"])),
            (None if pd.isna(r["price"]) else float(r["price"])),
            (None if pd.isna(r["avg_consumption_kWh"]) else float(r["avg_consumption_kWh"])),
            (None if pd.isna(r["final_bid_ok"]) else str(r["final_bid_ok"])),
        ))
    return rows

def sheet_to_site(sheet_name: str) -> str:
    name = str(sheet_name)
    for token in ["シミュレーション", "（一次）", "(一次)", "地区", "地区地区"]:
        name = name.replace(token, "")
    return name.strip(" _-（）()")

def ingest_all_sheets(file_path: str) -> int:
    xls = pd.ExcelFile(file_path)
    total = 0
    for sh in xls.sheet_names:
        df = parse_timeseries_from_sheet(file_path, sh)
        if df.empty:
            continue
        site = sheet_to_site(sh) or sh
        rows = to_rows_for_db(site, df)
        if rows:
            upsert_timeseries(rows)
            total += len(rows)
    return total
