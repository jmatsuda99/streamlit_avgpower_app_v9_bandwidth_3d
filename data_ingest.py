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
from pathlib import Path

TARGET_SHEETS = [
    "武芸川地区シミュレーション (一次)",
    "極楽寺シミュレーション (一次)",
    "笠神地区シミュレーション (一次)",
    "土岐地区地区シミュレーション (一次)",
]

def _detect_header_row(xlsx_path, sheet, max_rows=500):
    df0 = pd.read_excel(xlsx_path, sheet_name=sheet, header=None, nrows=max_rows)
    for i in range(len(df0)):
        row = df0.iloc[i].astype(str).fillna("")
        row_str = " ".join(row.tolist())
        if "消費電気量" in row_str and "発電量" in row_str:
            return i
    return None

def _read_timeseries_table(xlsx_path, sheet):
    hdr = _detect_header_row(xlsx_path, sheet)
    if hdr is None:
        raise ValueError(f"Header row not found in sheet: {sheet}")
    df = pd.read_excel(xlsx_path, sheet_name=sheet, header=hdr)
    df.columns = [str(c).strip().replace("\\n", " ").replace("\n", " ") for c in df.columns]
    return df

def _normalize_timeseries(df: pd.DataFrame, site_name: str):
    # Identify columns
    col_date = next((c for c in df.columns if str(c).startswith("Unnamed: 0")), None)
    col_time = next((c for c in df.columns if str(c).startswith("Unnamed: 1")), None)
    col_cons = next((c for c in df.columns if "消費電気量" in str(c)), None)
    col_gen  = next((c for c in df.columns if "発電量" in str(c)), None)
    col_sur  = next((c for c in df.columns if "余剰" in str(c)), None)
    col_price_candidates = [c for c in df.columns if "価格" in str(c) or "JEPX" in str(c)]
    col_price = col_price_candidates[0] if col_price_candidates else None
    col_bid30 = next((c for c in df.columns if "入札可否" in str(c) and "30分" in str(c)), None)

    if not col_date or not col_time or not col_cons:
        raise ValueError("Required columns missing (date/time/consumption).")

    df2 = df[[col_date, col_time, col_cons]].copy()
    if col_gen: df2[col_gen] = df[col_gen]
    if col_sur: df2[col_sur] = df[col_sur]
    if col_price: df2[col_price] = df[col_price]
    if col_bid30: df2[col_bid30] = df[col_bid30]

    # Forward-fill date and parse timestamps
    df2[col_date] = pd.to_datetime(df2[col_date], errors="coerce")
    df2[col_date] = df2[col_date].ffill()
    # Parse times that may be strings like "0:30"
    # --- robust time parsing ---
    # Excel 時刻は「文字列 '0:30'」「datetime」「シリアル(0.5=12:00)」のいずれか
    tcol = df2[col_time]
    t_time = pd.to_datetime(tcol, errors="coerce").dt.time  # datetime/strings first
    # Fallback: numeric serial (fraction of day)
    mask_num = tcol.apply(lambda x: isinstance(x, (int, float))) & tcol.notna()
    if mask_num.any():
        frac = tcol.where(mask_num, np.nan).astype(float)  # 0.5 = 12:00
        secs = (frac * 24 * 3600).round().astype("Int64")
        h = (secs // 3600).astype("Int64")
        m = ((secs % 3600) // 60).astype("Int64")
        # Build time strings HH:MM
        tm_str = (h.astype(str).str.zfill(2) + ":" + m.astype(str).str.zfill(2))
        t_time = t_time.astype(object)
        t_time = pd.to_datetime(tm_str, format="%H:%M", errors="coerce").dt.time.where(t_time.isna(), t_time)
    df2[col_time] = t_time
    # Build timestamp
    ts = pd.to_datetime(df2[col_date].dt.date.astype(str) + " " + df2[col_time].astype(str), errors="coerce")
    df2 = df2.assign(timestamp=ts).dropna(subset=["timestamp"])

    df2 = df2.rename(columns={
        col_cons: "consumption_kWh",
        col_gen if col_gen else "generation_kWh": "generation_kWh",
        col_sur if col_sur else "surplus_kWh": "surplus_kWh",
        col_price if col_price else "price": "price",
        col_bid30 if col_bid30 else "入札可否": "final_bid_ok"
    })
    df2["avg_consumption_kWh"] = None

    keep = ["timestamp", "consumption_kWh", "generation_kWh", "surplus_kWh", "price", "avg_consumption_kWh", "final_bid_ok"]
    for c in keep:
        if c not in df2.columns:
            df2[c] = None
    out = df2[keep].copy()
    out.insert(0, "site", site_name)
    return out

def to_rows_for_db_multi(site: str, df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        ts_iso = pd.to_datetime(r["timestamp"]).isoformat()
        rows.append((
            site,
            ts_iso,
            None if pd.isna(r["consumption_kWh"]) else float(r["consumption_kWh"]),
            None if pd.isna(r["generation_kWh"]) else float(r["generation_kWh"]),
            None if pd.isna(r["surplus_kWh"]) else float(r["surplus_kWh"]),
            None if pd.isna(r["price"]) else float(r["price"]),
            None if pd.isna(r["avg_consumption_kWh"]) else float(r["avg_consumption_kWh"]),
            None if pd.isna(r["final_bid_ok"]) else str(r["final_bid_ok"]),
        ))
    return rows

def slugify(name: str):

    s = re.sub(r"[^\w\u3040-\u30FF\u4E00-\u9FFF]+", "_", name)
    return s.strip("_")

def ingest_excel_to_separate_dbs(xlsx_path: str, out_dir: str, _unused=None):
    """
    Read TARGET_SHEETS and create one SQLite DB each under out_dir.
    This function does NOT import from db to avoid import errors; it uses sqlite3 directly.
    """
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
        final_bid_ok TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_ts ON timeseries(ts);
    CREATE INDEX IF NOT EXISTS idx_site_ts ON timeseries(site, ts);
    """
    created = []
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    for sh in TARGET_SHEETS:
        try:
            df = _read_timeseries_table(xlsx_path, sh)
            norm = _normalize_timeseries(df, site_name=sh)
            rows = to_rows_for_db_multi(sh, norm)
        except Exception:
            continue
        db_name = f"timeseries_{slugify(sh)}.db"
        db_path = str(Path(out_dir) / db_name)
        # Write rows using sqlite3 directly
        with sqlite3.connect(db_path) as conn:
            conn.executescript(SCHEMA)
            conn.executemany("""
                INSERT INTO timeseries
                (site, ts, consumption_kWh, generation_kWh, surplus_kWh, price, avg_consumption_kWh, final_bid_ok)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            conn.commit()
        created.append((db_path, len(rows)))
    return created
