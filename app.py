
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import os

from data_ingest import sheet_to_site, ingest_all_sheets, ingest_excel_to_separate_dbs
from db import query_timeseries
import db as _db

st.set_page_config(page_title="AvgPower 3D", layout="wide")
APP_DIR = Path(__file__).resolve().parent

st.sidebar.header("Datasets (per-sheet DB)")
uploaded = st.sidebar.file_uploader("Upload Excel", type=["xlsx","xlsm"], accept_multiple_files=False)
colA, colB = st.sidebar.columns(2)
if colA.button("Ingest to 4 DBs (per sheet)", use_container_width=True, disabled=not uploaded):
    try:
        tmp = APP_DIR/"_tmp_ingest.xlsx"
        with open(tmp, "wb") as f: f.write(uploaded.getbuffer())
        n = ingest_excel_to_separate_dbs(str(tmp))
        st.sidebar.success(f"Ingested rows: {n}")
    except Exception as e:
        st.sidebar.error(f"Ingest error: {e}")
if colB.button("Ingest to 1 DB (all-in-one)", use_container_width=True, disabled=not uploaded):
    try:
        tmp = APP_DIR/"_tmp_ingest.xlsx"
        with open(tmp, "wb") as f: f.write(uploaded.getbuffer())
        n = ingest_all_sheets(str(tmp), db_path=str(APP_DIR/"timeseries_all.db"))
        st.sidebar.success(f"Ingested rows: {n}")
    except Exception as e:
        st.sidebar.error(f"Ingest error: {e}")

db_files = sorted(str(p) for p in APP_DIR.glob("timeseries_*.db"))
selected_db = st.sidebar.selectbox("Select Dataset (DB)", options=db_files or ["(no DBs found)"])
valid_db_selected = selected_db and selected_db.startswith(str(APP_DIR))

sites = []
if valid_db_selected:
    try:
        import sqlite3
        with sqlite3.connect(selected_db) as conn:
            sites = [r[0] for r in conn.execute("SELECT DISTINCT site FROM timeseries ORDER BY site").fetchall()]
            cur = conn.execute("SELECT MIN(ts), MAX(ts) FROM timeseries")
            min_ts, max_ts = cur.fetchone()
    except Exception as e:
        st.sidebar.warning(f"Site load error: {e}")
else:
    min_ts = max_ts = None

site_label = st.sidebar.selectbox("Site (Sheet)", options=sites or ["(no sites)"])
q_start = st.sidebar.date_input("Start date", value=(datetime.now()-timedelta(days=30)).date())
q_end   = st.sidebar.date_input("End date", value=datetime.now().date())

if valid_db_selected and site_label and site_label != "(no sites)":
    db_site = sheet_to_site(site_label)
    rows = query_timeseries(db_site, str(q_start), str(q_end + timedelta(days=1)), db_path=selected_db)
    if rows:
        df = pd.DataFrame(rows, columns=["ts","site","consumption_kWh","generation_kWh","surplus_kWh","price","avg_consumption_kWh","final_bid_ok"])
        df["ts"] = pd.to_datetime(df["ts"])
        df["hour"] = df["ts"].dt.hour + df["ts"].dt.minute/60.0
        metric = st.selectbox("Z metric", ["consumption_kWh","generation_kWh","surplus_kWh","price","avg_consumption_kWh"], index=0)
        x_vals = df["ts"].map(pd.Timestamp.toordinal)
        y_vals = df["hour"]
        z_vals = df[metric].astype(float)
        fig = go.Figure(data=[go.Scatter3d(x=x_vals, y=y_vals, z=z_vals, mode="markers")])
        fig.update_layout(height=600, scene=dict(
            xaxis_title="date (ordinal)",
            yaxis_title="hour",
            zaxis_title=metric
        ))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df[["ts","site",metric]].tail(100))
    else:
        st.info("No data found in DB for the selected site/range. Ingest the Excel first or adjust the filters.")

st.markdown("---")
with st.expander("DB Diagnostics", expanded=False):
    if valid_db_selected:
        try:
            import sqlite3
            with sqlite3.connect(selected_db) as conn:
                total, min_ts, max_ts = conn.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM timeseries").fetchone()
                sites = [r[0] for r in conn.execute("SELECT DISTINCT site FROM timeseries ORDER BY site").fetchall()]
            st.write(f"**DB**: {os.path.basename(selected_db)}  |  **rows**: {total}")
            st.write(f"**ts range**: {min_ts}  →  {max_ts}")
            st.write("**sites**:", ", ".join(sites) if sites else "(none)")
        except Exception as e:
            st.error(f"Diagnostics error: {e}")
    else:
        st.info("No valid DB selected.")
