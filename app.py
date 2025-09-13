
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sqlite3
from pathlib import Path

# --- Startup self-check for API contract ---
try:
    import db as _dbcheck
    _required = ["query_timeseries", "ensure_db", "get_conn"]
    _missing = [fn for fn in _required if not hasattr(_dbcheck, fn)]
    if _missing:
        st.warning("DB API mismatch: missing " + ", ".join(_missing))
    APP_SELF_CHECK_PASSED = True
except Exception as _e:
    st.warning(f"Startup check error: {_e}")
    APP_SELF_CHECK_PASSED = False


from db import init_db, reset_db, query_timeseries
from data_ingest import ingest_all_sheets

st.set_page_config(page_title="Average Power (kW) Viewer", layout="wide")
st.title("Average Power (30-min) Viewer")

# --- Sidebar: Upload & Ingest ALL ---
st.sidebar.header("Data Ingest")
uploaded = st.sidebar.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])
# === Multi-DB: ingest 4 target sheets into separate DBs and select DB ===
st.sidebar.markdown("---")
st.sidebar.subheader("Datasets (per-sheet DB)")

if uploaded:
    if st.sidebar.button("Ingest to 4 DBs (per sheet)"):
        try:
            tmp_xlsx = Path("uploaded.xlsx")
            with open(tmp_xlsx, "wb") as f:
                f.write(uploaded.getbuffer())
            from data_ingest import ingest_excel_to_separate_dbs
            created = ingest_excel_to_separate_dbs(str(tmp_xlsx), ".")
            st.sidebar.success(f"Ingested into {len(created)} DBs.")
            if created:
                st.sidebar.write("Created:")
                for p in created:
                    st.sidebar.code(Path(p).name)
        except Exception as e:
            st.sidebar.error(f"Ingest error: {e}")

import glob
db_files = sorted(glob.glob("timeseries_*.db"))
selected_db = st.sidebar.selectbox("Select Dataset (DB)", options=(db_files if db_files else ["(no DBs found)"]))
selected_db = str(selected_db) if selected_db else ""
valid_db_selected = (selected_db.endswith('.db') and selected_db in db_files)

SITE_CHOICES = [
    "武芸川地区シミュレーション (一次)",
    "極楽寺シミュレーション (一次)",
    "笠神地区シミュレーション (一次)",
    "土岐地区地区シミュレーション (一次)",
]
query_site = st.sidebar.selectbox("Site (Sheet)", options=SITE_CHOICES)


# DB controls
colA, colB = st.sidebar.columns(2)
with colA:
    if st.button("Init DB"):
        init_db()
        st.success("DB initialized (tables ensured).")
with colB:
    if st.button("Reset DB (Drop & Recreate)"):
        reset_db()
        st.warning("DB reset. Please ingest again.")

if uploaded:
    tmp_path = f"/tmp/{uploaded.name}"
    with open(tmp_path, "wb") as f:
        f.write(uploaded.getbuffer())

    if st.sidebar.button("Ingest ALL Sheets into DB"):
        init_db()
        n = ingest_all_sheets(tmp_path)
        st.sidebar.success(f"Ingested/updated {n} rows from ALL sheets.")

# --- Plot options ---
st.header("Plot")
query_site = st.text_input("Site", value="武芸川")
col1, col2 = st.columns(2)
with col1:
    q_start = st.date_input("Start Date", value=datetime(2024,7,1))
with col2:
    q_end = st.date_input("End Date (exclusive)", value=datetime(2024,7,2))

show_consumption = st.checkbox("Show Consumption (kW)", value=True)
show_avg = st.checkbox("Show Average (kW, filled)", value=True)
show_accept_bg = st.checkbox('Highlight "Accepted" (最終入札可否=〇) blocks', value=True)
show_avg_band = st.checkbox("Show band between Average ±X kW (inside accepted blocks)", value=True)

# Band width (kW), step = 100 kW, default = 1000 kW
band_width = st.number_input(
    "± Band width (kW)",
    min_value=100,
    max_value=10000,
    step=100,
    value=1000
)

# --- Query & Plot ---
try:
    if not valid_db_selected:
        st.warning('No valid DB selected. Please ingest the Excel and choose a DB from the sidebar.')
        rows = []
    else:
        rows = query_timeseries(query_site, str(q_start), str(q_end), db_path=selected_db)
except sqlite3.OperationalError as e:
    st.error("Database schema error. Click **Init DB** or **Reset DB**, then ingest the Excel again.")
    st.code(str(e))
    rows = []

df = pd.DataFrame(rows, columns=["ts", "consumption_kWh", "generation_kWh", "surplus_kWh", "price", "avg_consumption_kWh", "final_bid_ok"])

if df.empty:
    st.info("No data found in DB for the selected site/range. Ingest the Excel first or adjust the filters.")
else:
    df["ts"] = pd.to_datetime(df["ts"]).sort_values()

    # kWh(30-min) -> kW
    df["consumption_kW"] = df["consumption_kWh"] * 2.0
    df["avg_kW_raw"] = df["avg_consumption_kWh"] * 2.0

    # Forward-fill the 3-hourly average across 30-min slots
    df = df.set_index("ts").asfreq("30min")
    df["avg_kW_filled"] = df["avg_kW_raw"].ffill()

    # Plot (English title)
    fig = plt.figure(figsize=(12,6))
    ax = plt.gca()

    # Background for accepted blocks and ±band width band
    if show_accept_bg:
        accept_mask = df["final_bid_ok"].astype(str) == "〇"
        for t in df.index[accept_mask]:
            ax.axvspan(t, t + pd.Timedelta(minutes=30), alpha=0.25, color="yellow")
            if show_avg_band and not pd.isna(df.loc[t, "avg_kW_filled"]):
                m = df.loc[t, "avg_kW_filled"]
                ax.fill_between(
                    [t, t + pd.Timedelta(minutes=30)],
                    [m - band_width, m - band_width],
                    [m + band_width, m + band_width],
                    alpha=0.25,
                    color="lightblue"
                )

    if show_consumption:
        ax.plot(df.index, df["consumption_kW"], marker="o", linestyle="-", label="Consumption (kW)")
    if show_avg:
        ax.plot(df.index, df["avg_kW_filled"], marker="s", linestyle="--", label="Average Power (kW, filled)")

    ax.set_title(f"{query_site} - Power Consumption (kW)")
    ax.set_xlabel("Time")
    ax.set_ylabel("Power (kW)")
    ax.legend()
    st.pyplot(fig)

    st.download_button(
        "Download Plotted Data (CSV)",
        data=df[["consumption_kW", "avg_kW_filled", "final_bid_ok"]].to_csv(index=True).encode("utf-8"),
        file_name=f"{query_site}_{q_start}_{q_end}_plot_data.csv",
        mime="text/csv"
    )

st.markdown("---")
st.caption("Yellow accepted background + lightblue ±band band (user-set). English titles; robust DB; ALL-sheets ingest; 30-min kWh→kW; 3h average forward-filled.")