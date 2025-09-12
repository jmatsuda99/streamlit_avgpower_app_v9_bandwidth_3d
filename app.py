
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sqlite3

from db import init_db, reset_db, query_timeseries
from data_ingest import ingest_all_sheets

st.set_page_config(page_title="Average Power (kW) Viewer", layout="wide")
st.title("Average Power (30-min) Viewer")

# --- Sidebar: Upload & Ingest ALL ---
st.sidebar.header("Data Ingest")
uploaded = st.sidebar.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

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
    rows = query_timeseries(query_site, str(q_start), str(q_end))
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

# --- 3D View (Experimental) ---
st.markdown("---")
st.subheader("3D View (Experimental)")

enable_3d = st.checkbox("Enable 3D visualization", value=False, help="Plotly interactive 3D view (rotate, zoom with mouse)")

if enable_3d:
    try:
        import plotly.graph_objects as go
        import numpy as np
        import math

        if 'df' not in locals():
            st.warning("No plotted data available yet. Please run a query above to load data.")
        else:
            df3 = df.copy().reset_index().rename(columns={"ts": "timestamp"})
            df3['date'] = pd.to_datetime(df3['timestamp']).dt.date
            df3['time_minutes'] = pd.to_datetime(df3['timestamp']).dt.hour * 60 + pd.to_datetime(df3['timestamp']).dt.minute

            # Sort and build date index mapping (for X axis)
            dates_sorted = sorted(df3['date'].unique())
            date_to_idx = {d:i for i,d in enumerate(dates_sorted)}
            df3['date_idx'] = df3['date'].map(date_to_idx)

            # === UI controls ===
            xlabel = "Date"
            ylabel = "Time of Day (min)"
            z_option = st.selectbox(
                "Z-axis (kW) value",
                ["consumption_kW", "avg_kW_filled"],
                index=0,
                help="Select which metric to display as kW (depth)."
            )
            plot_type = st.radio(
                "Plot type",
                ["Lines by day"],
                index=0,
                horizontal=True
            )

            st.markdown("**Camera (viewpoint) controls**")
            col1, col2, col3 = st.columns(3)
            with col1:
                az_deg = st.slider("Azimuth (°)", 0, 360, 45, step=5,
                                   help="Rotate around the vertical axis.")
            with col2:
                elev = st.slider("Elevation (Y eye)", 0.2, 3.0, 1.2, step=0.1,
                                 help="Height of the camera eye (along Y).")
            with col3:
                radius = st.slider("Radius", 0.5, 4.0, 2.0, step=0.1,
                                   help="Distance from the center.")

            # Compute camera eye from azimuth, radius, elevation
            theta = math.radians(az_deg)
            eye = dict(x=radius*math.cos(theta), y=elev, z=radius*math.sin(theta))
            camera = dict(eye=eye, up=dict(x=0, y=1, z=0))

            # Lines by day: for each date, draw a 3D line along time (Y) with kW as Z and date index as X
            fig = go.Figure()
            for d in dates_sorted:
                dsub = df3[df3['date'] == d].sort_values('time_minutes')
                fig.add_trace(go.Scatter3d(
                    x=dsub['date_idx'],              # X: date index (横)
                    y=dsub['time_minutes'],          # Y: time minutes (高さ)
                    z=dsub[z_option],                # Z: kW (奥行)
                    mode='lines',
                    name=str(d),
                    line=dict(width=3)
                ))
            fig.update_layout(
                scene=dict(
                    xaxis=dict(
                        title=xlabel,
                        tickmode='array',
                        tickvals=list(range(len(dates_sorted))),
                        ticktext=[str(d) for d in dates_sorted]
                    ),
                    yaxis_title=ylabel,
                    zaxis_title=f"{z_option} (kW)",
                ),
                height=780,
                margin=dict(l=0,r=0,b=0,t=30),
                title=f"{query_site} - 3D Lines (X=date, Y=time, Z={z_option} kW)",
                scene_camera=camera
            )
            st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"3D rendering error: {e}")
