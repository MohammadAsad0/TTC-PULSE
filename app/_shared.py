from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


def bootstrap_src() -> None:
    src_dir = Path(__file__).resolve().parents[1] / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


bootstrap_src()

from ttc_pulse.service import build_overview, load_fast_datasets, refresh_fast_artifacts


@st.cache_data(show_spinner=False)
def get_data() -> dict[str, object]:
    return load_fast_datasets()


@st.cache_data(show_spinner=False)
def get_overview(bus_rows: pd.DataFrame, subway_rows: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return build_overview(bus_rows, subway_rows)


def render_cache_controls() -> None:
    col_action, col_text = st.columns([1, 4])
    with col_action:
        do_refresh = st.button("Refresh Data Cache")
    with col_text:
        st.caption("Using Parquet/DuckDB cache for faster filtering. Refresh only after raw CSV changes.")
    if do_refresh:
        meta = refresh_fast_artifacts()
        get_data.clear()
        st.success(
            f"Cache refreshed. Bus rows: {int(meta.get('rows_bus', 0)):,}; "
            f"Subway rows: {int(meta.get('rows_subway', 0)):,}."
        )


def apply_theme() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ttc-navy: #0e2c4b;
            --ttc-ink: #22384f;
            --ttc-muted: #6d7a89;
            --ttc-mint: #eaf8ef;
            --ttc-mint-border: #caead4;
        }
        .block-container {
            padding-top: 1.75rem;
            padding-bottom: 2rem;
        }
        h1, h2, h3 {
            color: var(--ttc-navy);
            letter-spacing: -0.02em;
        }
        .takeaway {
            background: var(--ttc-mint);
            border: 1px solid var(--ttc-mint-border);
            border-radius: 0.75rem;
            padding: 1rem 1.25rem;
            color: #256c45;
            margin: 1rem 0 1.5rem 0;
            font-size: 1.05rem;
        }
        .metric-label {
            color: var(--ttc-muted);
            font-size: 0.95rem;
            margin-bottom: 0.25rem;
        }
        .metric-value {
            color: var(--ttc-navy);
            font-size: 2.2rem;
            line-height: 1.05;
            margin-bottom: 0.75rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_dataset_explorer(bus_rows: pd.DataFrame, subway_rows: pd.DataFrame) -> None:
    apply_theme()
    st.title("Dataset Explorer")
    st.write("Inspect the row-level TTC delay dataset by mode and service-date window.")
    st.markdown("**Audience Question:** What raw-like event rows are behind the dashboard?")
    st.markdown(
        '<div class="takeaway"><strong>Takeaway:</strong> This page exposes the row-level bus and subway event dataset with source lineage so date-window checks are transparent.</div>',
        unsafe_allow_html=True,
    )

    dataset_label = st.radio("Dataset", options=["Bus", "Subway"], horizontal=True)
    frame = bus_rows if dataset_label == "Bus" else subway_rows
    if "route_id_gtfs" in frame.columns:
        gtfs_route = frame["route_id_gtfs"].astype("string").str.strip()
        frame = frame.loc[gtfs_route.notna() & gtfs_route.ne("")].copy()
    frame = frame.sort_values(["service_date", "source_file"], ascending=[True, True]).reset_index(drop=True)

    if frame.empty:
        st.warning(f"No cleaned {dataset_label.lower()} rows are available.")
        return

    min_date = frame["service_date"].min().date()
    max_date = frame["service_date"].max().date()

    filters_left, filters_right = st.columns([2.3, 1.2], gap="large")
    with filters_left:
        service_window = st.date_input(
            "Service Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    with filters_right:
        row_limit = st.selectbox("Rows to Show", options=[100, 250, 500, 1000, 2500], index=2)

    if isinstance(service_window, tuple) and len(service_window) == 2:
        start_date, end_date = service_window
    else:
        start_date, end_date = min_date, max_date

    filtered = frame.loc[frame["service_date"].between(pd.Timestamp(start_date), pd.Timestamp(end_date))].copy()
    displayed = filtered.head(row_limit)

    metric_cols = st.columns(3)
    metrics = [
        ("Rows Displayed", f"{len(displayed):,}"),
        ("Window Start", str(start_date)),
        ("Window End", str(end_date)),
    ]
    for col, (label, value) in zip(metric_cols, metrics):
        with col:
            st.markdown(f'<div class="metric-label">{label}</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="metric-value">{value}</div>', unsafe_allow_html=True)

    st.caption(f"Filtered rows in window: {len(filtered):,}. Showing first {len(displayed):,} rows.")
    st.dataframe(displayed, width="stretch", hide_index=True)


def render_story_overview(bus_rows: pd.DataFrame, subway_rows: pd.DataFrame) -> None:
    apply_theme()
    st.title("Story Overview")
    st.write("Simple operational summaries across cleaned bus and subway delay records.")
    summaries = get_overview(bus_rows, subway_rows)

    top = st.columns(4)
    cards = [
        ("Bus Rows", f"{len(bus_rows):,}"),
        ("Subway Rows", f"{len(subway_rows):,}"),
        ("Bus Date Coverage", summaries["coverage"].loc[0, "coverage"]),
        ("Subway Date Coverage", summaries["coverage"].loc[1, "coverage"]),
    ]
    for col, (label, value) in zip(top, cards):
        with col:
            st.markdown(f"<div class='metric-label'>{label}</div>", unsafe_allow_html=True)
            st.markdown(f"<div class='metric-value'>{value}</div>", unsafe_allow_html=True)

    left, right = st.columns(2, gap="large")
    with left:
        st.subheader("Date Coverage")
        st.dataframe(summaries["coverage"], width="stretch", hide_index=True)
        st.subheader("Top Bus Routes")
        st.dataframe(summaries["bus_routes"], width="stretch", hide_index=True)
    with right:
        st.subheader("Top Subway Stations")
        st.dataframe(summaries["subway_stations"], width="stretch", hide_index=True)
        st.subheader("Top Subway Lines")
        st.dataframe(summaries["subway_lines"], width="stretch", hide_index=True)


def render_placeholder_page(title: str, description: str) -> None:
    apply_theme()
    st.title(title)
    st.info(description)
    st.caption("This page is scaffolded to match your requested navigation structure.")
