from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd
import streamlit as st


def _bootstrap_src_path() -> None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        src_dir = parent / "src"
        if (src_dir / "ttc_pulse").exists():
            if str(src_dir) not in sys.path:
                sys.path.insert(0, str(src_dir))
            return


_bootstrap_src_path()

from ttc_pulse.dashboard.charts import horizontal_bar_chart, stacked_bar_chart
from ttc_pulse.dashboard.formatting import fmt_pct
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_category_coverage():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """,
    )


@st.cache_data(ttl=120)
def _load_category_totals(start_date: str, end_date: str):
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            incident_category,
            SUM(event_count)::DOUBLE AS event_count
        FROM {source}
        WHERE service_date IS NOT NULL
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_category_monthly(start_date: str, end_date: str):
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
            incident_category,
            SUM(event_count)::DOUBLE AS event_count
        FROM {source}
        WHERE service_date IS NOT NULL
            AND service_date BETWEEN ? AND ?
        GROUP BY 1, 2, 3
        ORDER BY month_start, mode, incident_category
        """,
        params=[start_date, end_date],
    )


def _normalize_date_range(selection: object, min_date: date, max_date: date) -> tuple[date, date]:
    if isinstance(selection, tuple) and len(selection) == 2:
        start_date, end_date = selection
    else:
        start_date = end_date = selection if isinstance(selection, date) else min_date

    if start_date is None:
        start_date = min_date
    if end_date is None:
        end_date = max_date
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


st.title("Cause Category Mix")
st.caption("Source: `gold_delay_events_core`")

coverage_result = _load_category_coverage()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty":
    st.info("gold_delay_events_core is available but has no `service_date` rows.")
    st.stop()

coverage = coverage_result.frame.copy()
coverage["min_service_date"] = pd.to_datetime(coverage["min_service_date"], errors="coerce")
coverage["max_service_date"] = pd.to_datetime(coverage["max_service_date"], errors="coerce")
coverage = coverage.dropna(subset=["min_service_date", "max_service_date"])
if coverage.empty:
    st.info("No valid service-date coverage found in `gold_delay_events_core`.")
    st.stop()

mode_options = sorted(coverage["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)

selected_coverage = coverage[coverage["mode"] == selected_mode]
if selected_coverage.empty:
    st.info("No coverage rows found for selected mode.")
    st.stop()

min_service_date = selected_coverage["min_service_date"].iloc[0].date()
max_service_date = selected_coverage["max_service_date"].iloc[0].date()

date_selection = st.date_input(
    "Service date range",
    value=(min_service_date, max_service_date),
    min_value=min_service_date,
    max_value=max_service_date,
    key="cause_mix_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(
    date_selection,
    min_date=min_service_date,
    max_date=max_service_date,
)

st.caption(
    "Data coverage "
    f"(`service_date`, {selected_mode}): {min_service_date:%Y-%m} to {max_service_date:%Y-%m} "
    f"({min_service_date:%Y-%m-%d} to {max_service_date:%Y-%m-%d}) | "
    f"Selected: {selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d}"
)

top_n = st.slider("Top N Categories", min_value=3, max_value=20, value=8, step=1)

totals_result = _load_category_totals(selected_start_date.isoformat(), selected_end_date.isoformat())
if totals_result.status in {"missing", "error"}:
    st.error(totals_result.message)
    st.stop()
if totals_result.status == "empty":
    st.info("No incident-category rows found for the selected date range.")
    st.stop()

totals = totals_result.frame.copy()
totals = totals[(totals["mode"] == selected_mode) & totals["incident_category"].notna()].copy()
if totals.empty:
    st.info("No incident categories are available for the selected filters.")
    st.stop()

mode_totals_all = totals.sort_values("event_count", ascending=False)
mode_total_count = float(mode_totals_all["event_count"].sum())
mode_totals = mode_totals_all.head(top_n).copy()
mode_totals["share"] = mode_totals["event_count"] / mode_total_count if mode_total_count else 0.0

bar_chart = horizontal_bar_chart(
    frame=mode_totals,
    x="event_count:Q",
    y="incident_category:N",
    title=(
        f"{selected_mode.title()} Incident Category Mix "
        f"({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d}, Top {top_n})"
    ),
    tooltip=["incident_category:N", "event_count:Q", "share:Q"],
)
if bar_chart is not None:
    st.altair_chart(bar_chart, use_container_width=True)

table_totals = mode_totals.copy()
table_totals["share"] = table_totals["share"].map(fmt_pct)
table_totals = table_totals.rename(
    columns={
        "incident_category": "Incident Category",
        "event_count": "Event Count",
        "share": "Share of Selected Window",
    }
)
st.dataframe(table_totals, use_container_width=True, hide_index=True)

monthly_result = _load_category_monthly(selected_start_date.isoformat(), selected_end_date.isoformat())
if monthly_result.status != "ok" or monthly_result.frame.empty:
    st.info("Monthly category trend is unavailable for the selected date range.")
    st.stop()

monthly = monthly_result.frame.copy()
monthly["month_start"] = pd.to_datetime(monthly["month_start"], errors="coerce")
monthly = monthly[(monthly["mode"] == selected_mode) & monthly["month_start"].notna()]
monthly = monthly[monthly["incident_category"].isin(mode_totals["incident_category"])]
if monthly.empty:
    st.info("No monthly category rows for the selected filters.")
    st.stop()

trend_chart = stacked_bar_chart(
    frame=monthly,
    x="month_start:T",
    y="event_count:Q",
    color="incident_category:N",
    title=f"Monthly Category Trend ({selected_start_date:%Y-%m-%d} to {selected_end_date:%Y-%m-%d})",
    tooltip=["month_start:T", "incident_category:N", "event_count:Q"],
)
if trend_chart is not None:
    st.altair_chart(trend_chart, use_container_width=True)
