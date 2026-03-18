from __future__ import annotations

from pathlib import Path
import sys

import altair as alt
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
from ttc_pulse.dashboard.formatting import fmt_pct, sort_day_name
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_category_totals():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            incident_category,
            SUM(event_count)::DOUBLE AS event_count
        FROM {source}
        GROUP BY 1, 2
        """,
    )


@st.cache_data(ttl=120)
def _load_category_monthly():
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
        GROUP BY 1, 2, 3
        ORDER BY month_start, mode, incident_category
        """,
    )


@st.cache_data(ttl=120)
def _load_category_weekday():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            incident_category,
            STRFTIME(service_date, '%A') AS day_name,
            SUM(event_count)::DOUBLE AS event_count
        FROM {source}
        WHERE service_date IS NOT NULL
            AND incident_category IS NOT NULL
        GROUP BY 1, 2, 3
        """,
    )


@st.cache_data(ttl=120)
def _load_category_hour():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            incident_category,
            CAST(hour_bin AS INTEGER) AS hour_bin,
            SUM(event_count)::DOUBLE AS event_count
        FROM {source}
        WHERE hour_bin IS NOT NULL
            AND incident_category IS NOT NULL
        GROUP BY 1, 2, 3
        """,
    )


st.title("Cause Category Mix")

totals_result = _load_category_totals()
monthly_result = _load_category_monthly()
weekday_result = _load_category_weekday()
hour_result = _load_category_hour()

if totals_result.status in {"missing", "error"}:
    st.error(totals_result.message)
    st.stop()
if totals_result.status == "empty":
    st.info("gold_delay_events_core is available but empty.")
    st.stop()

totals = totals_result.frame.copy()
totals = totals[totals["incident_category"].notna()].copy()
if totals.empty:
    st.info("No incident categories are available.")
    st.stop()

mode_options = sorted(totals["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)
top_n = st.slider("Top N Categories", min_value=3, max_value=20, value=8, step=1)

mode_totals = totals[totals["mode"] == selected_mode].copy()
mode_totals = mode_totals.sort_values("event_count", ascending=False)
mode_totals = mode_totals.head(top_n)
category_order = mode_totals["incident_category"].astype(str).tolist()
mode_total_count = float(mode_totals["event_count"].sum())
mode_totals["share"] = mode_totals["event_count"] / mode_total_count if mode_total_count else 0.0

bar_chart = horizontal_bar_chart(
    frame=mode_totals,
    x="event_count:Q",
    y="incident_category:N",
    title=f"{selected_mode.title()} Incident Category Mix (Top {top_n})",
    tooltip=["incident_category:N", "event_count:Q", "share:Q"],
)
if bar_chart is not None:
    st.altair_chart(bar_chart, use_container_width=True)

mode_totals["share"] = mode_totals["share"].map(fmt_pct)
st.dataframe(mode_totals, use_container_width=True, hide_index=True)

if monthly_result.status == "ok" and not monthly_result.frame.empty:
    monthly = monthly_result.frame.copy()
    monthly["month_start"] = pd.to_datetime(monthly["month_start"])
    monthly = monthly[monthly["mode"] == selected_mode]
    monthly = monthly[monthly["incident_category"].isin(mode_totals["incident_category"])]
    if not monthly.empty:
        trend_chart = stacked_bar_chart(
            frame=monthly,
            x="month_start:T",
            y="event_count:Q",
            color="incident_category:N",
            title="Monthly Category Trend",
            tooltip=["month_start:T", "incident_category:N", "event_count:Q"],
        )
        if trend_chart is not None:
            st.altair_chart(trend_chart, use_container_width=True)
    else:
        st.info("No monthly category rows for the selected filters.")
else:
    st.info("Monthly category trend is unavailable.")

if weekday_result.status == "ok" and not weekday_result.frame.empty:
    weekday = weekday_result.frame.copy()
    weekday = weekday[weekday["mode"] == selected_mode]
    weekday = weekday[weekday["incident_category"].isin(mode_totals["incident_category"])]
    if not weekday.empty:
        weekday = sort_day_name(weekday, column="day_name")
        st.markdown("### Cause Distribution by Weekday")
        weekday_heatmap = (
            alt.Chart(weekday)
            .mark_rect()
            .encode(
                x=alt.X("day_name:N", title="Weekday"),
                y=alt.Y("incident_category:N", title="Cause Category", sort=category_order),
                color=alt.Color("event_count:Q", title="Event Count", scale=alt.Scale(scheme="blues")),
                tooltip=["incident_category:N", "day_name:N", "event_count:Q"],
            )
            .properties(height=380)
        )
        st.altair_chart(weekday_heatmap, use_container_width=True)
    else:
        st.info("No weekday cause rows are available for the selected mode/category filter.")
else:
    st.info("Weekday cause distribution is unavailable.")

if hour_result.status == "ok" and not hour_result.frame.empty:
    hour = hour_result.frame.copy()
    hour = hour[hour["mode"] == selected_mode]
    hour = hour[hour["incident_category"].isin(mode_totals["incident_category"])]
    if not hour.empty:
        hour["hour_bin"] = pd.to_numeric(hour["hour_bin"], errors="coerce").fillna(0).astype(int)
        st.markdown("### Cause Distribution by Hour of Day")
        hour_heatmap = (
            alt.Chart(hour)
            .mark_rect()
            .encode(
                x=alt.X("hour_bin:O", title="Hour of Day", sort=list(range(24))),
                y=alt.Y("incident_category:N", title="Cause Category", sort=category_order),
                color=alt.Color("event_count:Q", title="Event Count", scale=alt.Scale(scheme="greens")),
                tooltip=["incident_category:N", "hour_bin:O", "event_count:Q"],
            )
            .properties(height=380)
        )
        st.altair_chart(hour_heatmap, use_container_width=True)
    else:
        st.info("No hour-of-day cause rows are available for the selected mode/category filter.")
else:
    st.info("Hour-of-day cause distribution is unavailable.")
