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

from ttc_pulse.dashboard.formatting import sort_day_name
from ttc_pulse.dashboard.loaders import query_table

RAW_METRIC_OPTIONS: list[str] = ["Frequency", "Min Delay P90", "Min Gap P90"]
RAW_METRIC_COLUMN_MAP: dict[str, str] = {
    "Frequency": "frequency",
    "Min Delay P90": "min_delay_p90",
    "Min Gap P90": "min_gap_p90",
}


@st.cache_data(ttl=120)
def _load_time_heatmap_frame():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            STRFTIME(service_date, '%A') AS day_name,
            CAST(hour_bin AS INTEGER) AS hour_bin,
            SUM(event_count)::DOUBLE AS frequency,
            SUM(CASE WHEN min_delay_p90 IS NOT NULL THEN min_delay_p90 * event_count ELSE 0 END)
                / NULLIF(SUM(CASE WHEN min_delay_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS min_delay_p90,
            SUM(CASE WHEN min_gap_p90 IS NOT NULL THEN min_gap_p90 * event_count ELSE 0 END)
                / NULLIF(SUM(CASE WHEN min_gap_p90 IS NOT NULL THEN event_count ELSE 0 END), 0) AS min_gap_p90
        FROM {source}
        WHERE service_date IS NOT NULL
            AND hour_bin IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY mode, day_name, hour_bin
        """,
    )


st.title("Weekday Hour Heatmap")
selected_metric_label = st.selectbox(
    "Metric to analyze",
    options=RAW_METRIC_OPTIONS,
    index=0,
    key="heatmap_metric_raw",
)

result = _load_time_heatmap_frame()
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No weekday-hour rows are available in the current Gold mart.")
    st.stop()

frame = result.frame.copy()
frame = sort_day_name(frame, column="day_name")
frame["hour_bin"] = pd.to_numeric(frame["hour_bin"], errors="coerce").fillna(0).astype(int)

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)

filtered = frame[frame["mode"] == selected_mode].copy()
if filtered.empty:
    st.info("No rows available for the selected mode.")
    st.stop()

metric_column = RAW_METRIC_COLUMN_MAP[selected_metric_label]
chart_title = f"{selected_mode.title()} {selected_metric_label} by Weekday and Hour"

chart = (
    alt.Chart(filtered)
    .mark_rect()
    .encode(
        x=alt.X("hour_bin:O", title="Hour of Day", sort=list(range(24))),
        y=alt.Y("day_name:N", title="Day of Week"),
        color=alt.Color(
            f"{metric_column}:Q",
            title=selected_metric_label,
            scale=alt.Scale(scheme="tealblues"),
        ),
        tooltip=[
            "day_name:N",
            "hour_bin:O",
            f"{metric_column}:Q",
            "frequency:Q",
            "min_delay_p90:Q",
            "min_gap_p90:Q",
        ],
    )
    .properties(title=chart_title, height=520, width=1100)
)
st.altair_chart(chart, use_container_width=False)

st.dataframe(
    filtered.sort_values(["day_name", "hour_bin"]),
    use_container_width=True,
    hide_index=True,
)
