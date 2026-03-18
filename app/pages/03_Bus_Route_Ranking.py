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

from ttc_pulse.dashboard.formatting import fmt_date, fmt_float, fmt_int
from ttc_pulse.dashboard.metric_config import (
    METRIC_OPTIONS,
    metric_axis_title,
    metric_chart_title,
    resolve_metric_choice,
)
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_bus_route_rankings():
    return query_table(
        table_name="gold_top_offender_ranking",
        query_template="""
        SELECT
            ranking_date,
            entity_id AS route_id,
            rank_position,
            frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM {source}
        WHERE entity_type = 'route'
            AND mode = 'bus'
        ORDER BY ranking_date DESC, rank_position ASC
        """,
    )


st.title("Bus Route Ranking")
st.caption("Gold table: `gold_top_offender_ranking`")
selected_metric_label = st.selectbox("Metric to analyze", options=METRIC_OPTIONS, index=0, key="bus_route_metric")

result = _load_bus_route_rankings()
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("No bus route rankings are currently available.")
    st.stop()

frame = result.frame.copy()
frame["ranking_date"] = pd.to_datetime(frame["ranking_date"])

ranking_dates = sorted(frame["ranking_date"].dropna().dt.date.unique().tolist(), reverse=True)
selected_date = st.selectbox("Ranking Date", options=ranking_dates, index=0 if ranking_dates else None)
top_n = st.slider("Top N Routes", min_value=5, max_value=100, value=25, step=5)

filtered = frame[frame["ranking_date"].dt.date == selected_date].copy()
if filtered.empty:
    st.info("No route rows available for the selected date.")
    st.stop()

metric_resolution = resolve_metric_choice(filtered, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)

metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

if composite_selected:
    filtered = filtered.sort_values("rank_position").head(top_n)
    chart_title = "Bus Routes by Composite Reliability Risk Score"
else:
    filtered = filtered.sort_values([metric_column, "rank_position"], ascending=[False, True]).head(top_n)
    chart_title = metric_chart_title("Bus Routes", metric_title)

kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric("Ranking Date", fmt_date(selected_date))
kpi_b.metric("Routes Displayed", fmt_int(len(filtered)))
metric_value = filtered[metric_column].max()
kpi_label = "Top Composite Score" if composite_selected else f"Top {metric_title}"
metric_formatter = fmt_int if metric_title == "Frequency" else fmt_float
kpi_c.metric(kpi_label, metric_formatter(metric_value, digits=3) if metric_formatter is fmt_float else metric_formatter(metric_value))

chart_frame = filtered.copy()
tooltip_fields = ["route_id:N", "rank_position:Q", f"{metric_column}:Q", "frequency:Q", "severity_p90:Q", "regularity_p90:Q", "cause_mix_score:Q", "composite_score:Q"]
if composite_selected:
    tooltip_fields = ["route_id:N", "rank_position:Q", "composite_score:Q", "frequency:Q"]

chart = (
    alt.Chart(chart_frame)
    .mark_bar()
    .encode(
        x=alt.X(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        y=alt.Y("route_id:N", sort="-x", title="Route"),
        tooltip=tooltip_fields,
    )
    .properties(title=chart_title, height=360)
)
st.altair_chart(chart, use_container_width=True)

display_frame = filtered[
    [
        "route_id",
        "rank_position",
        "frequency",
        "severity_p90",
        "regularity_p90",
        "cause_mix_score",
        "composite_score",
    ]
].copy()
display_frame = display_frame.sort_values("rank_position")
st.dataframe(display_frame, use_container_width=True, hide_index=True)
