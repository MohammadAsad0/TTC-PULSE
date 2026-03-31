from __future__ import annotations

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

from ttc_pulse.dashboard.charts import horizontal_bar_chart
from ttc_pulse.dashboard.formatting import fmt_date, fmt_float, fmt_int
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
filtered = filtered.sort_values("rank_position").head(top_n)
if filtered.empty:
    st.info("No route rows available for the selected date.")
    st.stop()

kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric("Ranking Date", fmt_date(selected_date))
kpi_b.metric("Routes Displayed", fmt_int(len(filtered)))
kpi_c.metric("Top Composite Score", fmt_float(filtered["composite_score"].max(), digits=3))

chart_frame = filtered.sort_values("composite_score", ascending=False).copy()
chart = horizontal_bar_chart(
    frame=chart_frame,
    x="composite_score:Q",
    y="route_id:N",
    title="Bus Routes by Composite Reliability Risk Score",
    tooltip=["route_id:N", "rank_position:Q", "composite_score:Q", "frequency:Q"],
)
if chart is not None:
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
