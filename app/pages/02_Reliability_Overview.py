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

from ttc_pulse.dashboard.charts import line_chart
from ttc_pulse.dashboard.formatting import fmt_float, fmt_int
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_monthly_reliability():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            mode,
            CAST(DATE_TRUNC('month', service_date) AS DATE) AS month_start,
            SUM(event_count)::BIGINT AS event_count,
            AVG(min_delay_p90) AS min_delay_p90_avg,
            AVG(min_gap_p90) AS min_gap_p90_avg
        FROM {source}
        WHERE service_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY month_start, mode
        """,
    )


st.title("Reliability Overview")

monthly_result = _load_monthly_reliability()

if monthly_result.status in {"missing", "error"}:
    st.error(monthly_result.message)
    st.stop()
if monthly_result.status == "empty":
    st.info("gold_delay_events_core is available but empty.")
    st.stop()

monthly = monthly_result.frame.copy()
monthly["month_start"] = pd.to_datetime(monthly["month_start"])
monthly["year"] = monthly["month_start"].dt.year.astype(int)

mode_options = sorted(monthly["mode"].dropna().unique().tolist())
year_options = sorted(monthly["year"].dropna().unique().tolist())

ctrl_a, ctrl_b = st.columns(2)
selected_mode = ctrl_a.selectbox("Mode", options=mode_options, index=0 if mode_options else None)
selected_year_range = ctrl_b.slider(
    "Year Range",
    min_value=int(min(year_options)) if year_options else 2014,
    max_value=int(max(year_options)) if year_options else 2026,
    value=(
        int(min(year_options)) if year_options else 2014,
        int(max(year_options)) if year_options else 2026,
    ),
    step=1,
)

mode_monthly = monthly[
    (monthly["mode"] == selected_mode)
    & (monthly["year"] >= int(selected_year_range[0]))
    & (monthly["year"] <= int(selected_year_range[1]))
].copy()
if mode_monthly.empty:
    st.info("No monthly reliability rows available for the selected mode.")
    st.stop()

freq_metric = mode_monthly["event_count"].mean() if not mode_monthly.empty else pd.NA
sev_metric = mode_monthly["min_delay_p90_avg"].mean() if not mode_monthly.empty else pd.NA
reg_metric = mode_monthly["min_gap_p90_avg"].mean() if not mode_monthly.empty else pd.NA
months_metric = mode_monthly["month_start"].nunique() if not mode_monthly.empty else 0

kpi_a, kpi_b, kpi_c, kpi_d = st.columns(4)
kpi_a.metric("Avg Monthly Event Count", fmt_float(freq_metric, digits=1))
kpi_b.metric("Avg Min Delay P90 (min)", fmt_float(sev_metric, digits=2))
kpi_c.metric("Avg Min Gap P90 (min)", fmt_float(reg_metric, digits=2))
kpi_d.metric("Months in Scope", fmt_int(months_metric))

events_chart = line_chart(
    frame=mode_monthly,
    x="month_start:T",
    y="event_count:Q",
    title=f"{selected_mode.title()} Monthly Event Volume",
    tooltip=["month_start:T", "event_count:Q"],
)
if events_chart is not None:
    st.altair_chart(events_chart, use_container_width=True)

trend_frame = mode_monthly.melt(
    id_vars=["month_start"],
    value_vars=["min_delay_p90_avg", "min_gap_p90_avg"],
    var_name="metric",
    value_name="value",
)
trend_chart = line_chart(
    frame=trend_frame,
    x="month_start:T",
    y="value:Q",
    color="metric:N",
    title=f"{selected_mode.title()} Monthly Severity and Regularity Trend",
    tooltip=["month_start:T", "metric:N", "value:Q"],
)
if trend_chart is not None:
    st.altair_chart(trend_chart, use_container_width=True)

table_frame = mode_monthly.sort_values("month_start", ascending=False).copy()
table_frame["month_start"] = table_frame["month_start"].dt.date.astype(str)
table_frame["event_count"] = table_frame["event_count"].map(fmt_int)
st.dataframe(table_frame, use_container_width=True, hide_index=True)
