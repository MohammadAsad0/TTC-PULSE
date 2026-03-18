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
from ttc_pulse.dashboard.formatting import fmt_date, fmt_float, fmt_int
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


@st.cache_data(ttl=120)
def _load_mode_scorecard():
    return query_table(
        table_name="gold_time_reliability",
        query_template="""
        SELECT
            mode,
            AVG(frequency) AS avg_frequency,
            AVG(severity_p90) AS avg_severity_p90,
            AVG(regularity_p90) AS avg_regularity_p90,
            AVG(composite_score) AS avg_composite_score
        FROM {source}
        GROUP BY 1
        ORDER BY mode
        """,
    )


@st.cache_data(ttl=120)
def _load_overview_coverage():
    return query_table(
        table_name="gold_delay_events_core",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE service_date IS NOT NULL
        """,
    )


st.title("Reliability Overview")
st.caption("Gold tables: `gold_delay_events_core`, `gold_time_reliability`")

coverage_result = _load_overview_coverage()
if coverage_result.status == "ok" and not coverage_result.frame.empty:
    coverage_row = coverage_result.frame.iloc[0]
    min_service_date = coverage_row["min_service_date"]
    max_service_date = coverage_row["max_service_date"]
    if pd.notna(min_service_date) and pd.notna(max_service_date):
        st.caption(
            "Historical service-date coverage window: "
            f"{fmt_date(pd.to_datetime(min_service_date).date())} to "
            f"{fmt_date(pd.to_datetime(max_service_date).date())}."
        )
        st.caption("This view summarizes historical performance only; it is not a live or forecasted status panel.")

monthly_result = _load_monthly_reliability()
score_result = _load_mode_scorecard()

if monthly_result.status in {"missing", "error"}:
    st.error(monthly_result.message)
    st.stop()
if monthly_result.status == "empty":
    st.info("gold_delay_events_core is available but empty.")
    st.stop()

monthly = monthly_result.frame.copy()
monthly["month_start"] = pd.to_datetime(monthly["month_start"])

mode_options = sorted(monthly["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)

mode_monthly = monthly[monthly["mode"] == selected_mode].copy()
if mode_monthly.empty:
    st.info("No monthly reliability rows available for the selected mode.")
    st.stop()

if score_result.status == "ok" and not score_result.frame.empty:
    score_frame = score_result.frame.copy()
    score_row = score_frame[score_frame["mode"] == selected_mode]
else:
    score_row = pd.DataFrame()

freq_metric = score_row["avg_frequency"].iloc[0] if not score_row.empty else pd.NA
sev_metric = score_row["avg_severity_p90"].iloc[0] if not score_row.empty else pd.NA
reg_metric = score_row["avg_regularity_p90"].iloc[0] if not score_row.empty else pd.NA
comp_metric = score_row["avg_composite_score"].iloc[0] if not score_row.empty else pd.NA

kpi_a, kpi_b, kpi_c, kpi_d = st.columns(4)
kpi_a.metric("Avg Hourly Event Frequency", fmt_float(freq_metric, digits=1))
kpi_b.metric("Avg Severity P90 (min)", fmt_float(sev_metric, digits=2))
kpi_c.metric("Avg Regularity P90 (min)", fmt_float(reg_metric, digits=2))
kpi_d.metric("Avg Composite Score", fmt_float(comp_metric, digits=3))

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
