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
from ttc_pulse.dashboard.formatting import fmt_int, fmt_pct
from ttc_pulse.dashboard.loaders import query_table


@st.cache_data(ttl=120)
def _load_alert_status_counts():
    return query_table(
        table_name="gold_alert_validation",
        query_template="""
        SELECT
            match_status,
            COUNT(*)::BIGINT AS alert_count
        FROM {source}
        GROUP BY 1
        ORDER BY alert_count DESC
        """,
    )


@st.cache_data(ttl=120)
def _load_recent_alerts():
    return query_table(
        table_name="gold_alert_validation",
        query_template="""
        SELECT
            snapshot_ts,
            alert_id,
            selector_scope,
            match_status,
            route_id_gtfs,
            stop_id_gtfs,
            selector_valid,
            route_id_valid,
            stop_id_valid,
            header_text,
            description_text
        FROM {source}
        ORDER BY snapshot_ts DESC NULLS LAST, alert_id
        LIMIT 500
        """,
    )


st.title("Live Alert Validation")
st.caption("Gold table: `gold_alert_validation`")
st.caption(
    "`snapshot_ts` records the alert capture timestamp for each validation row; it is a snapshot time, not a forecast."
)

status_result = _load_alert_status_counts()
alerts_result = _load_recent_alerts()

if status_result.status in {"missing", "error"}:
    st.error(status_result.message)
    st.stop()
if status_result.status == "empty":
    st.info(
        "No GTFS-RT validation rows are currently available. This is expected when no live alert records "
        "exist in `fact_gtfsrt_alerts_norm`."
    )
    st.stop()

status_frame = status_result.frame.copy()
alerts_frame = alerts_result.frame.copy() if alerts_result.status == "ok" else pd.DataFrame()

total_alerts = int(status_frame["alert_count"].sum())
selector_valid_rate = pd.NA
invalid_route_count = 0
invalid_stop_count = 0
latest_snapshot = None
if not alerts_frame.empty:
    selector_valid_rate = alerts_frame["selector_valid"].fillna(False).mean()
    invalid_route_count = int((alerts_frame["route_id_valid"] == False).sum())  # noqa: E712
    invalid_stop_count = int((alerts_frame["stop_id_valid"] == False).sum())  # noqa: E712
    alerts_frame["snapshot_ts"] = pd.to_datetime(alerts_frame["snapshot_ts"])
    latest_snapshot = alerts_frame["snapshot_ts"].max()

kpi_a, kpi_b, kpi_c, kpi_d = st.columns(4)
kpi_a.metric("Alerts in View", fmt_int(total_alerts))
kpi_b.metric("Selector Valid Rate", fmt_pct(selector_valid_rate))
kpi_c.metric("Invalid Route Selectors", fmt_int(invalid_route_count))
kpi_d.metric("Invalid Stop Selectors", fmt_int(invalid_stop_count))

if latest_snapshot is not None and not pd.isna(latest_snapshot):
    st.caption(f"Most recent capture timestamp: {latest_snapshot.strftime('%Y-%m-%d %H:%M:%S')}")

chart = horizontal_bar_chart(
    frame=status_frame,
    x="alert_count:Q",
    y="match_status:N",
    title="Alert Validation Status Distribution",
    tooltip=["match_status:N", "alert_count:Q"],
)
if chart is not None:
    st.altair_chart(chart, use_container_width=True)

if alerts_frame.empty:
    st.info("Detailed alert rows are not available.")
else:
    st.dataframe(alerts_frame, use_container_width=True, hide_index=True)
