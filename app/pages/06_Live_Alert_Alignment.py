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

from ttc_pulse.dashboard.formatting import fmt_int, fmt_pct
from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector


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
def _load_alert_scope_validity():
    return query_table(
        table_name="gold_alert_validation",
        query_template="""
        SELECT
            selector_scope,
            CASE WHEN selector_valid THEN 'valid' ELSE 'invalid' END AS selector_validity,
            COUNT(*)::BIGINT AS alert_count
        FROM {source}
        GROUP BY 1, 2
        ORDER BY selector_scope, selector_validity
        """,
    )


@st.cache_data(ttl=120)
def _load_alert_timeline():
    return query_table(
        table_name="gold_alert_validation",
        query_template="""
        SELECT
            CAST(DATE_TRUNC('hour', snapshot_ts) AS TIMESTAMP) AS snapshot_hour,
            COUNT(*)::BIGINT AS alert_count,
            SUM(CASE WHEN selector_valid THEN 1 ELSE 0 END)::BIGINT AS valid_count
        FROM {source}
        WHERE snapshot_ts IS NOT NULL
        GROUP BY 1
        ORDER BY snapshot_hour
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
        LIMIT 250
        """,
    )


@st.cache_data(ttl=120)
def _load_linkage_snapshot():
    return query_table(
        table_name="gold_linkage_quality",
        query_template="""
        SELECT
            mode,
            period_start,
            link_status,
            SUM(row_count)::BIGINT AS row_count
        FROM {source}
        GROUP BY 1, 2, 3
        ORDER BY period_start DESC
        LIMIT 60
        """,
    )


st.title("Live Alert Alignment")
st.caption("Understand and verify whether live disruptions align with historically risky entities.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="Do live GTFS-RT alerts align with historical hotspot patterns?",
    takeaway="Live alerts provide a validation signal for recurring risk, but selector quality and data sparsity must be monitored.",
)

status_result = _load_alert_status_counts()
scope_result = _load_alert_scope_validity()
timeline_result = _load_alert_timeline()
alerts_result = _load_recent_alerts()

if status_result.status in {"missing", "error"}:
    st.error(status_result.message)
    st.stop()
if status_result.status == "empty":
    st.info(
        "No GTFS-RT validation rows are currently available. "
        "This is expected when live alert facts are not yet populated."
    )
    next_question_hint("Need confidence diagnostics and caveats? Open: QA / Methodology.")
    st.stop()

status_frame = status_result.frame.copy()
alerts_frame = alerts_result.frame.copy() if alerts_result.status == "ok" else pd.DataFrame()
scope_frame = scope_result.frame.copy() if scope_result.status == "ok" else pd.DataFrame()
timeline_frame = timeline_result.frame.copy() if timeline_result.status == "ok" else pd.DataFrame()

total_alerts = int(status_frame["alert_count"].sum())
selector_valid_rate = pd.NA
invalid_route_count = 0
invalid_stop_count = 0
unmatched_review_count = 0
latest_snapshot = None
if not alerts_frame.empty:
    selector_valid_rate = alerts_frame["selector_valid"].fillna(False).mean()
    invalid_route_count = int((alerts_frame["route_id_valid"] == False).sum())  # noqa: E712
    invalid_stop_count = int((alerts_frame["stop_id_valid"] == False).sum())  # noqa: E712
    unmatched_review_count = int((alerts_frame["match_status"] == "unmatched_review").sum())
    alerts_frame["snapshot_ts"] = pd.to_datetime(alerts_frame["snapshot_ts"])
    latest_snapshot = alerts_frame["snapshot_ts"].max()

kpi_a, kpi_b, kpi_c, kpi_d, kpi_e = st.columns(5)
kpi_a.metric("Alerts in View", fmt_int(total_alerts))
kpi_b.metric("Selector Valid Rate", fmt_pct(selector_valid_rate))
kpi_c.metric("Invalid Route Selectors", fmt_int(invalid_route_count))
kpi_d.metric("Invalid Stop Selectors", fmt_int(invalid_stop_count))
kpi_e.metric("Unmatched Review", fmt_int(unmatched_review_count))

if latest_snapshot is not None and not pd.isna(latest_snapshot):
    st.info(f"Latest alert capture snapshot: {latest_snapshot.strftime('%Y-%m-%d %H:%M:%S')}")

status_chart = (
    alt.Chart(status_frame)
    .mark_bar()
    .encode(
        x=alt.X("alert_count:Q", title="Alert Count"),
        y=alt.Y("match_status:N", sort="-x", title="Validation Status"),
        color=alt.Color("match_status:N", legend=None),
        tooltip=["match_status:N", "alert_count:Q"],
    )
    .properties(title="Live Alert Match-Status Distribution", height=300)
)
st.altair_chart(status_chart, use_container_width=True)

if not scope_frame.empty and not presentation:
    scope_chart = (
        alt.Chart(scope_frame)
        .mark_bar()
        .encode(
            x=alt.X("selector_scope:N", title="Selector Scope"),
            y=alt.Y("alert_count:Q", title="Alert Count"),
            color=alt.Color("selector_validity:N", title="Selector Validity"),
            tooltip=["selector_scope:N", "selector_validity:N", "alert_count:Q"],
        )
        .properties(title="Selector Scope and Validity", height=280)
    )
    st.altair_chart(scope_chart, use_container_width=True)

if not timeline_frame.empty:
    timeline_frame["snapshot_hour"] = pd.to_datetime(timeline_frame["snapshot_hour"])
    timeline_frame["valid_rate"] = timeline_frame["valid_count"] / timeline_frame["alert_count"].replace(0, pd.NA)

    timeline_chart = (
        alt.Chart(timeline_frame)
        .mark_line(point=True)
        .encode(
            x=alt.X("snapshot_hour:T", title="Snapshot Hour"),
            y=alt.Y("alert_count:Q", title="Alert Count"),
            tooltip=["snapshot_hour:T", "alert_count:Q", "valid_count:Q"],
        )
        .properties(title="Snapshot Capture Timeline", height=280)
    )
    st.altair_chart(timeline_chart, use_container_width=True)

if not presentation and not alerts_frame.empty:
    with st.expander("Recent Alerts (Exploration)"):
        st.dataframe(alerts_frame, use_container_width=True, hide_index=True)

with st.expander("QA Context (linkage snapshot)"):
    linkage_result = _load_linkage_snapshot()
    if linkage_result.status == "ok" and not linkage_result.frame.empty:
        linkage = linkage_result.frame.copy()
        linkage["period_start"] = pd.to_datetime(linkage["period_start"], errors="coerce")
        linkage_chart = (
            alt.Chart(linkage)
            .mark_bar()
            .encode(
                x=alt.X("row_count:Q", title="Rows"),
                y=alt.Y("link_status:N", title="Link Status"),
                color=alt.Color("mode:N", title="Mode"),
                tooltip=["mode:N", "period_start:T", "link_status:N", "row_count:Q"],
            )
            .properties(height=220)
        )
        st.altair_chart(linkage_chart, use_container_width=True)
    else:
        st.caption("Linkage QA snapshot unavailable.")

next_question_hint("Want technical caveats and data-quality diagnostics? Open: QA / Methodology.")
