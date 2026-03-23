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

from ttc_pulse.dashboard.charts import line_chart, stacked_bar_chart
from ttc_pulse.dashboard.formatting import fmt_date, fmt_int, fmt_pct, status_label
from ttc_pulse.dashboard.loaders import GOLD_TABLE_FILES, get_gold_table_status_frame, query_table, resolve_duckdb_path
from ttc_pulse.dashboard.storytelling import is_presentation_mode, page_story_header, story_mode_selector


@st.cache_data(ttl=120)
def _load_linkage_quality():
    return query_table(
        table_name="gold_linkage_quality",
        query_template="""
        SELECT
            mode,
            period_start,
            period_end,
            match_method,
            link_status,
            confidence_tier,
            row_count,
            pct_of_mode_rows
        FROM {source}
        """,
    )


@st.cache_data(ttl=120)
def _load_table_status() -> tuple[Path, pd.DataFrame]:
    db_path = resolve_duckdb_path()
    frame = get_gold_table_status_frame(table_names=GOLD_TABLE_FILES.keys(), db_path=db_path)
    if frame.empty:
        frame = pd.DataFrame(columns=["table_name", "status", "source", "row_count", "message"])
    return db_path, frame


st.title("QA / Methodology")
st.caption("Trust the analysis without interrupting the main story flow.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="How reliable are joins, marts, and caveat handling?",
    takeaway="Core data-quality diagnostics are transparent and available, while the stakeholder story remains uncluttered.",
)

linkage_result = _load_linkage_quality()
if linkage_result.status in {"missing", "error"}:
    st.error(linkage_result.message)
    st.stop()
if linkage_result.status == "empty":
    st.info("`gold_linkage_quality` is available but empty.")
    st.stop()

frame = linkage_result.frame.copy()
frame["period_start"] = pd.to_datetime(frame["period_start"], errors="coerce")
frame["period_end"] = pd.to_datetime(frame["period_end"], errors="coerce")
frame["row_count"] = pd.to_numeric(frame["row_count"], errors="coerce").fillna(0)
frame = frame.dropna(subset=["period_start"]).copy()
if frame.empty:
    st.info("No valid linkage QA timestamps are available.")
    st.stop()

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_modes = st.multiselect("Mode", options=mode_options, default=mode_options)
if selected_modes:
    frame = frame[frame["mode"].isin(selected_modes)]
if frame.empty:
    st.info("No rows match selected mode filter.")
    st.stop()

min_period = frame["period_start"].min().date()
max_period = frame["period_start"].max().date()
start_period, end_period = st.slider(
    "Period Start Range",
    min_value=min_period,
    max_value=max_period,
    value=(min_period, max_period),
    format="YYYY-MM",
)

frame = frame[(frame["period_start"].dt.date >= start_period) & (frame["period_start"].dt.date <= end_period)]
if frame.empty:
    st.info("No rows remain after period filter.")
    st.stop()

total_rows = float(frame["row_count"].sum())
matched_rows = float(frame.loc[frame["link_status"] == "matched", "row_count"].sum())
review_rows = float(frame.loc[frame["link_status"].isin(["unmatched_review", "ambiguous_review"]), "row_count"].sum())
ambiguous_rows = float(frame.loc[frame["link_status"] == "ambiguous_review", "row_count"].sum())

metric_a, metric_b, metric_c, metric_d = st.columns(4)
metric_a.metric("Rows in Scope", fmt_int(total_rows))
metric_b.metric("Matched Rows", fmt_int(matched_rows))
metric_c.metric("Review Share", fmt_pct(review_rows / total_rows if total_rows else 0.0))
metric_d.metric("Ambiguous Share", fmt_pct(ambiguous_rows / total_rows if total_rows else 0.0))

confidence_dist = (
    frame.groupby(["period_start", "confidence_tier"], as_index=False)["row_count"]
    .sum()
    .sort_values(["period_start", "confidence_tier"])
)
confidence_chart = stacked_bar_chart(
    frame=confidence_dist,
    x="period_start:T",
    y="row_count:Q",
    color="confidence_tier:N",
    title="Confidence Tier Distribution Over Time",
    tooltip=["period_start:T", "confidence_tier:N", "row_count:Q"],
)
if confidence_chart is not None:
    st.altair_chart(confidence_chart, use_container_width=True)

review_trend = frame.groupby("period_start", as_index=False).agg(total_rows=("row_count", "sum"))
frame["review_row_count"] = frame["row_count"].where(frame["link_status"].isin(["unmatched_review", "ambiguous_review"]), 0)
review_rows_df = frame.groupby("period_start", as_index=False).agg(review_rows=("review_row_count", "sum"))
review_trend = review_trend.merge(review_rows_df, on="period_start", how="left")
review_trend["review_share"] = review_trend["review_rows"] / review_trend["total_rows"].replace(0, pd.NA)
review_chart = line_chart(
    frame=review_trend,
    x="period_start:T",
    y="review_share:Q",
    title="Unmatched + Ambiguous Review Share",
    tooltip=["period_start:T", "review_rows:Q", "total_rows:Q", "review_share:Q"],
)
if review_chart is not None:
    st.altair_chart(review_chart, use_container_width=True)

if not presentation:
    latest_period = frame["period_start"].max()
    latest_frame = frame[frame["period_start"] == latest_period].copy().sort_values("row_count", ascending=False)
    latest_frame["period_start"] = latest_frame["period_start"].dt.date.apply(fmt_date)
    latest_frame["period_end"] = latest_frame["period_end"].dt.date.apply(fmt_date)
    st.subheader(f"Latest Period Snapshot ({fmt_date(latest_period.date())})")
    st.dataframe(latest_frame, use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown("### Gold Mart Runtime Status")
db_file, status_frame = _load_table_status()
ready_count = int((status_frame["status"] == "ok").sum()) if "status" in status_frame else 0
empty_count = int((status_frame["status"] == "empty").sum()) if "status" in status_frame else 0
missing_count = int((status_frame["status"] == "missing").sum()) if "status" in status_frame else 0

col_a, col_b, col_c = st.columns(3)
col_a.metric("Gold Tables Ready", fmt_int(ready_count))
col_b.metric("Gold Tables Empty", fmt_int(empty_count))
col_c.metric("Gold Tables Missing", fmt_int(missing_count))

st.write(f"DuckDB path: `{db_file.as_posix()}`")
status_display = status_frame.copy()
if not status_display.empty and "status" in status_display.columns:
    status_display["status"] = status_display["status"].map(status_label)
st.dataframe(status_display, use_container_width=True, hide_index=True)
