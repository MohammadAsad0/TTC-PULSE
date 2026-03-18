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
from ttc_pulse.dashboard.formatting import fmt_date, fmt_int, fmt_pct
from ttc_pulse.dashboard.loaders import query_table


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


st.title("Linkage QA")
st.caption("Gold table: `gold_linkage_quality`")

result = _load_linkage_quality()
if result.status in {"missing", "error"}:
    st.error(result.message)
    st.stop()
if result.status == "empty":
    st.info("gold_linkage_quality is available but empty.")
    st.stop()

frame = result.frame.copy()
frame["period_start"] = pd.to_datetime(frame["period_start"])
frame["period_end"] = pd.to_datetime(frame["period_end"])
frame["row_count"] = pd.to_numeric(frame["row_count"], errors="coerce").fillna(0)

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_modes = st.multiselect(
    "Mode",
    options=mode_options,
    default=mode_options,
)

if selected_modes:
    frame = frame[frame["mode"].isin(selected_modes)]

if frame.empty:
    st.info("No rows match the selected mode filter.")
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

frame = frame[
    (frame["period_start"].dt.date >= start_period)
    & (frame["period_start"].dt.date <= end_period)
]
if frame.empty:
    st.info("No rows remain after the period filter.")
    st.stop()

total_rows = float(frame["row_count"].sum())
matched_rows = float(frame.loc[frame["link_status"] == "matched", "row_count"].sum())
review_rows = float(
    frame.loc[
        frame["link_status"].isin(["unmatched_review", "ambiguous_review"]),
        "row_count",
    ].sum()
)
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

review_trend = frame.groupby("period_start", as_index=False).agg(
    total_rows=("row_count", "sum"),
)
frame["review_row_count"] = frame["row_count"].where(
    frame["link_status"].isin(["unmatched_review", "ambiguous_review"]),
    0,
)
review_rows = frame.groupby("period_start", as_index=False).agg(review_rows=("review_row_count", "sum"))
review_trend = review_trend.merge(review_rows, on="period_start", how="left")
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

latest_period = frame["period_start"].max()
latest_frame = frame[frame["period_start"] == latest_period].copy()
latest_frame = latest_frame.sort_values("row_count", ascending=False)
latest_frame["period_start"] = latest_frame["period_start"].dt.date.apply(fmt_date)
latest_frame["period_end"] = latest_frame["period_end"].dt.date.apply(fmt_date)

st.subheader(f"Latest Period Snapshot ({fmt_date(latest_period.date())})")
st.dataframe(
    latest_frame[
        [
            "mode",
            "period_start",
            "period_end",
            "match_method",
            "link_status",
            "confidence_tier",
            "row_count",
            "pct_of_mode_rows",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
