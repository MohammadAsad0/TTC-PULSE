from __future__ import annotations

from pathlib import Path
import sys

import altair as alt
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


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
from ttc_pulse.utils.project_setup import resolve_project_paths


def _project_file(*parts: str) -> Path:
    return resolve_project_paths().project_root.joinpath(*parts)


@st.cache_data(ttl=30)
def _load_alert_archive():
    archive_path = _project_file("alerts", "parsed", "service_alert_entities.csv")
    if not archive_path.exists():
        return pd.DataFrame(
            columns=[
                "snapshot_ts_utc",
                "alert_id",
                "route_id",
                "stop_id",
                "alert_scope",
                "header_text",
                "description_text",
                "cause",
                "effect",
                "active_start_utc",
                "active_end_utc",
            ]
        )

    frame = pd.read_csv(
        archive_path,
        usecols=lambda column: column
        in {
            "snapshot_ts_utc",
            "alert_id",
            "route_id",
            "stop_id",
            "header_text",
            "description_text",
            "cause",
            "effect",
            "active_start_utc",
            "active_end_utc",
        },
    )
    if frame.empty:
        return frame

    frame["snapshot_ts_utc"] = pd.to_datetime(frame["snapshot_ts_utc"], errors="coerce", utc=True)
    for column in ["active_start_utc", "active_end_utc"]:
        if column in frame.columns:
            frame[column] = pd.to_datetime(frame[column], errors="coerce", utc=True)
    for column in ["route_id", "stop_id"]:
        if column in frame.columns:
            frame[column] = frame[column].astype("string").str.replace(r"\.0$", "", regex=True).str.strip()
            frame.loc[frame[column].isin(["", "nan", "None", "<NA>"]), column] = pd.NA
    for column in ["header_text", "description_text", "cause", "effect"]:
        if column in frame.columns:
            frame[column] = frame[column].astype("string").str.strip()
            frame.loc[frame[column].isin(["", "nan", "None", "<NA>"]), column] = pd.NA

    has_route = frame["route_id"].notna() if "route_id" in frame.columns else pd.Series(False, index=frame.index)
    has_stop = frame["stop_id"].notna() if "stop_id" in frame.columns else pd.Series(False, index=frame.index)
    frame["alert_scope"] = "network-wide notice"
    frame.loc[has_route & ~has_stop, "alert_scope"] = "route-tagged alert"
    frame.loc[~has_route & has_stop, "alert_scope"] = "stop-tagged alert"
    frame.loc[has_route & has_stop, "alert_scope"] = "route + stop tagged alert"
    return frame


@st.cache_data(ttl=30)
def _load_scheduler_cycles() -> pd.DataFrame:
    log_path = _project_file("logs", "step3_alerts_sidecar_log.csv")
    if not log_path.exists():
        return pd.DataFrame(columns=["logged_at", "row_count", "status"])

    frame = pd.read_csv(log_path)
    if frame.empty or "step" not in frame.columns:
        return pd.DataFrame(columns=["logged_at", "row_count", "status"])

    frame = frame[frame["step"] == "alerts_sidecar_cycle"].copy()
    if frame.empty:
        return pd.DataFrame(columns=["logged_at", "row_count", "status"])

    frame["logged_at"] = pd.to_datetime(frame["logged_at"], errors="coerce", utc=True)
    frame["row_count"] = pd.to_numeric(frame["row_count"], errors="coerce").fillna(0).astype(int)
    frame = frame.dropna(subset=["logged_at"]).sort_values("logged_at")
    return frame[["logged_at", "row_count", "status", "details"]].copy()


@st.cache_data(ttl=30)
def _load_hotspot_reference(top_k: int = 50):
    result = query_table(
        table_name="gold_top_offender_ranking",
        query_template="""
        SELECT
            mode,
            entity_type,
            entity_id,
            rank_position,
            frequency,
            composite_score
        FROM {source}
        WHERE entity_type = 'route'
            AND ranking_date = (
                SELECT MAX(ranking_date)
                FROM {source}
                WHERE entity_type = 'route'
            )
        ORDER BY composite_score DESC, frequency DESC, entity_id
        LIMIT ?
        """,
        params=[top_k],
    )
    return result.frame.copy() if result.status == "ok" else pd.DataFrame(columns=["mode", "entity_type", "entity_id", "rank_position", "frequency", "composite_score"])


@st.cache_data(ttl=30)
def _load_route_universe_size() -> int:
    result = query_table(
        table_name="gold_top_offender_ranking",
        query_template="""
        SELECT COUNT(DISTINCT entity_id)::BIGINT AS route_count
        FROM {source}
        WHERE entity_type = 'route'
            AND ranking_date = (
                SELECT MAX(ranking_date)
                FROM {source}
                WHERE entity_type = 'route'
            )
        """,
    )
    if result.status != "ok" or result.frame.empty:
        return 0
    return int(result.frame.iloc[0]["route_count"])


def _auto_refresh_every(minutes: int) -> None:
    delay_ms = max(1, minutes) * 60 * 1000
    components.html(
        f"""
        <script>
        setTimeout(function() {{
            window.parent.location.reload();
        }}, {delay_ms});
        </script>
        """,
        height=0,
    )


def _alert_scope_distribution_frame(alerts_frame: pd.DataFrame) -> pd.DataFrame:
    if alerts_frame.empty:
        return pd.DataFrame(columns=["alert_scope", "alert_count"])
    return (
        alerts_frame.groupby("alert_scope", as_index=False)
        .size()
        .rename(columns={"size": "alert_count"})
        .sort_values("alert_count", ascending=False)
    )


def _join_unique(values: pd.Series, limit: int = 4) -> str:
    unique_values = [value for value in values.dropna().astype(str).str.strip().unique().tolist() if value]
    if not unique_values:
        return "None listed"
    visible = unique_values[:limit]
    suffix = "" if len(unique_values) <= limit else f" +{len(unique_values) - limit} more"
    return ", ".join(visible) + suffix


def _latest_alerts_table(alerts_frame: pd.DataFrame, limit: int = 5) -> pd.DataFrame:
    if alerts_frame.empty:
        return pd.DataFrame(columns=["captured_utc", "current_ttc_alert", "details", "affected_service", "service_tag"])

    latest_snapshot_ts = alerts_frame["snapshot_ts_utc"].max()
    latest_snapshot = alerts_frame[alerts_frame["snapshot_ts_utc"] == latest_snapshot_ts].copy()
    if latest_snapshot.empty:
        return pd.DataFrame(columns=["captured_utc", "current_ttc_alert", "details", "affected_service", "service_tag"])

    latest_snapshot["alert_key"] = (
        latest_snapshot["alert_id"].fillna(latest_snapshot["header_text"]).fillna(latest_snapshot["description_text"])
    )
    latest_snapshot["header_text"] = latest_snapshot["header_text"].fillna("Service notice")
    latest_snapshot["description_text"] = latest_snapshot["description_text"].fillna(
        "No TTC alert description was provided in the live feed."
    )
    latest_snapshot["effect"] = latest_snapshot["effect"].fillna("Effect not specified")
    latest_snapshot["cause"] = latest_snapshot["cause"].fillna("Cause not specified")

    grouped = (
        latest_snapshot.groupby(["snapshot_ts_utc", "alert_key"], as_index=False)
        .agg(
            current_ttc_alert=("header_text", "first"),
            details=("description_text", "first"),
            service_tag=("alert_scope", lambda s: _join_unique(s, limit=2)),
            affected_routes=("route_id", _join_unique),
            affected_stops=("stop_id", _join_unique),
            effect=("effect", "first"),
            cause=("cause", "first"),
        )
        .sort_values(["snapshot_ts_utc", "current_ttc_alert"], ascending=[False, True])
        .head(limit)
    )
    grouped["captured_utc"] = grouped["snapshot_ts_utc"].dt.strftime("%Y-%m-%d %H:%M:%S %Z")
    grouped["details"] = grouped["details"].str.slice(0, 220)
    grouped["affected_service"] = grouped.apply(
        lambda row: f"Routes: {row['affected_routes']} | Stops: {row['affected_stops']}",
        axis=1,
    )
    return grouped[["captured_utc", "current_ttc_alert", "details", "affected_service", "service_tag", "effect", "cause"]]


def _hit_rate_frame(
    alerts_frame: pd.DataFrame,
    hotspot_frame: pd.DataFrame,
    route_universe_size: int,
) -> tuple[pd.DataFrame, dict[str, float]]:
    if alerts_frame.empty or hotspot_frame.empty or "route_id" not in alerts_frame.columns:
        empty = pd.DataFrame(columns=["series", "rate"])
        return empty, {"hit_rate": 0.0, "baseline_rate": 0.0, "lift": 0.0, "route_alert_count": 0.0}

    route_alerts = alerts_frame[alerts_frame["route_id"].notna()].copy()
    route_alerts["route_id"] = route_alerts["route_id"].astype(str)
    hotspot_routes = set(hotspot_frame["entity_id"].astype(str).tolist())
    hit_mask = route_alerts["route_id"].isin(hotspot_routes)
    hit_rate = float(hit_mask.mean()) if len(route_alerts) else 0.0
    baseline_rate = float(len(hotspot_routes) / route_universe_size) if route_universe_size else 0.0
    lift = float(hit_rate / baseline_rate) if baseline_rate else 0.0
    frame = pd.DataFrame(
        {
            "series": ["Historical hotspot baseline", "Live route hit rate"],
            "rate": [baseline_rate, hit_rate],
        }
    )
    return frame, {
        "hit_rate": hit_rate,
        "baseline_rate": baseline_rate,
        "lift": lift,
        "route_alert_count": float(len(route_alerts)),
    }


st.title("Live Alert Alignment")
st.caption("Use the scheduler archive to see what has been captured and whether it lands on historical hotspot routes.")
_auto_refresh_every(30)

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

if st.button("Refresh Alert Data", type="secondary"):
    _load_alert_archive.clear()
    _load_scheduler_cycles.clear()
    _load_hotspot_reference.clear()
    st.rerun()
archive_frame = _load_alert_archive()
cycle_frame = _load_scheduler_cycles()
hotspot_frame = _load_hotspot_reference(top_k=50)
route_universe_size = _load_route_universe_size()

if archive_frame.empty:
    st.info("No parsed live-alert archive is available yet. Start the scheduler or parse the latest GTFS-RT snapshots.")
    next_question_hint("Need technical caveats and data-quality diagnostics? Open: Technical Appendix.")
    st.stop()

archive_frame = archive_frame.dropna(subset=["snapshot_ts_utc"]).copy()
archive_frame = archive_frame.sort_values("snapshot_ts_utc")
archive_summary = (
    archive_frame.groupby("snapshot_ts_utc", as_index=False)
    .agg(
        alert_rows=("alert_id", "size"),
        route_rows=("route_id", lambda s: int(s.notna().sum())),
        stop_rows=("stop_id", lambda s: int(s.notna().sum())),
        route_plus_stop_rows=("alert_scope", lambda s: int((s == "route + stop tagged alert").sum())),
    )
    .sort_values("snapshot_ts_utc")
)
latest_alerts_table = _latest_alerts_table(archive_frame, limit=5)

total_captured_rows = int(len(archive_frame))
snapshot_count = int(archive_summary["snapshot_ts_utc"].nunique())
latest_capture_ts = archive_summary["snapshot_ts_utc"].max()
latest_capture_rows = int(archive_summary.iloc[-1]["alert_rows"]) if not archive_summary.empty else 0
route_linked_rows = int(archive_frame["route_id"].notna().sum())
stop_linked_rows = int(archive_frame["stop_id"].notna().sum())

validation_frame, hit_stats = _hit_rate_frame(archive_frame, hotspot_frame, route_universe_size)
alert_scope_frame = _alert_scope_distribution_frame(archive_frame)

page_story_header(
    audience_question="Are new GTFS-RT alert captures arriving, and do they land on historically bad routes?",
    takeaway=(
        f"The scheduler has captured {fmt_int(total_captured_rows)} alert rows across {fmt_int(snapshot_count)} snapshots; "
        f"route-linked alerts hit the historical hotspot set at {fmt_pct(hit_stats['hit_rate'])} "
        f"({hit_stats['lift']:.2f}x baseline)."
    ),
)
st.caption(
    "Hit rate shows how many current route-tagged TTC alerts land on historically bad routes, baseline rate is the share we would expect if alerts were spread across all routes, and lift shows how much stronger the real concentration is than that baseline."
)

kpi_a, kpi_b, kpi_c, kpi_d, kpi_e = st.columns(5)
kpi_a.metric("Captured Alert Rows", fmt_int(total_captured_rows))
kpi_b.metric("Snapshots Captured", fmt_int(snapshot_count))
kpi_c.metric("Latest Capture Rows", fmt_int(latest_capture_rows))
kpi_d.metric("Route Hit Rate", fmt_pct(hit_stats["hit_rate"]))
kpi_e.metric("Lift vs Baseline", f"{hit_stats['lift']:.2f}x")

if latest_capture_ts is not None and not pd.isna(latest_capture_ts):
    st.info(
        "Latest scheduler capture: "
        f"{latest_capture_ts.strftime('%Y-%m-%d %H:%M:%S %Z')} "
        f"with {fmt_int(latest_capture_rows)} parsed alert rows."
    )
    st.caption(f"Route-tagged rows: {fmt_int(route_linked_rows)} | Stop-tagged rows: {fmt_int(stop_linked_rows)}")

if not latest_alerts_table.empty:
    st.subheader("Current TTC alerts")
    st.dataframe(latest_alerts_table, use_container_width=True, hide_index=True)
    st.caption("These are the most recent distinct TTC service alerts from the latest scheduler capture.")

timeline_chart = (
    alt.Chart(archive_summary)
    .mark_line(point=True)
    .encode(
        x=alt.X("snapshot_ts_utc:T", title="Capture Time"),
        y=alt.Y("alert_rows:Q", title="Parsed Alert Rows"),
        tooltip=["snapshot_ts_utc:T", "alert_rows:Q", "route_rows:Q", "stop_rows:Q"],
    )
    .properties(title="Scheduler Capture Timeline", height=300)
)
st.altair_chart(timeline_chart, use_container_width=True)
st.caption("Each point is one scheduler cycle. A larger value means that cycle parsed more live alert entities.")

if not alert_scope_frame.empty:
    scope_chart_height = max(260, len(alert_scope_frame.index) * 70)
    selector_chart = (
        alt.Chart(alert_scope_frame)
        .mark_bar()
        .encode(
            x=alt.X("alert_count:Q", title="Alert Rows"),
            y=alt.Y("alert_scope:N", sort="-x", title="Alert Scope"),
            color=alt.Color("alert_scope:N", legend=None),
            tooltip=["alert_scope:N", "alert_count:Q"],
        )
        .properties(title="Which parts of the network are tagged in live alerts?", height=scope_chart_height)
    )
    st.altair_chart(selector_chart, use_container_width=True)
    st.caption("Route-tagged alerts can be matched directly to historical hotspot routes; stop-tagged alerts still provide live disruption context.")

if not validation_frame.empty:
    validation_chart = (
        alt.Chart(validation_frame)
        .mark_bar()
        .encode(
            x=alt.X("rate:Q", title="Share"),
            y=alt.Y("series:N", sort="-x", title="Reference"),
            color=alt.Color("series:N", legend=None),
            tooltip=["series:N", alt.Tooltip("rate:Q", format=".2%")],
        )
        .properties(title="Historical hotspot baseline vs live hit rate", height=220)
    )
    st.altair_chart(validation_chart, use_container_width=True)

    st.caption(
        f"Route-tagged alerts: {fmt_int(hit_stats['route_alert_count'])} | "
        f"Baseline rate: {fmt_pct(hit_stats['baseline_rate'])} | "
        f"Lift: {hit_stats['lift']:.2f}x"
    )
    if route_universe_size:
        st.caption(f"Historical route universe for baseline: {fmt_int(route_universe_size)} routes.")

    if not presentation:
        hotspot_display = hotspot_frame[["mode", "entity_id", "frequency", "composite_score", "rank_position"]].copy()
        hotspot_display = hotspot_display.rename(columns={"entity_id": "route_id"})
        st.dataframe(hotspot_display.head(10), use_container_width=True, hide_index=True)

if not presentation:
    with st.expander("How this validation works"):
        st.markdown(
            "- The reference set is the latest route-level hotspot ranking.\n"
            "- `HitRate` is the share of route-tagged live alerts whose route is in the hotspot set.\n"
            "- `Lift` compares that share to the baseline share of the same hotspot set size within the historical route universe."
        )

next_question_hint("Need technical caveats and data-quality diagnostics? Open: Technical Appendix.")
