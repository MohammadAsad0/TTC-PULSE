from __future__ import annotations

import os
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

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

from ttc_pulse.alerts.live_alert_scheduler import DEFAULT_FEED_URLS, LiveAlertPollingManager
from ttc_pulse.dashboard.formatting import fmt_int, fmt_pct
from ttc_pulse.dashboard.loaders import query_table
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector, sync_dashboard_data_cache

sync_dashboard_data_cache()
from ttc_pulse.utils.project_setup import resolve_project_paths


_LOCAL_TZ = ZoneInfo("America/Toronto")
_DEFAULT_TTC_FEED_URLS = [*DEFAULT_FEED_URLS]


def _project_file(*parts: str) -> Path:
    return resolve_project_paths().project_root.joinpath(*parts)


def _resolve_feed_urls() -> list[str]:
    env_value = (os.getenv("TTC_GTFSRT_FEED_URLS") or os.getenv("TTC_GTFSRT_ALERT_FEED_URLS") or "").strip()
    if not env_value:
        return _DEFAULT_TTC_FEED_URLS

    parts = [segment.strip() for segment in env_value.replace("\n", ",").split(",")]
    urls = [url for url in parts if url]
    return urls or _DEFAULT_TTC_FEED_URLS


@st.cache_resource
def _get_live_polling_manager(feed_urls: tuple[str, ...]) -> LiveAlertPollingManager:
    manager = LiveAlertPollingManager(feed_urls=feed_urls, poll_seconds=30)
    manager.start()
    return manager


def _sync_live_state_into_session(manager: LiveAlertPollingManager) -> None:
    snapshot = manager.snapshot()
    st.session_state["live_alert_runtime_current_alerts"] = snapshot.get("current_alerts", [])
    st.session_state["live_alert_runtime_poll_timeline"] = snapshot.get("poll_timeline", [])
    st.session_state["live_alert_runtime_new_alert_events"] = snapshot.get("new_alert_events", [])
    st.session_state["live_alert_runtime_last_poll_utc"] = snapshot.get("last_poll_utc", "")
    st.session_state["live_alert_runtime_last_error"] = snapshot.get("last_error", "")


def _format_local_timestamp(value: pd.Timestamp | str | None) -> str:
    if value is None:
        return "N/A"
    timestamp = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(timestamp):
        return "N/A"
    return timestamp.tz_convert(_LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


@st.cache_data(ttl=5)
def _load_alert_archive() -> pd.DataFrame:
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
            if column in {"cause", "effect"}:
                frame[column] = frame[column].str.upper()
                frame.loc[
                    frame[column].isin(["UNKNOWN_CAUSE", "UNKNOWN_EFFECT", "CAUSE_UNKNOWN", "EFFECT_UNKNOWN"]),
                    column,
                ] = pd.NA

    has_route = frame["route_id"].notna() if "route_id" in frame.columns else pd.Series(False, index=frame.index)
    has_stop = frame["stop_id"].notna() if "stop_id" in frame.columns else pd.Series(False, index=frame.index)
    frame["alert_scope"] = "network-wide notice"
    frame.loc[has_route & ~has_stop, "alert_scope"] = "route-tagged alert"
    frame.loc[~has_route & has_stop, "alert_scope"] = "stop-tagged alert"
    frame.loc[has_route & has_stop, "alert_scope"] = "route + stop tagged alert"
    return frame


@st.cache_data(ttl=30)
def _load_hotspot_reference(top_k: int = 50) -> pd.DataFrame:
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


@st.cache_data(ttl=300)
def _load_route_mode_lookup() -> dict[str, str]:
    route_dim_path = _project_file("dimensions", "dim_route_gtfs.parquet")
    if not route_dim_path.exists():
        return {}

    frame = pd.read_parquet(route_dim_path)
    if frame.empty or "route_id" not in frame.columns or "route_mode" not in frame.columns:
        return {}

    frame = frame[["route_id", "route_mode"]].copy()
    frame["route_id"] = frame["route_id"].astype("string").str.strip()
    frame["route_mode"] = frame["route_mode"].astype("string").str.lower().str.strip()
    frame = frame[frame["route_id"].notna() & (frame["route_id"] != "")]
    frame = frame[frame["route_mode"].isin(["bus", "subway", "streetcar"])]
    frame = frame.drop_duplicates(subset=["route_id"], keep="first")
    return dict(zip(frame["route_id"].astype(str), frame["route_mode"].astype(str)))


def _join_unique(values: pd.Series, limit: int = 4) -> str:
    unique_values = [value for value in values.dropna().astype(str).str.strip().unique().tolist() if value]
    if not unique_values:
        return "None listed"
    visible = unique_values[:limit]
    suffix = "" if len(unique_values) <= limit else f" +{len(unique_values) - limit} more"
    return ", ".join(visible) + suffix


def _alert_scope_distribution_frame(alerts_frame: pd.DataFrame) -> pd.DataFrame:
    if alerts_frame.empty:
        return pd.DataFrame(columns=["alert_scope", "alert_count"])

    frame = (
        alerts_frame.groupby("alert_scope", as_index=False)
        .size()
        .rename(columns={"size": "alert_count"})
    )
    allowed_scopes = [
        "route-tagged alert",
        "route + stop tagged alert",
        "network-wide notice",
    ]
    frame = frame[frame["alert_scope"].isin(allowed_scopes)].copy()
    if frame.empty:
        frame = pd.DataFrame({"alert_scope": allowed_scopes, "alert_count": [0, 0, 0]})
    else:
        existing = set(frame["alert_scope"].tolist())
        for scope in allowed_scopes:
            if scope not in existing:
                frame = pd.concat([frame, pd.DataFrame([{"alert_scope": scope, "alert_count": 0}])], ignore_index=True)
    order = {scope: idx for idx, scope in enumerate(allowed_scopes)}
    frame["scope_order"] = frame["alert_scope"].map(order).fillna(99).astype(int)
    return frame.sort_values("scope_order").drop(columns=["scope_order"])


def _latest_archive_alerts_table(
    alerts_frame: pd.DataFrame,
    route_mode_lookup: dict[str, str],
    limit: int = 5,
    mode_filter: str = "all",
) -> pd.DataFrame:
    empty_cols = [
        "captured_local",
        "mode",
        "current_ttc_alert",
        "details",
        "affected_service",
        "service_tag",
        "effect",
        "cause",
    ]
    if alerts_frame.empty:
        return pd.DataFrame(columns=empty_cols)

    latest_snapshot_ts = alerts_frame["snapshot_ts_utc"].max()
    latest_snapshot = alerts_frame[alerts_frame["snapshot_ts_utc"] == latest_snapshot_ts].copy()
    if latest_snapshot.empty:
        return pd.DataFrame(columns=empty_cols)

    latest_snapshot["route_id_norm"] = latest_snapshot["route_id"].astype("string").str.strip()
    latest_snapshot["route_mode"] = latest_snapshot["route_id_norm"].map(route_mode_lookup)
    latest_snapshot["route_mode"] = latest_snapshot["route_mode"].fillna("unknown")

    selected_mode = str(mode_filter or "all").lower().strip()
    if selected_mode != "all":
        latest_snapshot = latest_snapshot[latest_snapshot["route_mode"] == selected_mode].copy()
        if latest_snapshot.empty:
            return pd.DataFrame(columns=empty_cols)

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
            mode=("route_mode", lambda s: _join_unique(s, limit=2)),
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
    grouped["captured_local"] = grouped["snapshot_ts_utc"].apply(_format_local_timestamp)
    grouped["details"] = grouped["details"].str.slice(0, 220)
    grouped["affected_service"] = grouped.apply(
        lambda row: f"Routes: {row['affected_routes']} | Stops: {row['affected_stops']}",
        axis=1,
    )
    grouped["mode"] = grouped["mode"].replace({"unknown": "Unspecified"})
    return grouped[["captured_local", "mode", "current_ttc_alert", "details", "affected_service", "service_tag", "effect", "cause"]]


def _runtime_poll_timeline_frame(poll_timeline: list[dict[str, object]]) -> pd.DataFrame:
    if not poll_timeline:
        return pd.DataFrame(columns=["polled_at_utc", "cumulative_distinct_alerts", "new_alert_count", "parse_rows", "status"])

    frame = pd.DataFrame(poll_timeline)
    if frame.empty:
        return frame
    frame["polled_at_utc"] = pd.to_datetime(frame["polled_at_utc"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["polled_at_utc"]).sort_values("polled_at_utc")
    for col in ["total_alert_count", "new_alert_count", "parse_rows", "cumulative_distinct_alerts"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce").fillna(0)
    if "cumulative_distinct_alerts" not in frame.columns:
        frame["cumulative_distinct_alerts"] = frame["total_alert_count"] if "total_alert_count" in frame.columns else 0
    return frame


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


@st.fragment(run_every="30s")
def _render_live_updates(manager: LiveAlertPollingManager, route_mode_lookup: dict[str, str]) -> None:
    _sync_live_state_into_session(manager)
    runtime_error = str(st.session_state.get("live_alert_runtime_last_error") or "").strip()
    if runtime_error:
        st.warning(f"Latest scheduler poll warning: {runtime_error}")

    archive_live = _load_alert_archive()
    poll_timeline = list(st.session_state.get("live_alert_runtime_poll_timeline", []))
    poll_timeline_frame = _runtime_poll_timeline_frame(poll_timeline)
    archive_summary_live = (
        archive_live.dropna(subset=["snapshot_ts_utc"])
        .groupby("snapshot_ts_utc", as_index=False)
        .agg(
            alert_rows=("alert_id", "size"),
            route_rows=("route_id", lambda s: int(s.notna().sum())),
            stop_rows=("stop_id", lambda s: int(s.notna().sum())),
        )
        .sort_values("snapshot_ts_utc")
    )

    if st.session_state.get("live_alert_runtime_last_poll_utc"):
        last_runtime_poll_utc = str(st.session_state.get("live_alert_runtime_last_poll_utc") or "")
        runtime_alerts = list(st.session_state.get("live_alert_runtime_current_alerts", []))
        st.info(
            "Latest scheduler capture: "
            + f"{_format_local_timestamp(last_runtime_poll_utc)} "
            + f"with {fmt_int(len(runtime_alerts))} current distinct alerts."
        )
    elif not archive_summary_live.empty:
        latest_snapshot_ts = archive_summary_live["snapshot_ts_utc"].max()
        latest_alert_rows = int(archive_summary_live.iloc[-1]["alert_rows"])
        st.info(
            "Latest scheduler capture: "
            + f"{_format_local_timestamp(latest_snapshot_ts)} "
            + f"with {fmt_int(latest_alert_rows)} parsed alert rows."
        )

    alerts_header_col, alerts_mode_col, alerts_limit_col = st.columns([5, 2, 2], vertical_alignment="bottom")
    with alerts_header_col:
        st.subheader("Current TTC alerts")
    with alerts_mode_col:
        selected_alert_mode = st.selectbox(
            "Mode",
            options=["all", "bus", "subway", "streetcar"],
            index=0,
            format_func=lambda value: "All" if value == "all" else str(value).title(),
            key="live_alert_current_mode_filter",
        )
    with alerts_limit_col:
        selected_alert_limit = st.number_input(
            "Rows",
            min_value=1,
            max_value=500,
            value=5,
            step=1,
            key="live_alert_current_limit",
        )

    latest_alerts_table = _latest_archive_alerts_table(
        alerts_frame=archive_live,
        route_mode_lookup=route_mode_lookup,
        limit=int(selected_alert_limit),
        mode_filter=str(selected_alert_mode),
    )

    if latest_alerts_table.empty:
        st.info("No current TTC alerts match the selected mode filter.")
    else:
        st.dataframe(latest_alerts_table, width="stretch", hide_index=True)
        st.caption("These are the most recent distinct TTC service alerts from the latest scheduler capture.")

    if not archive_summary_live.empty:
        timeline_chart = (
            alt.Chart(archive_summary_live)
            .mark_line(point=True)
            .encode(
                x=alt.X("snapshot_ts_utc:T", title="Capture Time"),
                y=alt.Y("alert_rows:Q", title="Parsed Alert Rows"),
                tooltip=[
                    alt.Tooltip("snapshot_ts_utc:T", title="Capture Time"),
                    alt.Tooltip("alert_rows:Q", title="Parsed Alert Rows"),
                    alt.Tooltip("route_rows:Q", title="Route Rows"),
                    alt.Tooltip("stop_rows:Q", title="Stop Rows"),
                ],
            )
            .properties(title="Scheduler Capture Timeline", height=300)
        )
        st.altair_chart(timeline_chart, width="stretch")
        st.caption("Each point is one scheduler cycle. A larger value means that cycle parsed more live alert entities.")


st.title("Live Alert Alignment")
st.caption("Live polling uses APScheduler every 30 seconds. Displayed times are shown in EDT/EST.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

manager = _get_live_polling_manager(tuple(_resolve_feed_urls()))
_sync_live_state_into_session(manager)

if st.button("Refresh Alert Data", type="secondary"):
    with st.spinner("Running immediate live poll and parse..."):
        refresh_result = manager.trigger_now()
        st.session_state["live_alert_refresh_notice"] = refresh_result
    _load_alert_archive.clear()
    _load_hotspot_reference.clear()
    st.rerun()

refresh_result = st.session_state.pop("live_alert_refresh_notice", None)
if isinstance(refresh_result, dict):
    if str(refresh_result.get("status") or "") == "error":
        st.error(f"Refresh failed: {refresh_result.get('error', 'Unknown error')}")
    else:
        st.success(
            "Manual refresh completed. "
            f"New alerts: {fmt_int(int(refresh_result.get('new_alert_count', 0) or 0))}. "
            f"Total current alerts: {fmt_int(int(refresh_result.get('total_alert_count', 0) or 0))}."
        )

archive_frame = _load_alert_archive()
hotspot_frame = _load_hotspot_reference(top_k=50)
route_universe_size = _load_route_universe_size()
route_mode_lookup = _load_route_mode_lookup()

if archive_frame.empty:
    last_poll_utc = str(st.session_state.get("live_alert_runtime_last_poll_utc") or "")
    if last_poll_utc:
        st.info(f"Latest scheduler capture: {_format_local_timestamp(last_poll_utc)}")
    st.warning(
        "No parsed live-alert archive rows are available yet in alerts/parsed/service_alert_entities.csv. "
        "Polling is active; the feed may currently have no entities or parsing has not produced rows yet."
    )
    _render_live_updates(manager, route_mode_lookup)
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

st.caption(f"Route-tagged rows: {fmt_int(route_linked_rows)} | Stop-tagged rows: {fmt_int(stop_linked_rows)}")

_render_live_updates(manager, route_mode_lookup)

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
    st.altair_chart(selector_chart, width="stretch")

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
    st.altair_chart(validation_chart, width="stretch")

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
        st.dataframe(hotspot_display.head(10), width="stretch", hide_index=True)

if not presentation:
    with st.expander("How this validation works"):
        st.markdown(
            "- The reference set is the latest route-level hotspot ranking.\n"
            "- `HitRate` is the share of route-tagged live alerts whose route is in the hotspot set.\n"
            "- `Lift` compares that share to the baseline share of the same hotspot set size within the historical route universe."
        )

next_question_hint("Need technical caveats and data-quality diagnostics? Open: Technical Appendix.")









