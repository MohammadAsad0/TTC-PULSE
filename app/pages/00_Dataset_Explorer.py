from __future__ import annotations

from datetime import date
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

from ttc_pulse.dashboard.loaders import get_dataset_coverage, load_dataset_rows, resolve_dataset_path
from ttc_pulse.dashboard.storytelling import is_presentation_mode, mark_dataset_reloaded, page_story_header, story_mode_selector, sync_dashboard_data_cache

sync_dashboard_data_cache()
from ttc_pulse.pipeline.load_dataset import run_load_dataset
from ttc_pulse.utils.project_setup import resolve_project_paths

MODE_COLUMNS: dict[str, list[str]] = {
    "bus": [
        "service_date",
        "event_ts",
        "route_label_raw",
        "route_short_name_norm",
        "route_id_gtfs",
        "location_text_raw",
        "station_text_raw",
        "incident_text_raw",
        "incident_code_raw",
        "incident_category",
        "min_delay",
        "min_gap",
        "direction_raw",
        "direction_norm",
        "vehicle_id_raw",
        "match_method",
        "match_confidence",
        "link_status",
        "source_file",
        "source_row_id",
        "ingested_at",
    ],
    "streetcar": [
        "service_date",
        "event_ts",
        "route_label_raw",
        "route_short_name_norm",
        "route_id_gtfs",
        "location_text_raw",
        "station_text_raw",
        "incident_text_raw",
        "incident_code_raw",
        "incident_category",
        "min_delay",
        "min_gap",
        "direction_raw",
        "direction_norm",
        "vehicle_id_raw",
        "match_method",
        "match_confidence",
        "link_status",
        "source_file",
        "source_row_id",
        "ingested_at",
    ],
    "subway": [
        "service_date",
        "event_ts",
        "line_code_raw",
        "line_code_norm",
        "route_id_gtfs",
        "station_text_raw",
        "station_canonical",
        "incident_code_raw",
        "incident_category",
        "min_delay",
        "min_gap",
        "direction_raw",
        "direction_norm",
        "vehicle_id_raw",
        "match_method",
        "match_confidence",
        "link_status",
        "source_file",
        "source_row_id",
        "ingested_at",
    ],
}


def _normalize_date_range(selection: object, min_date: date, max_date: date) -> tuple[date, date]:
    if isinstance(selection, tuple) and len(selection) == 2:
        start_date, end_date = selection
    else:
        start_date = end_date = selection if isinstance(selection, date) else min_date

    if start_date is None:
        start_date = min_date
    if end_date is None:
        end_date = max_date
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


@st.cache_data(ttl=120)
def _load_coverage(mode: str):
    return get_dataset_coverage(mode=mode)


@st.cache_data(ttl=120)
def _load_rows(mode: str, start_date: str, end_date: str, row_limit: int, filter_route: str | None = None, filter_station: str | None = None):
    return load_dataset_rows(mode=mode, start_date=start_date, end_date=end_date, limit=row_limit, filter_route=filter_route, filter_station=filter_station)

@st.cache_data(ttl=300)
def _load_route_catalog(route_mode: str) -> pd.DataFrame:
    route_file = resolve_project_paths().project_root / "dimensions" / "dim_route_gtfs.parquet"
    if not route_file.exists():
        return pd.DataFrame(columns=["route_id", "route_short_name", "route_long_name"])
    frame = pd.read_parquet(route_file)
    if frame.empty:
        return pd.DataFrame(columns=["route_id", "route_short_name", "route_long_name"])
    if "route_mode" in frame.columns:
        frame = frame[frame["route_mode"].astype(str).str.lower() == route_mode].copy()
    if frame.empty:
        return pd.DataFrame(columns=["route_id", "route_short_name", "route_long_name"])
    frame["route_id"] = frame.get("route_id", "").astype(str).str.strip()
    frame["route_short_name"] = frame.get("route_short_name", "").astype(str).str.strip()
    frame["route_long_name"] = frame.get("route_long_name", "").astype(str).str.strip()
    frame = frame[frame["route_id"] != ""].copy()
    frame = frame.drop_duplicates(subset=["route_id"], keep="first")
    frame.loc[frame["route_short_name"] == "", "route_short_name"] = frame["route_id"]
    return frame[["route_id", "route_short_name", "route_long_name"]]


def _format_entity_label(mode: str, entity_id: object, entity_labels: dict[str, str] | None = None, prefix: str = "Route") -> str:
    entity_text = str(entity_id)
    if entity_labels and entity_text in entity_labels:
        return entity_labels[entity_text]
    if mode in {"bus", "streetcar"}:
        return f"Streetcar Route {entity_text}" if mode == "streetcar" else f"Route {entity_text}"
    return entity_text


@st.cache_data(ttl=3600)
def _get_explorer_catalog(mode: str) -> list[str]:
    from ttc_pulse.dashboard.loaders import query_table
    if mode == "subway":
        res = query_table(
            table_name="gold_station_time_metrics",
            query_template="SELECT DISTINCT station_canonical FROM {source} WHERE station_canonical IS NOT NULL ORDER BY station_canonical"
        )
        if res.status in ("ok", "empty") and not res.frame.empty:
            return res.frame["station_canonical"].tolist()
    else:
        res = query_table(
            table_name="gold_route_time_metrics",
            query_template=f"SELECT DISTINCT route_id_gtfs FROM {{source}} WHERE route_id_gtfs IS NOT NULL AND mode = '{mode}' ORDER BY TRY_CAST(route_id_gtfs AS INTEGER), route_id_gtfs"
        )
        if res.status in ("ok", "empty") and not res.frame.empty:
            return res.frame["route_id_gtfs"].tolist()
    return []



header_left, header_right = st.columns([4, 1], vertical_alignment="bottom")
with header_left:
    st.title("Dataset Explorer")
with header_right:
    paths = resolve_project_paths()
    raw_roots = [
        paths.project_root / "data" / "bus",
        paths.project_root / "data" / "streetcar",
        paths.project_root / "data" / "subway",
        paths.project_root / "data" / "gtfs",
    ]
    raw_ready = all(root.exists() for root in raw_roots)

    if st.button("Load Dataset", type="primary", use_container_width=True, disabled=not raw_ready):
        with st.spinner("Loading raw CSV files into DuckDB and Parquet..."):
            try:
                load_result = run_load_dataset()
            except TimeoutError as exc:  # pragma: no cover
                st.error(
                    "Dataset load is already running in another session/process. "
                    "Please wait for it to finish and retry. "
                    f"Details: {exc}"
                )
            except RuntimeError as exc:  # pragma: no cover
                st.error(f"Dataset load failed due to concurrent DuckDB writers: {exc}")
            except Exception as exc:  # pragma: no cover
                st.error(f"Dataset load failed: {type(exc).__name__}: {exc}")
            else:
                st.success("Dataset load completed.")
                st.cache_data.clear()
                st.cache_resource.clear()
                mark_dataset_reloaded()
                sync_dashboard_data_cache()
                highlights = load_result.get("highlights", {})
                st.caption(
                    " | ".join(
                        [
                            f"Bus: {highlights.get('silver_bus_rows', 0):,}",
                            f"Streetcar: {highlights.get('silver_streetcar_rows', 0):,}",
                            f"Subway: {highlights.get('silver_subway_rows', 0):,}",
                            f"Fact: {highlights.get('fact_delay_events_norm_rows', 0):,}",
                        ]
                    )
                )
    if not raw_ready:
        st.caption("Raw CSV folders not found. Running from existing DuckDB/Parquet artifacts.")

st.caption("Inspect the row-level TTC delay dataset by mode and service-date window.")

presentation = is_presentation_mode(story_mode_selector(sidebar=True, key="story_mode"))
page_story_header(
    audience_question="What raw-like event rows are behind the dashboard?",
    takeaway="This page exposes the row-level bus and subway event dataset with source lineage so date-window checks are transparent.",
)

selected_mode = st.radio("Dataset", options=["bus", "streetcar", "subway"], horizontal=True, format_func=str.title)
coverage_result = _load_coverage(selected_mode)
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()

coverage_frame = coverage_result.frame.copy()
if coverage_frame.empty:
    st.info("No dataset coverage is available.")
    st.stop()

min_service_date = pd.to_datetime(coverage_frame.loc[0, "min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_frame.loc[0, "max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.error("The dataset does not expose a valid service-date range.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()

control_a, control_b, control_c = st.columns([2, 1, 1])
with control_a:
    selected_window = st.date_input(
        "Service Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

filter_route = None
filter_station = None
catalog = _get_explorer_catalog(selected_mode)

bus_route_labels = {}
if selected_mode in {"bus", "streetcar"}:
    route_df = _load_route_catalog(selected_mode)
    prefix = "Streetcar Route" if selected_mode == "streetcar" else "Route"
    for row in route_df.itertuples(index=False):
        rid = getattr(row, "route_id", "")
        if not rid: continue
        label = f"{prefix} {rid}"
        sname = getattr(row, "route_short_name", "")
        lname = getattr(row, "route_long_name", "")
        if sname and sname != rid:
            label += f" ({sname})"
        if lname:
            label += f" - {lname}"
        bus_route_labels[rid] = label

with control_b:
    if selected_mode == "subway":
        if catalog:
            selected_entity = st.selectbox(
                "Filter Station", 
                options=["All"] + catalog,
                format_func=lambda x: "All Stations" if x == "All" else x
            )
            if selected_entity != "All":
                filter_station = selected_entity
    else:
        if catalog:
            selected_entity = st.selectbox(
                "Filter Route", 
                options=["All"] + catalog,
                format_func=lambda x: "All Routes" if x == "All" else _format_entity_label(selected_mode, x, bus_route_labels)
            )
            if selected_entity != "All":
                filter_route = selected_entity

with control_c:
    row_limit = st.selectbox("Rows to Show", options=[100, 250, 500, 1000, 5000], index=2)


start_date, end_date = _normalize_date_range(selected_window, min_date, max_date)
start_iso = start_date.isoformat()
end_iso = end_date.isoformat()

rows_result = _load_rows(selected_mode, start_iso, end_iso, int(row_limit), filter_route=filter_route, filter_station=filter_station)

if rows_result.status in {"missing", "error"}:
    st.error(rows_result.message)
    st.stop()
if rows_result.status == "empty" or rows_result.frame.empty:
    st.info("No rows match the selected dataset and service-date range.")
    st.stop()

frame = rows_result.frame.copy()
display_columns = [column for column in MODE_COLUMNS[selected_mode] if column in frame.columns]
if not display_columns:
    display_columns = frame.columns.tolist()

filtered_rows = rows_result.total_row_count if rows_result.total_row_count is not None else len(frame)
st.caption(
    f"Filtered rows in window: {filtered_rows:,}. Showing first {int(row_limit):,} rows."
)

metric_a, metric_b, metric_c = st.columns(3)
metric_a.metric("Rows Displayed", f"{len(frame):,}")
metric_b.metric("Window Start", start_iso)
metric_c.metric("Window End", end_iso)

if not presentation:
    dataset_path = resolve_dataset_path(selected_mode)
    st.caption(
        f"Source: `{dataset_path.as_posix()}`. This page reads the normalized row-level dataset that preserves original text columns and lineage fields."
    )

st.dataframe(frame[display_columns], use_container_width=True, hide_index=True)

csv_bytes = frame[display_columns].to_csv(index=False).encode("utf-8")
st.download_button(
    "Download current view as CSV",
    data=csv_bytes,
    file_name=f"ttc_pulse_{selected_mode}_{start_iso}_to_{end_iso}.csv",
    mime="text/csv",
)
