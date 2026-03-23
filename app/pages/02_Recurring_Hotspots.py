from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import altair as alt
import pandas as pd
import pydeck as pdk
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

from ttc_pulse.dashboard.formatting import fmt_float, fmt_int
from ttc_pulse.dashboard.loaders import query_table, resolve_project_root
from ttc_pulse.dashboard.metric_config import METRIC_OPTIONS, metric_axis_title, resolve_metric_choice
from ttc_pulse.dashboard.storytelling import is_presentation_mode, next_question_hint, page_story_header, story_mode_selector


@st.cache_data(ttl=120)
def _load_bus_coverage_window():
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE mode = 'bus'
            AND route_id_gtfs IS NOT NULL
            AND service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_station_coverage_window():
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        SELECT
            MIN(service_date) AS min_service_date,
            MAX(service_date) AS max_service_date
        FROM {source}
        WHERE station_canonical IS NOT NULL
            AND service_date IS NOT NULL
        """,
    )


@st.cache_data(ttl=120)
def _load_bus_route_rankings(start_date: str, end_date: str):
    return query_table(
        table_name="gold_route_time_metrics",
        query_template="""
        WITH route_agg AS (
            SELECT
                route_id_gtfs AS entity_id,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE mode = 'bus'
                AND route_id_gtfs IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                entity_id,
                frequency,
                severity_p90,
                regularity_p90,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (
                    0.35 * COALESCE((frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_SAMP(frequency) OVER(), 0.0), 0.0)
                    + 0.30 * COALESCE((severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_SAMP(severity_p90) OVER(), 0.0), 0.0)
                    + 0.20 * COALESCE((regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_SAMP(regularity_p90) OVER(), 0.0), 0.0)
                    + 0.15 * COALESCE(cause_mix_score, 0.0)
                ) AS composite_score
            FROM route_agg
        )
        SELECT
            entity_id,
            RANK() OVER (ORDER BY composite_score DESC NULLS LAST, frequency DESC) AS rank_position,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY rank_position ASC, entity_id
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_station_rankings(start_date: str, end_date: str):
    return query_table(
        table_name="gold_station_time_metrics",
        query_template="""
        WITH station_agg AS (
            SELECT
                station_canonical AS entity_id,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
            FROM {source}
            WHERE station_canonical IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        ),
        scored AS (
            SELECT
                entity_id,
                frequency,
                severity_p90,
                regularity_p90,
                COALESCE(cause_mix_score, 0.0) AS cause_mix_score,
                (
                    0.35 * COALESCE((frequency - AVG(frequency) OVER()) / NULLIF(STDDEV_SAMP(frequency) OVER(), 0.0), 0.0)
                    + 0.30 * COALESCE((severity_p90 - AVG(severity_p90) OVER()) / NULLIF(STDDEV_SAMP(severity_p90) OVER(), 0.0), 0.0)
                    + 0.20 * COALESCE((regularity_p90 - AVG(regularity_p90) OVER()) / NULLIF(STDDEV_SAMP(regularity_p90) OVER(), 0.0), 0.0)
                    + 0.15 * COALESCE(cause_mix_score, 0.0)
                ) AS composite_score
            FROM station_agg
        )
        SELECT
            entity_id,
            RANK() OVER (ORDER BY composite_score DESC NULLS LAST, frequency DESC) AS rank_position,
            CAST(frequency AS BIGINT) AS frequency,
            severity_p90,
            regularity_p90,
            cause_mix_score,
            composite_score
        FROM scored
        ORDER BY rank_position ASC, entity_id
        """,
        params=[start_date, end_date],
    )


@st.cache_data(ttl=120)
def _load_subway_spatial_hotspot():
    return query_table(
        table_name="gold_spatial_hotspot",
        query_template="""
        SELECT
            mode,
            spatial_unit_type,
            spatial_unit_id,
            centroid_lat,
            centroid_lon,
            frequency,
            severity_p90,
            regularity_p90,
            composite_score,
            confidence_score
        FROM {source}
        WHERE mode = 'subway'
        """,
    )


@st.cache_data(ttl=120)
def _load_bus_spatial_hotspot_provisional(start_date: str, end_date: str):
    bridge_path = resolve_project_root() / "bridge" / "bridge_route_direction_stop.parquet"
    if not bridge_path.exists():
        return query_table(
            table_name="gold_route_time_metrics",
            query_template="SELECT * FROM {source} WHERE FALSE",
        )

    escaped_bridge_path = bridge_path.as_posix().replace("'", "''")
    return query_table(
        table_name="gold_route_time_metrics",
        query_template=f"""
        WITH route_centroids AS (
            SELECT
                route_id AS route_id_gtfs,
                AVG(stop_lat) AS centroid_lat,
                AVG(stop_lon) AS centroid_lon
            FROM read_parquet('{escaped_bridge_path}')
            WHERE route_id IS NOT NULL
                AND stop_lat IS NOT NULL
                AND stop_lon IS NOT NULL
            GROUP BY 1
        ),
        route_agg AS (
            SELECT
                route_id_gtfs AS spatial_unit_id,
                SUM(frequency)::DOUBLE AS frequency,
                quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                AVG(composite_score) FILTER (WHERE composite_score IS NOT NULL) AS composite_score
            FROM {{source}}
            WHERE mode = 'bus'
                AND route_id_gtfs IS NOT NULL
                AND service_date BETWEEN ? AND ?
            GROUP BY 1
        )
        SELECT
            'bus' AS mode,
            'route_centroid_provisional' AS spatial_unit_type,
            r.spatial_unit_id,
            c.centroid_lat,
            c.centroid_lon,
            r.frequency,
            r.severity_p90,
            r.regularity_p90,
            r.composite_score,
            0.55 AS confidence_score
        FROM route_agg AS r
        JOIN route_centroids AS c
            ON r.spatial_unit_id = c.route_id_gtfs
        """,
        params=[start_date, end_date],
    )


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


st.title("Recurring Hotspots")
st.caption("This page exists because it helps the audience understand which entities repeatedly drive TTC reliability risk.")

mode = story_mode_selector(sidebar=True, key="story_mode")
presentation = is_presentation_mode(mode)

page_story_header(
    audience_question="Which routes or stations are repeatedly the worst performers?",
    takeaway="A small set of entities consistently appears at the top of the risk ranking across the selected window.",
)

selected_mode = st.radio("Entity scope", options=["bus", "subway"], horizontal=True)
coverage_result = _load_bus_coverage_window() if selected_mode == "bus" else _load_station_coverage_window()
if coverage_result.status in {"missing", "error"}:
    st.error(coverage_result.message)
    st.stop()
if coverage_result.status == "empty" or coverage_result.frame.empty:
    st.info("No service-date rows are available for this mode.")
    st.stop()

coverage_row = coverage_result.frame.iloc[0]
min_service_date = pd.to_datetime(coverage_row["min_service_date"], errors="coerce")
max_service_date = pd.to_datetime(coverage_row["max_service_date"], errors="coerce")
if pd.isna(min_service_date) or pd.isna(max_service_date):
    st.info("Coverage dates are unavailable.")
    st.stop()

min_date = min_service_date.date()
max_date = max_service_date.date()

selected_metric_label = st.selectbox("Metric", options=METRIC_OPTIONS, index=0)
default_top_n = 15 if presentation else 25
top_n = st.slider("Top N", min_value=5, max_value=100, value=default_top_n, step=5)

if selected_mode == "subway":
    min_frequency = st.slider("Minimum Frequency", min_value=0, max_value=500, value=0 if not presentation else 20, step=5)
else:
    min_frequency = 0

date_selection = st.date_input(
    "Service date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    key="hotspot_date_range",
)
selected_start_date, selected_end_date = _normalize_date_range(date_selection, min_date=min_date, max_date=max_date)
selected_start_iso = selected_start_date.isoformat()
selected_end_iso = selected_end_date.isoformat()

ranking_result = (
    _load_bus_route_rankings(selected_start_iso, selected_end_iso)
    if selected_mode == "bus"
    else _load_station_rankings(selected_start_iso, selected_end_iso)
)
if ranking_result.status in {"missing", "error"}:
    st.error(ranking_result.message)
    st.stop()
if ranking_result.status == "empty":
    st.info("No ranking rows are available in this window.")
    st.stop()

ranking = ranking_result.frame.copy()
ranking = ranking[ranking["frequency"] >= min_frequency].copy()
if ranking.empty:
    st.info("No entities remain after filters.")
    st.stop()

metric_resolution = resolve_metric_choice(ranking, selected_metric_label)
if metric_resolution.fallback_used and metric_resolution.message:
    st.info(metric_resolution.message)
metric_column = metric_resolution.resolved_column
metric_title = metric_resolution.resolved_label
composite_selected = metric_resolution.requested_label == "Composite Score" and metric_column == "composite_score"

if composite_selected:
    ranking = ranking.sort_values(["rank_position", "entity_id"]).head(top_n)
else:
    ranking = ranking.sort_values([metric_column, "rank_position"], ascending=[False, True]).head(top_n)

entity_title = "Route" if selected_mode == "bus" else "Station"
kpi_a, kpi_b, kpi_c = st.columns(3)
kpi_a.metric(f"{entity_title}s Shown", fmt_int(len(ranking)))
kpi_b.metric("Top Frequency", fmt_int(ranking["frequency"].max()))
kpi_c.metric("Top Composite", fmt_float(ranking["composite_score"].max(), digits=3))

chart = (
    alt.Chart(ranking)
    .mark_bar()
    .encode(
        x=alt.X(f"{metric_column}:Q", title=metric_axis_title(metric_title)),
        y=alt.Y("entity_id:N", sort="-x", title=entity_title),
        tooltip=[
            "entity_id:N",
            "rank_position:Q",
            f"{metric_column}:Q",
            "frequency:Q",
            "severity_p90:Q",
            "regularity_p90:Q",
            "cause_mix_score:Q",
            "composite_score:Q",
        ],
    )
    .properties(
        title=f"Top {selected_mode.title()} {entity_title}s by {metric_title} ({selected_start_iso} to {selected_end_iso})",
        height=360,
    )
)
st.altair_chart(chart, use_container_width=True)

if not presentation:
    st.dataframe(
        ranking[["entity_id", "rank_position", "frequency", "severity_p90", "regularity_p90", "cause_mix_score", "composite_score"]],
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")
st.markdown("#### Spatial Map")

spatial_result = (
    _load_bus_spatial_hotspot_provisional(selected_start_iso, selected_end_iso)
    if selected_mode == "bus"
    else _load_subway_spatial_hotspot()
)

if spatial_result.status in {"missing", "error"}:
    st.info("Spatial context is unavailable in the current environment.")
elif spatial_result.status == "empty" or spatial_result.frame.empty:
    st.info("No mapped hotspots are available for the selected mode.")
else:
    spatial = spatial_result.frame.copy()
    spatial = spatial.dropna(subset=["centroid_lat", "centroid_lon"]).copy()
    if spatial.empty:
        st.info("Mapped centroid coordinates are unavailable for this mode.")
    else:
        visible_entities = set(ranking["entity_id"].astype(str).tolist())
        spatial["spatial_unit_id"] = spatial["spatial_unit_id"].astype(str)
        spatial = spatial[spatial["spatial_unit_id"].isin(visible_entities)].copy()
        if spatial.empty:
            st.info("No mapped hotspots are available for the currently visible Top N entities.")
        else:
            spatial_metric = metric_column if metric_column in spatial.columns else "composite_score"
            spatial = spatial.sort_values(spatial_metric, ascending=False, na_position="last").head(min(top_n, len(spatial)))
            metric_vals = pd.to_numeric(spatial[spatial_metric], errors="coerce").fillna(0.0)
            min_metric = float(metric_vals.min()) if not metric_vals.empty else 0.0
            max_metric = float(metric_vals.max()) if not metric_vals.empty else 0.0
            denom = max(max_metric - min_metric, 1e-9)
            spatial["radius"] = 150 + ((metric_vals - min_metric) / denom) * 700
            spatial["mode_color"] = [[214, 96, 77] if selected_mode == "bus" else [31, 119, 180] for _ in range(len(spatial))]

            view_state = pdk.ViewState(
                latitude=float(spatial["centroid_lat"].mean()),
                longitude=float(spatial["centroid_lon"].mean()),
                zoom=10,
                pitch=0,
            )
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=spatial.rename(columns={"centroid_lat": "lat", "centroid_lon": "lon"}),
                get_position=["lon", "lat"],
                get_fill_color="mode_color",
                get_radius="radius",
                pickable=True,
                auto_highlight=True,
                stroked=True,
                radius_scale=1,
            )
            st.pydeck_chart(
                pdk.Deck(
                    layers=[layer],
                    initial_view_state=view_state,
                    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                    tooltip={
                        "html": (
                            "<b>ID:</b> {spatial_unit_id}<br/>"
                            "<b>Frequency:</b> {frequency}<br/>"
                            "<b>Composite:</b> {composite_score}"
                        ),
                        "style": {"backgroundColor": "#1f2937", "color": "white"},
                    },
                )
            )

            if selected_mode == "bus":
                st.warning("Bus spatial points are provisional route-centroid estimates and should be interpreted directionally.")
            if not presentation:
                st.dataframe(spatial, use_container_width=True, hide_index=True)

next_question_hint("When do these hotspots recur most often? Open: Time Patterns.")
