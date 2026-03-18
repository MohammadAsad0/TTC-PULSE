from __future__ import annotations

from pathlib import Path
import sys

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

from ttc_pulse.dashboard.formatting import fmt_int
from ttc_pulse.dashboard.loaders import query_table, resolve_project_root

METRIC_OPTIONS: dict[str, str] = {
    "Composite Score": "composite_score",
    "Frequency": "frequency",
    "Severity P90": "severity_p90",
    "Regularity P90": "regularity_p90",
}


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
        ORDER BY composite_score DESC NULLS LAST
        """,
    )


@st.cache_data(ttl=120)
def _load_bus_spatial_hotspot_provisional():
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
            WHERE LOWER(mode) = 'bus'
                AND route_id_gtfs IS NOT NULL
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
    )


st.title("Spatial Hotspot Map")
subway_result = _load_subway_spatial_hotspot()
bus_result = _load_bus_spatial_hotspot_provisional()

if subway_result.status in {"missing", "error"} and bus_result.status in {"missing", "error"}:
    st.error(subway_result.message if subway_result.status in {"missing", "error"} else bus_result.message)
    st.stop()

subway_frame = subway_result.frame.copy() if subway_result.status == "ok" else pd.DataFrame()
bus_frame = bus_result.frame.copy() if bus_result.status == "ok" else pd.DataFrame()

frame = pd.concat([subway_frame, bus_frame], ignore_index=True)
frame = frame.dropna(subset=["centroid_lat", "centroid_lon"])
if frame.empty:
    st.info(
        "No spatial hotspots with centroid coordinates are currently available. "
        "Subway hotspots depend on confidence-gated publication; bus hotspots require route centroid derivation."
    )
    st.stop()

mode_options = sorted(frame["mode"].dropna().unique().tolist())
selected_mode = st.selectbox("Mode", options=mode_options, index=0 if mode_options else None)
selected_metric_label = st.selectbox("Metric", options=list(METRIC_OPTIONS.keys()), index=0)
selected_metric_column = METRIC_OPTIONS[selected_metric_label]
top_n = st.slider("Top N Hotspots", min_value=1, max_value=69, value=25, step=1)
search_term = st.text_input("Filter hotspot id contains", value="").strip()

filtered = frame[frame["mode"] == selected_mode].copy()
if search_term:
    filtered = filtered[filtered["spatial_unit_id"].astype(str).str.contains(search_term, case=False, na=False)]
filtered = filtered.sort_values(selected_metric_column, ascending=False, na_position="last").head(top_n)
if filtered.empty:
    st.info("No mapped hotspots for the selected mode.")
    st.stop()

if selected_mode == "bus":
    st.warning(
        "Bus hotspots are provisional route-centroid estimates derived from GTFS route-stop geometry. "
        "Use for directional insight, not precise stop-level localization."
    )

metric_values = pd.to_numeric(filtered[selected_metric_column], errors="coerce").fillna(0.0)
min_metric = float(metric_values.min()) if not metric_values.empty else 0.0
max_metric = float(metric_values.max()) if not metric_values.empty else 0.0
denominator = max(max_metric - min_metric, 1e-9)
filtered["radius"] = 140 + ((metric_values - min_metric) / denominator) * 700
fill_color = [31, 119, 180] if selected_mode == "subway" else [214, 96, 77]
filtered["mode_color"] = [fill_color for _ in range(len(filtered))]

st.metric("Mapped Hotspots", fmt_int(len(filtered)))

view_state = pdk.ViewState(
    latitude=float(filtered["centroid_lat"].mean()),
    longitude=float(filtered["centroid_lon"].mean()),
    zoom=10,
    pitch=0,
)
layer = pdk.Layer(
    "ScatterplotLayer",
    data=filtered.rename(columns={"centroid_lat": "lat", "centroid_lon": "lon"}),
    get_position=["lon", "lat"],
    get_fill_color="mode_color",
    get_radius="radius",
    pickable=True,
    auto_highlight=True,
    stroked=True,
    radius_scale=1,
)
tooltip = {
    "html": (
        "<b>ID:</b> {spatial_unit_id}<br/>"
        "<b>Metric:</b> {" + selected_metric_column + "}<br/>"
        "<b>Frequency:</b> {frequency}<br/>"
        "<b>Severity P90:</b> {severity_p90}<br/>"
        "<b>Regularity P90:</b> {regularity_p90}<br/>"
        "<b>Confidence:</b> {confidence_score}"
    ),
    "style": {"backgroundColor": "steelblue", "color": "white"},
}
st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip=tooltip,
    )
)

table_columns = [
    "mode",
    "spatial_unit_type",
    "spatial_unit_id",
    "frequency",
    "severity_p90",
    "regularity_p90",
    "composite_score",
    "confidence_score",
    "centroid_lat",
    "centroid_lon",
]
st.dataframe(filtered[table_columns], use_container_width=True, hide_index=True)
