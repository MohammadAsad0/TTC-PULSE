-- TTC Pulse MVP Schema DDL
-- Target: DuckDB + Parquet (MVP), Postgres-compatible naming/types where practical

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ----------------------------
-- RAW LAYER (immutable landing)
-- ----------------------------

CREATE TABLE IF NOT EXISTS raw.raw_bus_delay_events (
    raw_id BIGINT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    payload_json TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_subway_delay_events (
    raw_id BIGINT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    payload_json TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_routes (
    route_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    direction_id INTEGER,
    shape_id TEXT,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat DOUBLE,
    stop_lon DOUBLE,
    parent_station TEXT,
    location_type INTEGER,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_calendar (
    service_id TEXT,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date DATE,
    end_date DATE,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfs_calendar_dates (
    service_id TEXT,
    date DATE,
    exception_type INTEGER,
    source_file TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_gtfsrt_alert_snapshots (
    snapshot_ts TIMESTAMP,
    source_url TEXT,
    feed_header_ts TIMESTAMP,
    raw_bytes BLOB,
    ingested_at TIMESTAMP
);

-- ----------------------------
-- BRONZE LAYER (row-preserving + lineage)
-- ----------------------------

CREATE TABLE IF NOT EXISTS bronze.bronze_bus_events (
    bronze_id BIGINT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    source_schema_version TEXT,
    row_hash TEXT,
    payload_json TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.bronze_subway_events (
    bronze_id BIGINT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    source_schema_version TEXT,
    row_hash TEXT,
    payload_json TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_routes AS SELECT * FROM raw.raw_gtfs_routes WHERE 1=0;
CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_trips AS SELECT * FROM raw.raw_gtfs_trips WHERE 1=0;
CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_stop_times AS SELECT * FROM raw.raw_gtfs_stop_times WHERE 1=0;
CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_stops AS SELECT * FROM raw.raw_gtfs_stops WHERE 1=0;
CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_calendar AS SELECT * FROM raw.raw_gtfs_calendar WHERE 1=0;
CREATE TABLE IF NOT EXISTS bronze.bronze_gtfs_calendar_dates AS SELECT * FROM raw.raw_gtfs_calendar_dates WHERE 1=0;

CREATE TABLE IF NOT EXISTS bronze.bronze_gtfsrt_alert_entities (
    snapshot_ts TIMESTAMP,
    feed_header_ts TIMESTAMP,
    alert_id TEXT,
    cause TEXT,
    effect TEXT,
    header_text TEXT,
    description_text TEXT,
    route_id TEXT,
    stop_id TEXT,
    trip_id TEXT,
    selector_scope TEXT,
    source_url TEXT,
    ingested_at TIMESTAMP
);

-- ----------------------------
-- SILVER LAYER (canonical model)
-- ----------------------------

CREATE TABLE IF NOT EXISTS silver.dim_route_gtfs (
    route_id TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    mode TEXT,
    is_active BOOLEAN,
    valid_from DATE,
    valid_to DATE
);

CREATE TABLE IF NOT EXISTS silver.dim_stop_gtfs (
    stop_id TEXT PRIMARY KEY,
    stop_name TEXT,
    stop_lat DOUBLE,
    stop_lon DOUBLE
);

CREATE TABLE IF NOT EXISTS silver.dim_service_gtfs (
    service_id TEXT PRIMARY KEY,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN,
    start_date DATE,
    end_date DATE
);

CREATE TABLE IF NOT EXISTS silver.dim_route_alias (
    alias_id BIGINT,
    mode TEXT,
    route_token_raw TEXT,
    route_short_name_norm TEXT,
    route_id_gtfs TEXT,
    mapping_method TEXT,
    mapping_confidence DOUBLE,
    is_active BOOLEAN,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_station_alias (
    alias_id BIGINT,
    station_raw TEXT,
    station_canonical TEXT,
    station_group TEXT,
    stop_id_gtfs_candidate TEXT,
    mapping_method TEXT,
    mapping_confidence DOUBLE,
    is_active BOOLEAN,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.dim_incident_code (
    code_dim_id BIGINT,
    mode TEXT,
    incident_code_raw TEXT,
    incident_text_raw TEXT,
    incident_category TEXT,
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS silver.bridge_route_direction_stop (
    route_id TEXT,
    direction_id INTEGER,
    stop_id TEXT,
    min_stop_sequence INTEGER,
    max_stop_sequence INTEGER,
    trip_count_serving_stop BIGINT
);

CREATE TABLE IF NOT EXISTS silver.route_alias_review (
    review_id BIGINT,
    mode TEXT,
    route_token_raw TEXT,
    proposed_route_id_gtfs TEXT,
    reason TEXT,
    review_status TEXT,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.station_alias_review (
    review_id BIGINT,
    station_raw TEXT,
    proposed_station_canonical TEXT,
    proposed_stop_id_gtfs TEXT,
    reason TEXT,
    review_status TEXT,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.incident_code_review (
    review_id BIGINT,
    mode TEXT,
    incident_code_raw TEXT,
    incident_text_raw TEXT,
    proposed_incident_category TEXT,
    reason TEXT,
    review_status TEXT,
    reviewed_by TEXT,
    reviewed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.silver_bus_events (
    event_id BIGINT,
    service_date DATE,
    event_ts TIMESTAMP,
    day_name TEXT,
    hour_bin INTEGER,
    month_bin DATE,
    route_label_raw TEXT,
    route_short_name_norm TEXT,
    route_id_gtfs TEXT,
    location_text_raw TEXT,
    station_text_raw TEXT,
    incident_text_raw TEXT,
    incident_code_raw TEXT,
    incident_category TEXT,
    min_delay DOUBLE,
    min_gap DOUBLE,
    direction_raw TEXT,
    direction_norm TEXT,
    vehicle_id_raw TEXT,
    match_method TEXT,
    match_confidence DOUBLE,
    link_status TEXT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS silver.silver_subway_events (
    event_id BIGINT,
    service_date DATE,
    event_ts TIMESTAMP,
    day_name TEXT,
    hour_bin INTEGER,
    month_bin DATE,
    line_code_raw TEXT,
    line_code_norm TEXT,
    route_id_gtfs TEXT,
    station_text_raw TEXT,
    station_canonical TEXT,
    location_text_raw TEXT,
    incident_text_raw TEXT,
    incident_code_raw TEXT,
    incident_category TEXT,
    min_delay DOUBLE,
    min_gap DOUBLE,
    direction_raw TEXT,
    direction_norm TEXT,
    vehicle_id_raw TEXT,
    match_method TEXT,
    match_confidence DOUBLE,
    link_status TEXT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    row_hash TEXT
);

CREATE TABLE IF NOT EXISTS silver.silver_gtfsrt_alert_entities (
    snapshot_ts TIMESTAMP,
    feed_ts TIMESTAMP,
    alert_id TEXT,
    cause TEXT,
    effect TEXT,
    header_text TEXT,
    description_text TEXT,
    route_id_gtfs TEXT,
    stop_id_gtfs TEXT,
    trip_id_gtfs TEXT,
    selector_scope TEXT,
    match_status TEXT,
    match_notes TEXT
);

CREATE TABLE IF NOT EXISTS silver.fact_delay_events_norm (
    event_id BIGINT PRIMARY KEY,
    mode TEXT,
    service_date DATE,
    event_ts TIMESTAMP,
    day_name TEXT,
    hour_bin INTEGER,
    month_bin DATE,
    route_label_raw TEXT,
    route_short_name_norm TEXT,
    route_id_gtfs TEXT,
    line_code_raw TEXT,
    line_code_norm TEXT,
    location_text_raw TEXT,
    station_text_raw TEXT,
    station_canonical TEXT,
    incident_text_raw TEXT,
    incident_code_raw TEXT,
    incident_category TEXT,
    min_delay DOUBLE,
    min_gap DOUBLE,
    direction_raw TEXT,
    direction_norm TEXT,
    vehicle_id_raw TEXT,
    match_method TEXT,
    match_confidence DOUBLE,
    link_status TEXT,
    source_mode TEXT,
    source_file TEXT,
    source_sheet TEXT,
    source_row_id BIGINT,
    row_hash TEXT,
    ingested_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS silver.fact_gtfsrt_alerts_norm (
    alert_event_id BIGINT PRIMARY KEY,
    snapshot_ts TIMESTAMP,
    feed_ts TIMESTAMP,
    alert_id TEXT,
    active_start_ts TIMESTAMP,
    active_end_ts TIMESTAMP,
    cause TEXT,
    effect TEXT,
    header_text TEXT,
    description_text TEXT,
    route_id_gtfs TEXT,
    stop_id_gtfs TEXT,
    trip_id_gtfs TEXT,
    selector_scope TEXT,
    match_status TEXT,
    match_notes TEXT,
    ingested_at TIMESTAMP
);

-- ----------------------------
-- GOLD LAYER (stakeholder marts)
-- ----------------------------

CREATE TABLE IF NOT EXISTS gold.gold_delay_events_core (
    mode TEXT,
    service_date DATE,
    hour_bin INTEGER,
    route_id_gtfs TEXT,
    station_canonical TEXT,
    incident_category TEXT,
    event_count BIGINT,
    min_delay_p50 DOUBLE,
    min_delay_p90 DOUBLE,
    min_gap_p90 DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.gold_linkage_quality (
    mode TEXT,
    period_start DATE,
    period_end DATE,
    match_method TEXT,
    link_status TEXT,
    row_count BIGINT,
    pct_of_mode_rows DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.gold_route_time_metrics (
    mode TEXT,
    route_id_gtfs TEXT,
    service_date DATE,
    hour_bin INTEGER,
    frequency BIGINT,
    severity_median DOUBLE,
    severity_p90 DOUBLE,
    regularity_p90 DOUBLE,
    cause_mix_score DOUBLE,
    composite_score DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.gold_station_time_metrics (
    line_code_norm TEXT,
    station_canonical TEXT,
    service_date DATE,
    hour_bin INTEGER,
    frequency BIGINT,
    severity_median DOUBLE,
    severity_p90 DOUBLE,
    regularity_p90 DOUBLE,
    cause_mix_score DOUBLE,
    composite_score DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.gold_time_reliability (
    mode TEXT,
    day_name TEXT,
    hour_bin INTEGER,
    frequency BIGINT,
    severity_p90 DOUBLE,
    regularity_p90 DOUBLE,
    composite_score DOUBLE
);

CREATE TABLE IF NOT EXISTS gold.gold_top_offender_ranking (
    ranking_date DATE,
    mode TEXT,
    entity_type TEXT,
    entity_id TEXT,
    frequency BIGINT,
    severity_p90 DOUBLE,
    regularity_p90 DOUBLE,
    cause_mix_score DOUBLE,
    composite_score DOUBLE,
    rank_position INTEGER
);

CREATE TABLE IF NOT EXISTS gold.gold_alert_validation (
    snapshot_ts TIMESTAMP,
    alert_id TEXT,
    route_id_gtfs TEXT,
    stop_id_gtfs TEXT,
    selector_scope TEXT,
    match_status TEXT,
    header_text TEXT,
    description_text TEXT
);

CREATE TABLE IF NOT EXISTS gold.gold_spatial_hotspot (
    mode TEXT,
    spatial_unit_type TEXT,
    spatial_unit_id TEXT,
    centroid_lat DOUBLE,
    centroid_lon DOUBLE,
    frequency BIGINT,
    severity_p90 DOUBLE,
    regularity_p90 DOUBLE,
    composite_score DOUBLE,
    confidence_score DOUBLE
);

