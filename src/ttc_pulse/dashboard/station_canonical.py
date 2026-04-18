"""Dashboard-facing re-exports for shared station canonicalization helpers."""

from __future__ import annotations

from ttc_pulse.station_canonical import SUBWAY_STATION_CANONICAL_MAP as SUBWAY_STATION_CANONICAL_MAP


def canonicalize_subway_station_name(value: object) -> str:
    from ttc_pulse.station_canonical import canonicalize_subway_station_name as _canonicalize_subway_station_name
    return _canonicalize_subway_station_name(value, output_style="title")


def subway_station_canonical_sql(column_name: str = "station_canonical") -> str:
    from ttc_pulse.station_canonical import subway_station_canonical_sql as _subway_station_canonical_sql
    return _subway_station_canonical_sql(column_name=column_name, output_style="title")
