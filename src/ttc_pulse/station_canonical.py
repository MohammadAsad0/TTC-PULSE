"""Global station canonicalization helpers shared across pipeline and dashboard."""

from __future__ import annotations

SUBWAY_STATION_CANONICAL_MAP: dict[str, str] = {
    "BLOOR": "Bloor-Yonge Station",
    "YONGE": "Bloor-Yonge Station",
    "BLOOR STATION": "Bloor-Yonge Station",
    "YONGE STATION": "Bloor-Yonge Station",
    "BLOOR-YONGE STATION": "Bloor-Yonge Station",
}


def canonicalize_subway_station_name(value: object, output_style: str = "title") -> str:
    text = str(value).strip()
    if not text:
        return ""
    canonical = SUBWAY_STATION_CANONICAL_MAP.get(text.upper(), text)
    if output_style == "upper":
        return canonical.upper()
    return canonical


def subway_station_canonical_sql(column_name: str = "station_canonical", output_style: str = "title") -> str:
    aliases_sql = ",\n            ".join(f"'{alias}'" for alias in SUBWAY_STATION_CANONICAL_MAP.keys())
    canonical_title = canonicalize_subway_station_name("BLOOR-YONGE STATION", output_style="title")
    canonical_sql = canonical_title.upper() if output_style == "upper" else canonical_title
    else_expr = (
        f"UPPER(TRIM(COALESCE({column_name}, '')))"
        if output_style == "upper"
        else f"TRIM(COALESCE({column_name}, ''))"
    )
    return f"""
    CASE
        WHEN UPPER(TRIM(COALESCE({column_name}, ''))) IN (
            {aliases_sql}
        ) THEN '{canonical_sql}'
        ELSE {else_expr}
    END
    """

