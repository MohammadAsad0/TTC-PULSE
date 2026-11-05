"""Export the gold marts to small JSON files for the TTC Pulse dashboard on iampatelom.com.

Usage: python scripts/export_site_data.py <output_dir> [<entity_dir>]
Run it from the repository root. It reads the committed Parquet files only.
With <entity_dir>, it also writes one drill-down file per route, station and line,
and drill-index.json in <output_dir>.
"""

from __future__ import annotations

import json
import os
import re
import string
import subprocess
import sys

import duckdb

YEARS = list(range(2014, 2026))
DRILL_YEARS = list(range(2014, 2027))  # 2026 has January only
MODES = ("bus", "streetcar", "subway")
TOP_ENTITIES = 40
DRILL_MIN_EVENTS = 20
DRILL_TOP_CAUSES = 5
CODE_PATTERN = "^[A-Z]{3,6}$"  # newer rows carry a raw incident code, not a name


def gold(table: str) -> str:
    return f"read_parquet('gold/{table}.parquet')"


def entity_column(mode: str) -> str:
    return "station_canonical" if mode == "subway" else "route_id_gtfs"


def clean(text: str) -> str:
    """Replace dash punctuation in TTC text so the site copy has none."""
    return re.sub(r"\s*[\u2013\u2014]\s*", ", ", text or "").strip(" ,")


def write(out_dir: str, name: str, payload: object) -> None:
    path = os.path.join(out_dir, name)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), ensure_ascii=False)
    print(f"{name:18s} {os.path.getsize(path):8d} bytes")


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# Drill-down scopes: (mode, scope, key column in the core mart, metric mart, key column in the metric mart).
DRILL_SCOPES = (
    ("bus", "route", "route_id_gtfs", "gold_route_time_metrics", "route_id_gtfs"),
    ("streetcar", "route", "route_id_gtfs", "gold_route_time_metrics", "route_id_gtfs"),
    ("subway", "station", "station_canonical", "gold_station_time_metrics", "station_canonical"),
    ("subway", "line", "route_id_gtfs", "gold_route_time_metrics", "route_id_gtfs"),
)

# Silver rows with the same key, date and delay rules as the delay_events_src view in
# src/ttc_pulse/marts/_gold_utils.py. The core mart groups this view, so the counts agree.
SILVER_EVENTS = """(
    SELECT
        COALESCE(NULLIF(TRIM(mode), ''), NULLIF(TRIM(source_mode), ''), 'unknown') AS mode,
        year(COALESCE(service_date, CAST(event_ts AS DATE))) AS year,
        COALESCE(NULLIF(TRIM(route_id_gtfs), ''), NULLIF(TRIM(route_short_name_norm), ''), NULLIF(TRIM(line_code_norm), '')) AS route_id_gtfs,
        COALESCE(NULLIF(TRIM(station_canonical), ''), NULLIF(TRIM(station_text_raw), ''), NULLIF(TRIM(location_text_raw), '')) AS station_canonical,
        CASE
            WHEN min_delay IS NULL THEN NULL
            WHEN lower(COALESCE(NULLIF(TRIM(mode), ''), NULLIF(TRIM(source_mode), ''), 'unknown')) = 'bus' AND min_delay >= 999 THEN NULL
            WHEN min_delay < 0 THEN 0
            ELSE min_delay
        END AS delay
    FROM read_parquet('silver/fact_delay_events_norm.parquet')
)"""


def export_drill(rows, label, totals: dict, commit: str, out_dir: str, entity_dir: str) -> None:
    """Write drill-index.json and one file per entity with at least DRILL_MIN_EVENTS delay events.

    Metric windows:
    - Frequency, Regularity, Severity, Cause Mix and Composite Score use the full window,
      2014-01-01 to 2026-01-31. This is the default date window of the Streamlit drill-down page.
      The definitions are the ones in src/ttc_pulse/dashboard/metric_config.py, over the hourly
      rows of the gold metric marts: Frequency is the sum of events. Severity and Regularity are
      the p90 of the hourly p90 delay and gap minutes. Cause Mix is the mean hourly cause mix.
      Composite is 0.35 z(F) + 0.30 z(Sev) + 0.20 z(Reg) + 0.15 C, with z over all entities of
      the same mode and scope.
    - The Hotspots ranking on the site uses gold_top_offender_ranking, which is the last 90 days.
      So its scores differ from these scores.
    - The entity files hold event counts from gold_delay_events_core by year, month, weekday
      and hour, and delay minutes (median and p90 of each event) from the silver rows by year.
    """
    os.makedirs(entity_dir, exist_ok=True)
    for name in os.listdir(entity_dir):
        if name.endswith(".json"):
            os.remove(os.path.join(entity_dir, name))
    core = gold("gold_delay_events_core")
    index, sizes, files = [], [], set()
    for mode, scope, key_col, mart, mart_col in DRILL_SCOPES:
        metrics = {
            key: (f, sev, reg, mix, score)
            for key, f, sev, reg, mix, score in rows(
                f"""
                WITH entity_agg AS (
                    SELECT {mart_col} AS entity_id,
                        SUM(frequency)::DOUBLE AS frequency,
                        quantile_cont(severity_p90, 0.9) FILTER (WHERE severity_p90 IS NOT NULL) AS severity_p90,
                        quantile_cont(regularity_p90, 0.9) FILTER (WHERE regularity_p90 IS NOT NULL) AS regularity_p90,
                        AVG(cause_mix_score) FILTER (WHERE cause_mix_score IS NOT NULL) AS cause_mix_score
                    FROM {gold(mart)}
                    WHERE {mart_col} IS NOT NULL AND {mart_col} <> '' AND service_date IS NOT NULL
                        {"AND mode = ?" if mart == "gold_route_time_metrics" else ""}
                    GROUP BY 1
                )
                SELECT entity_id, frequency, severity_p90, regularity_p90, COALESCE(cause_mix_score, 0.0),
                    0.35 * COALESCE((frequency - AVG(frequency) OVER ()) / NULLIF(STDDEV_SAMP(frequency) OVER (), 0.0), 0.0)
                    + 0.30 * COALESCE((severity_p90 - AVG(severity_p90) OVER ()) / NULLIF(STDDEV_SAMP(severity_p90) OVER (), 0.0), 0.0)
                    + 0.20 * COALESCE((regularity_p90 - AVG(regularity_p90) OVER ()) / NULLIF(STDDEV_SAMP(regularity_p90) OVER (), 0.0), 0.0)
                    + 0.15 * COALESCE(cause_mix_score, 0.0)
                FROM entity_agg
                """,
                *([mode] if mart == "gold_route_time_metrics" else []),
            )
        }
        counts = rows(f"SELECT {key_col}, sum(event_count) n FROM {core} WHERE mode = ? AND {key_col} IS NOT NULL GROUP BY 1 ORDER BY n DESC", mode)
        null_events = rows(f"SELECT coalesce(sum(event_count), 0) FROM {core} WHERE mode = ? AND {key_col} IS NULL", mode)[0][0]
        # Every event is on one entity or has no key. So the entity counts plus the NULL keys give the mode total.
        assert sum(n for _, n in counts) + null_events == totals[mode], (mode, scope)
        for key, n in counts:
            assert int(metrics[key][0]) == n, (mode, scope, key)  # the metric mart and the core mart agree
        keep = {key: int(n) for key, n in counts if n >= DRILL_MIN_EVENTS}

        ny = len(DRILL_YEARS)
        cube = {key: {"years": [0] * ny, "months": [[0] * 12 for _ in range(ny)], "days": [[0] * 84 for _ in range(ny)], "grid": [[0] * 168 for _ in range(ny)]} for key in keep}
        for key, y, m, d, h, c in rows(
            f"""SELECT {key_col}, year(service_date), month(service_date), isodow(service_date) - 1, hour_bin, sum(event_count)
                FROM {core} WHERE mode = ? AND {key_col} IS NOT NULL GROUP BY ALL""",
            mode,
        ):
            if key not in cube:
                continue
            e, yi, c = cube[key], DRILL_YEARS.index(y), int(c)
            e["years"][yi] += c
            e["months"][yi][m - 1] += c
            e["days"][yi][(m - 1) * 7 + d] += c
            e["grid"][yi][d * 24 + h] += c

        cause_rows = {}
        for key, y, cause, c in rows(
            f"SELECT {key_col}, year(service_date), incident_category, sum(event_count) n FROM {core} WHERE mode = ? AND {key_col} IS NOT NULL GROUP BY ALL ORDER BY n DESC, 3",
            mode,
        ):
            if key in keep:
                cause_rows.setdefault(key, []).append((DRILL_YEARS.index(y), cause or "Uncategorized", int(c)))

        delay = {}
        for key, y, p50, p90 in rows(
            f"SELECT {key_col}, year, quantile_cont(delay, 0.5), quantile_cont(delay, 0.9) FROM {SILVER_EVENTS} WHERE mode = ? AND {key_col} IS NOT NULL GROUP BY ALL",
            mode,
        ):
            delay[(key, y)] = None if p50 is None else [round(p50, 1), round(p90, 1)]
        delay_all = {
            key: None if p50 is None else [round(p50, 1), round(p90, 1)]
            for key, p50, p90 in rows(f"SELECT {key_col}, quantile_cont(delay, 0.5), quantile_cont(delay, 0.9) FROM {SILVER_EVENTS} WHERE mode = ? AND {key_col} IS NOT NULL GROUP BY 1", mode)
        }

        for key, n in keep.items():
            e = cube[key]
            assert sum(e["years"]) == n, (mode, scope, key)
            names: list[str] = []

            def pos(cause: str) -> int:
                if cause not in names:
                    names.append(cause)
                return names.index(cause)

            totals_by_cause: dict[str, int] = {}
            by_year = []
            for yi in range(ny):
                year_rows = [(cause, c) for i, cause, c in cause_rows[key] if i == yi]
                top = year_rows[:DRILL_TOP_CAUSES]
                by_year.append([[[pos(cause), c] for cause, c in top], e["years"][yi] - sum(c for _, c in top)])
                for cause, c in year_rows:
                    totals_by_cause[cause] = totals_by_cause.get(cause, 0) + c
            top_all = sorted(totals_by_cause.items(), key=lambda kv: (-kv[1], kv[0]))[: DRILL_TOP_CAUSES + 1]
            entity_id = f"line-{key}" if scope == "line" else key
            name = label(mode, key) if scope != "line" else label("bus", key)
            file = f"{mode}-{slug(entity_id)}"
            assert file not in files, file
            files.add(file)
            payload = {
                "id": entity_id,
                "name": name,
                "mode": mode,
                "scope": scope,
                "n": n,
                "y0": DRILL_YEARS[0],
                "years": e["years"],
                "months": e["months"],
                "days": e["days"],  # per year: month * 7 + weekday, Monday = 0
                "grid": e["grid"],  # per year: weekday * 24 + hour
                "causes": {
                    "names": names,
                    "all": [[[pos(cause), c] for cause, c in top_all], n - sum(c for _, c in top_all)],
                    "byYear": by_year,  # per year: top causes as [name index, events], then the rest
                },
                "delay": [delay.get((key, y)) for y in DRILL_YEARS],  # per year: [median, p90] minutes, or null
                "delayAll": delay_all.get(key),
            }
            path = os.path.join(entity_dir, f"{file}.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"), ensure_ascii=False)
            sizes.append((os.path.getsize(path), file))
            f, sev, reg, mix, score = metrics[key]
            index.append({
                "id": entity_id, "file": file, "name": name, "mode": mode, "scope": scope, "n": n,
                "sev": None if sev is None else round(sev, 1), "reg": None if reg is None else round(reg, 1),
                "mix": round(mix, 3), "score": round(score, 3),
            })
        print(f"drill {mode:9s} {scope:7s} {len(keep):4d} of {len(counts):4d} entities, {sum(keep.values()):7d} of {totals[mode]:7d} events, no key {null_events}")
    write(out_dir, "drill-index.json", {"meta": {"commit": commit, "y0": DRILL_YEARS[0], "minEvents": DRILL_MIN_EVENTS}, "entities": index})
    sizes.sort(reverse=True)
    print(f"entity files       {len(sizes):8d} files, {sum(s for s, _ in sizes):8d} bytes, largest {sizes[0][0]} bytes ({sizes[0][1]})")
    if sizes[0][0] > 20_000:
        print("warning: an entity file is larger than 20 KB")


def main(out_dir: str, entity_dir: str | None = None) -> None:
    os.makedirs(out_dir, exist_ok=True)
    con = duckdb.connect()
    rows = lambda sql, *p: con.execute(sql, list(p)).fetchall()
    commit = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True).stdout.strip()
    names = dict(rows("SELECT route_id, route_long_name FROM read_parquet('dimensions/dim_route_gtfs.parquet')"))
    core = gold("gold_delay_events_core")

    def label(mode: str, key: str) -> str:
        return string.capwords(key.removesuffix(" STATION").lower()) if mode == "subway" else names.get(key, "")

    overview, entities, scores, time_view, causes_view = {}, {}, {}, {}, {}
    for mode in MODES:
        ent = entity_column(mode)
        total = rows(f"SELECT sum(event_count) FROM {core} WHERE mode = ?", mode)[0][0]
        grid = [[0] * 24 for _ in range(7)]
        for dow, hour, n in rows(f"SELECT isodow(service_date) - 1, hour_bin, sum(event_count) FROM {core} WHERE mode = ? GROUP BY 1, 2", mode):
            grid[dow][hour] = int(n)
        rush = sum(grid[d][h] for d in range(5) for h in list(range(6, 10)) + list(range(15, 19))) / total
        by_entity = rows(f"SELECT {ent}, sum(event_count) n FROM {core} WHERE mode = ? AND {ent} IS NOT NULL GROUP BY 1 ORDER BY n DESC", mode)
        k10 = -(-len(by_entity) // 10)
        share10 = sum(n for _, n in by_entity[:k10]) / sum(n for _, n in by_entity)
        cause_rows = rows(f"SELECT incident_category, sum(event_count) n FROM {core} WHERE mode = ? GROUP BY 1 ORDER BY n DESC", mode)
        year_counts = dict(rows(f"SELECT year(service_date), sum(event_count) FROM {core} WHERE mode = ? GROUP BY 1", mode))
        matched = dict(rows(f"SELECT link_status, sum(row_count) FROM {gold('gold_linkage_quality')} WHERE mode = ? GROUP BY 1", mode))
        overview[mode] = {
            "total": int(total),
            "entityCount": len(by_entity),
            "top10Count": k10,
            "top10Share": round(share10 * 100, 1),
            "rushShare": round(rush * 100, 1),
            "matchedShare": round(100 * matched.get("matched", 0) / sum(matched.values()), 1),
            "grid": grid,
            "causes": [[c, int(n)] for c, n in cause_rows[:6]] + [["Other", int(total - sum(n for _, n in cause_rows[:6]))]],
            "years": [int(year_counts.get(y, 0)) for y in YEARS],
            "jan2026": int(year_counts.get(2026, 0)),
        }

        detail = []
        for key, n in by_entity[:TOP_ENTITIES]:
            hours, days, years, months = [0] * 24, [0] * 7, [0] * len(YEARS), [0] * 12
            for h, dow, y, m, c in rows(
                f"SELECT hour_bin, isodow(service_date) - 1, year(service_date), month(service_date), sum(event_count) FROM {core} WHERE mode = ? AND {ent} = ? GROUP BY 1, 2, 3, 4",
                mode, key,
            ):
                hours[h] += int(c)
                days[dow] += int(c)
                months[m - 1] += int(c)
                if y in YEARS:
                    years[YEARS.index(y)] += int(c)
            top_causes = rows(f"SELECT incident_category, sum(event_count) x FROM {core} WHERE mode = ? AND {ent} = ? GROUP BY 1 ORDER BY x DESC LIMIT 5", mode, key)
            detail.append({"id": key, "name": label(mode, key), "gtfs": mode == "subway" or key in names, "events": int(n),
                           "hours": hours, "days": days, "years": years, "months": months, "causes": [[a, int(b)] for a, b in top_causes]})
        entities[mode] = detail

        entity_type = "station" if mode == "subway" else "route"
        ranked = rows(f"SELECT entity_id, frequency, composite_score, severity_p90 FROM {gold('gold_top_offender_ranking')} WHERE mode = ? AND entity_type = ?", mode, entity_type)
        k = sorted(r[1] for r in ranked)[len(ranked) // 2]  # median event count: the weight of the mean
        scores[mode] = {"k": k, "points": [[key, label(mode, key), int(n), round(s, 3), round(s * n / (n + k), 3), round(sev or 0, 1)] for key, n, s, sev in ranked]}

        month_year = [[0] * 12 for _ in YEARS]
        for y, m, c in rows(f"SELECT year(service_date), month(service_date), sum(event_count) FROM {core} WHERE mode = ? GROUP BY 1, 2", mode):
            if y in YEARS:
                month_year[YEARS.index(y)][m - 1] = int(c)
        time_view[mode] = {"monthYear": month_year}

        named = [c for c, _ in cause_rows if c != "Other" and not re.match(CODE_PATTERN, c or "")][:8]
        cy = {c: [0] * len(YEARS) for c in named}
        for c, y, n in rows(f"SELECT incident_category, year(service_date), sum(event_count) FROM {core} WHERE mode = ? GROUP BY 1, 2", mode):
            if c in cy and y in YEARS:
                cy[c][YEARS.index(y)] = int(n)
        code_share = rows(f"SELECT year(service_date), sum(event_count) FILTER (WHERE regexp_matches(incident_category, '{CODE_PATTERN}')) / sum(event_count) FROM {core} WHERE mode = ? GROUP BY 1 ORDER BY 1", mode)
        causes_view[mode] = {"byYear": [[c, cy[c]] for c in named], "codeShare": [[int(y), round(100 * (s or 0), 1)] for y, s in code_share]}

    stations = [{"station": string.capwords(s.removesuffix(" STATION").lower()), "lat": round(lat, 5), "lon": round(lon, 5), "events": int(f), "severity": round(p90 or 0, 1)}
                for s, lat, lon, f, p90 in rows(f"SELECT spatial_unit_id, centroid_lat, centroid_lon, frequency, severity_p90 FROM {gold('gold_spatial_hotspot')} WHERE centroid_lat IS NOT NULL")]

    fact = "read_parquet('silver/fact_delay_events_norm.parquet')"
    marts = []
    for name in sorted(f[:-8] for f in os.listdir("gold") if f.endswith(".parquet")):
        marts.append({"name": name, "rows": int(rows(f"SELECT count(*) FROM {gold(name)}")[0][0]), "bytes": os.path.getsize(f"gold/{name}.parquet")})
    quality = {
        "linkage": [[m, s, int(n)] for m, s, n in rows(f"SELECT mode, link_status, sum(row_count) FROM {gold('gold_linkage_quality')} GROUP BY 1, 2 ORDER BY 1, 2")],
        "matchMethod": [[m, int(n)] for m, n in rows(f"SELECT match_method, count(*) FROM {fact} GROUP BY 1 ORDER BY 2 DESC")],
        "unmatchedBus": [[r, int(n)] for r, n in rows(f"SELECT route_label_raw, count(*) n FROM {fact} WHERE mode = 'bus' AND link_status <> 'matched' GROUP BY 1 ORDER BY n DESC LIMIT 10")],
        "lineage": int(rows(f"SELECT count(*) FROM {fact} WHERE source_file IS NOT NULL AND source_row_id IS NOT NULL AND row_hash IS NOT NULL")[0][0]),
        "events": int(rows(f"SELECT count(*) FROM {fact}")[0][0]),
        "marts": marts,
    }

    alert_rows = rows(f"SELECT alert_id, any_value(header_text), any_value(selector_scope), count(*), avg(CASE WHEN match_status = 'matched' THEN 1 ELSE 0 END) FROM {gold('gold_alert_validation')} GROUP BY 1 ORDER BY 4 DESC")
    alerts = {
        "builtAt": str(rows(f"SELECT max(snapshot_ts) FROM {gold('gold_alert_validation')}")[0][0])[:10],
        "alerts": len(alert_rows),
        "rows": int(sum(r[3] for r in alert_rows)),
        "matchedShare": round(100 * rows(f"SELECT avg(CASE WHEN match_status = 'matched' THEN 1 ELSE 0 END) FROM {gold('gold_alert_validation')}")[0][0], 1),
        "list": [{"id": a, "header": clean(h), "scope": s, "rows": int(n), "matched": round(100 * m, 1)} for a, h, s, n, m in alert_rows[:15]],
    }

    sample = {}
    for mode in MODES:
        sample[mode] = [
            {"date": str(d), "hour": int(h), "where": w or "", "cause": c or "", "delay": dl, "gap": g, "file": f, "row": int(r)}
            for d, h, w, c, dl, g, f, r in rows(
                f"""SELECT service_date, hour_bin, coalesce(route_id_gtfs || ' ' || coalesce(location_text_raw, ''), station_canonical, ''), incident_category,
                       min_delay, min_gap, source_file, source_row_id
                FROM {fact} WHERE mode = ? ORDER BY service_date DESC, event_ts DESC NULLS LAST, source_row_id LIMIT 25""",
                mode,
            )
        ]

    meta = {"commit": commit, "years": YEARS, "source": "TTC Pulse gold marts, exported by scripts/export_site_data.py"}
    write(out_dir, "overview.json", {"meta": meta, "modes": overview})
    write(out_dir, "entities.json", entities)
    write(out_dir, "scores.json", scores)
    write(out_dir, "time.json", time_view)
    write(out_dir, "causes.json", causes_view)
    write(out_dir, "stations.json", stations)
    write(out_dir, "quality.json", quality)
    write(out_dir, "alerts.json", alerts)
    write(out_dir, "sample.json", sample)

    if entity_dir:
        export_drill(rows, label, {mode: overview[mode]["total"] for mode in MODES}, commit, out_dir, entity_dir)


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit("Usage: python scripts/export_site_data.py <output_dir> [<entity_dir>]")
    main(*sys.argv[1:])
