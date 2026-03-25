"""Parse local GTFS-RT Service Alert snapshots into structured rows."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ttc_pulse.alerts._sidecar_log import append_alert_sidecar_log_row
from ttc_pulse.utils.project_setup import append_csv_rows, resolve_project_paths

try:  # pragma: no cover - optional dependency
    from google.transit import gtfs_realtime_pb2  # type: ignore

    PROTOBUF_DECODE_AVAILABLE = True
    PROTOBUF_IMPORT_ERROR = ""
except Exception as exc:  # pragma: no cover - environment dependent
    gtfs_realtime_pb2 = None  # type: ignore[assignment]
    PROTOBUF_DECODE_AVAILABLE = False
    PROTOBUF_IMPORT_ERROR = f"{type(exc).__name__}: {exc}"

SNAPSHOT_EXTENSIONS = {".pb", ".bin"}
SNAPSHOT_STAMP_PATTERN = re.compile(r"(\d{8}T\d{6}Z)")

FALLBACK_CAVEAT = (
    "Protobuf decoder unavailable; parser emitted fallback binary metadata rows only."
)

PARSED_COLUMNS = [
    "snapshot_file",
    "snapshot_path",
    "snapshot_rel_path",
    "snapshot_ts_utc",
    "feed_timestamp_utc",
    "parse_mode",
    "parse_caveat",
    "entity_index",
    "entity_id",
    "alert_id",
    "cause",
    "effect",
    "header_text",
    "description_text",
    "active_period_count",
    "active_start_utc",
    "active_end_utc",
    "informed_entity_index",
    "agency_id",
    "route_id",
    "route_type",
    "stop_id",
    "trip_id",
    "trip_route_id",
    "trip_start_date",
    "trip_start_time",
    "direction_id",
    "binary_prefix_hex",
    "source_bytes",
    "source_sha256",
]

PARSE_MANIFEST_COLUMNS = [
    "parsed_at",
    "snapshot_file",
    "snapshot_path",
    "snapshot_rel_path",
    "snapshot_ts_utc",
    "feed_timestamp_utc",
    "parse_mode",
    "status",
    "rows_emitted",
    "feed_entities",
    "alert_entities",
    "parse_caveat",
    "source_bytes",
    "source_sha256",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_project_root() -> Path:
    return resolve_project_paths().project_root


def _workspace_root(project_root: Path) -> Path:
    return project_root.parent


def _relative_posix(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch_to_iso(epoch_seconds: int | None) -> str:
    if epoch_seconds is None:
        return ""
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_snapshot_ts_from_name(snapshot_path: Path) -> str:
    match = SNAPSHOT_STAMP_PATTERN.search(snapshot_path.name)
    if not match:
        return ""
    try:
        parsed = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return ""
    return _utc_iso(parsed)


def _render_translated_text(translated: Any) -> str:
    translations = getattr(translated, "translation", None)
    if not translations:
        return ""
    chunks: list[str] = []
    for translation in translations:
        text_value = str(getattr(translation, "text", "") or "").strip()
        if text_value:
            chunks.append(text_value)
    return " | ".join(chunks)


def _enum_name(enum_container: Any, value: int, fallback: str) -> str:
    try:
        return enum_container.Name(value)
    except Exception:
        return fallback


def discover_local_alert_snapshots(include_eda_snapshots: bool = True) -> list[Path]:
    """Discover candidate local snapshots for parsing."""
    project_root = _resolve_project_root()
    roots = [project_root / "alerts" / "raw_snapshots"]
    if include_eda_snapshots:
        roots.append(_workspace_root(project_root) / "eda" / "gtfsrt_eda_outputs" / "raw")

    discovered: dict[str, Path] = {}
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in SNAPSHOT_EXTENSIONS:
                continue
            resolved = path.resolve()
            discovered[resolved.as_posix()] = resolved
    return [discovered[key] for key in sorted(discovered)]


def _build_fallback_rows(snapshot_path: Path, caveat: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = snapshot_path.read_bytes()
    checksum = _file_checksum(snapshot_path)
    row = {
        "snapshot_file": snapshot_path.name,
        "snapshot_path": snapshot_path.as_posix(),
        "snapshot_rel_path": "",
        "snapshot_ts_utc": _extract_snapshot_ts_from_name(snapshot_path),
        "feed_timestamp_utc": "",
        "parse_mode": "fallback_binary_metadata",
        "parse_caveat": caveat,
        "entity_index": "",
        "entity_id": "",
        "alert_id": "",
        "cause": "",
        "effect": "",
        "header_text": "",
        "description_text": "",
        "active_period_count": 0,
        "active_start_utc": "",
        "active_end_utc": "",
        "informed_entity_index": "",
        "agency_id": "",
        "route_id": "",
        "route_type": "",
        "stop_id": "",
        "trip_id": "",
        "trip_route_id": "",
        "trip_start_date": "",
        "trip_start_time": "",
        "direction_id": "",
        "binary_prefix_hex": payload[:16].hex(),
        "source_bytes": len(payload),
        "source_sha256": checksum,
    }
    return [row], {
        "parse_mode": "fallback_binary_metadata",
        "status": "ok_fallback",
        "rows_emitted": 1,
        "feed_entities": 0,
        "alert_entities": 0,
        "feed_timestamp_utc": "",
        "parse_caveat": caveat,
        "source_bytes": len(payload),
        "source_sha256": checksum,
    }


def _parse_snapshot_with_protobuf(snapshot_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    assert gtfs_realtime_pb2 is not None
    payload = snapshot_path.read_bytes()
    checksum = _file_checksum(snapshot_path)

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    feed_timestamp_utc = ""
    if feed.header.HasField("timestamp"):
        feed_timestamp_utc = _epoch_to_iso(int(feed.header.timestamp))

    rows: list[dict[str, Any]] = []
    alert_entities = 0
    for entity_index, entity in enumerate(feed.entity):
        if not entity.HasField("alert"):
            continue
        alert_entities += 1
        alert = entity.alert
        entity_id = str(entity.id or "")
        alert_id = entity_id
        cause = "UNKNOWN_CAUSE"
        if alert.HasField("cause"):
            cause = _enum_name(gtfs_realtime_pb2.Alert.Cause, int(alert.cause), f"CAUSE_{int(alert.cause)}")

        effect = "UNKNOWN_EFFECT"
        if alert.HasField("effect"):
            effect = _enum_name(gtfs_realtime_pb2.Alert.Effect, int(alert.effect), f"EFFECT_{int(alert.effect)}")

        active_start_utc = ""
        active_end_utc = ""
        active_period_count = len(alert.active_period)
        if alert.active_period:
            first_period = alert.active_period[0]
            if first_period.HasField("start"):
                active_start_utc = _epoch_to_iso(int(first_period.start))
            if first_period.HasField("end"):
                active_end_utc = _epoch_to_iso(int(first_period.end))

        base = {
            "snapshot_file": snapshot_path.name,
            "snapshot_path": snapshot_path.as_posix(),
            "snapshot_rel_path": "",
            "snapshot_ts_utc": _extract_snapshot_ts_from_name(snapshot_path),
            "feed_timestamp_utc": feed_timestamp_utc,
            "parse_mode": "protobuf",
            "parse_caveat": "",
            "entity_index": entity_index,
            "entity_id": entity_id,
            "alert_id": alert_id,
            "cause": cause,
            "effect": effect,
            "header_text": _render_translated_text(alert.header_text),
            "description_text": _render_translated_text(alert.description_text),
            "active_period_count": active_period_count,
            "active_start_utc": active_start_utc,
            "active_end_utc": active_end_utc,
            "binary_prefix_hex": "",
            "source_bytes": len(payload),
            "source_sha256": checksum,
        }

        informed_entities = list(alert.informed_entity)
        if not informed_entities:
            rows.append(
                {
                    **base,
                    "informed_entity_index": -1,
                    "agency_id": "",
                    "route_id": "",
                    "route_type": "",
                    "stop_id": "",
                    "trip_id": "",
                    "trip_route_id": "",
                    "trip_start_date": "",
                    "trip_start_time": "",
                    "direction_id": "",
                }
            )
            continue

        for informed_idx, informed in enumerate(informed_entities):
            trip = informed.trip if informed.HasField("trip") else None
            route_type = str(int(informed.route_type)) if informed.HasField("route_type") else ""
            direction_id = str(int(informed.direction_id)) if informed.HasField("direction_id") else ""

            rows.append(
                {
                    **base,
                    "informed_entity_index": informed_idx,
                    "agency_id": str(informed.agency_id or ""),
                    "route_id": str(informed.route_id or ""),
                    "route_type": route_type,
                    "stop_id": str(informed.stop_id or ""),
                    "trip_id": str(trip.trip_id if trip is not None else ""),
                    "trip_route_id": str(trip.route_id if trip is not None else ""),
                    "trip_start_date": str(trip.start_date if trip is not None else ""),
                    "trip_start_time": str(trip.start_time if trip is not None else ""),
                    "direction_id": direction_id,
                }
            )

    return rows, {
        "parse_mode": "protobuf",
        "status": "ok",
        "rows_emitted": len(rows),
        "feed_entities": len(feed.entity),
        "alert_entities": alert_entities,
        "feed_timestamp_utc": feed_timestamp_utc,
        "parse_caveat": "",
        "source_bytes": len(payload),
        "source_sha256": checksum,
    }


def _write_rows_csv(path: Path, columns: list[str], rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _load_existing_parse_manifest_keys(manifest_csv: Path) -> set[tuple[str, str]]:
    if not manifest_csv.exists() or manifest_csv.stat().st_size == 0:
        return set()

    existing: set[tuple[str, str]] = set()
    with manifest_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            snapshot_path = str(row.get("snapshot_path", "")).strip()
            source_sha256 = str(row.get("source_sha256", "")).strip()
            if snapshot_path and source_sha256:
                existing.add((snapshot_path, source_sha256))
    return existing


def parse_local_service_alert_snapshots(
    *,
    snapshot_paths: Sequence[Path] | None = None,
    output_dir: Path | None = None,
    include_eda_snapshots: bool = True,
    log_path: Path | None = None,
    append_outputs: bool = True,
) -> dict[str, Any]:
    """Parse local service-alert snapshots into CSV outputs with fallback safety."""
    project_root = _resolve_project_root()
    resolved_output_dir = (output_dir or (project_root / "alerts" / "parsed")).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    selected_paths: list[Path]
    if snapshot_paths is not None:
        selected_paths = [path.resolve() for path in snapshot_paths]
    else:
        selected_paths = discover_local_alert_snapshots(include_eda_snapshots=include_eda_snapshots)
    selected_paths = sorted({path.resolve().as_posix(): path.resolve() for path in selected_paths}.values())

    entities_csv = (resolved_output_dir / "service_alert_entities.csv").resolve()
    manifest_csv = (resolved_output_dir / "parse_manifest.csv").resolve()
    summary_json = (resolved_output_dir / "parse_summary.json").resolve()

    existing_manifest_keys: set[tuple[str, str]] = set()
    if append_outputs:
        existing_manifest_keys = _load_existing_parse_manifest_keys(manifest_csv)

    parsed_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    caveats: list[str] = []
    skipped_existing_snapshots = 0

    if not PROTOBUF_DECODE_AVAILABLE:
        caveat_message = FALLBACK_CAVEAT
        if PROTOBUF_IMPORT_ERROR:
            caveat_message += f" Import error: {PROTOBUF_IMPORT_ERROR}"
        caveats.append(caveat_message)

    for snapshot_path in selected_paths:
        if not snapshot_path.exists() or not snapshot_path.is_file():
            continue
        snapshot_path = snapshot_path.resolve()
        snapshot_sha256 = _file_checksum(snapshot_path)
        manifest_key = (snapshot_path.as_posix(), snapshot_sha256)
        if append_outputs and manifest_key in existing_manifest_keys:
            skipped_existing_snapshots += 1
            continue

        try:
            if PROTOBUF_DECODE_AVAILABLE:
                snapshot_rows, snapshot_meta = _parse_snapshot_with_protobuf(snapshot_path)
            else:
                fallback_caveat = caveats[0] if caveats else FALLBACK_CAVEAT
                snapshot_rows, snapshot_meta = _build_fallback_rows(snapshot_path, fallback_caveat)
        except Exception as exc:
            parse_caveat = f"Protobuf decode failed; fallback metadata emitted. Error: {type(exc).__name__}: {exc}"
            if parse_caveat not in caveats:
                caveats.append(parse_caveat)
            snapshot_rows, snapshot_meta = _build_fallback_rows(snapshot_path, parse_caveat)

        snapshot_rel_path = _relative_posix(snapshot_path, project_root)
        for row in snapshot_rows:
            row["snapshot_rel_path"] = snapshot_rel_path
        parsed_rows.extend(snapshot_rows)

        manifest_rows.append(
            {
                "parsed_at": _utc_iso(_utc_now()),
                "snapshot_file": snapshot_path.name,
                "snapshot_path": snapshot_path.as_posix(),
                "snapshot_rel_path": snapshot_rel_path,
                "snapshot_ts_utc": _extract_snapshot_ts_from_name(snapshot_path),
                "feed_timestamp_utc": snapshot_meta.get("feed_timestamp_utc", ""),
                "parse_mode": snapshot_meta.get("parse_mode", ""),
                "status": snapshot_meta.get("status", ""),
                "rows_emitted": snapshot_meta.get("rows_emitted", 0),
                "feed_entities": snapshot_meta.get("feed_entities", 0),
                "alert_entities": snapshot_meta.get("alert_entities", 0),
                "parse_caveat": snapshot_meta.get("parse_caveat", ""),
                "source_bytes": snapshot_meta.get("source_bytes", 0),
                "source_sha256": snapshot_meta.get("source_sha256", ""),
            }
        )
        if append_outputs:
            existing_manifest_keys.add(manifest_key)

    entities_rows_written = 0
    manifest_rows_written = 0
    if append_outputs:
        if parsed_rows:
            entities_rows_written = append_csv_rows(entities_csv, PARSED_COLUMNS, parsed_rows)
        if manifest_rows:
            manifest_rows_written = append_csv_rows(manifest_csv, PARSE_MANIFEST_COLUMNS, manifest_rows)
    else:
        _write_rows_csv(entities_csv, PARSED_COLUMNS, parsed_rows)
        _write_rows_csv(manifest_csv, PARSE_MANIFEST_COLUMNS, manifest_rows)
        entities_rows_written = len(parsed_rows)
        manifest_rows_written = len(manifest_rows)

    parse_status = "empty"
    if parsed_rows:
        parse_status = "ok_fallback" if (not PROTOBUF_DECODE_AVAILABLE or caveats) else "ok"
    elif skipped_existing_snapshots > 0 and append_outputs:
        parse_status = "skipped_existing"

    log_entry = append_alert_sidecar_log_row(
        step="parse_service_alert_snapshots",
        status=parse_status,
        row_count=entities_rows_written,
        details=(
            f"snapshot_count={len(selected_paths)}; protobuf_decode_available={PROTOBUF_DECODE_AVAILABLE}; "
            f"parsed_snapshot_count={len(manifest_rows)}; skipped_existing={skipped_existing_snapshots}; "
            f"append_outputs={append_outputs}; caveats={' | '.join(caveats) if caveats else '-'}"
        ),
        artifact_path=entities_csv.as_posix(),
        log_path=log_path,
    )

    summary: dict[str, Any] = {
        "parsed_at": _utc_iso(_utc_now()),
        "snapshot_count": len(selected_paths),
        "parsed_snapshot_count": len(manifest_rows),
        "skipped_existing_snapshots": skipped_existing_snapshots,
        "rows_emitted": len(parsed_rows),
        "sample_rows_produced": entities_rows_written > 0,
        "append_outputs": append_outputs,
        "rows_written": {
            "service_alert_entities_csv": entities_rows_written,
            "parse_manifest_csv": manifest_rows_written,
        },
        "protobuf_decode_available": PROTOBUF_DECODE_AVAILABLE,
        "caveats": caveats,
        "outputs": {
            "service_alert_entities_csv": entities_csv.as_posix(),
            "parse_manifest_csv": manifest_csv.as_posix(),
            "parse_summary_json": summary_json.as_posix(),
        },
        "sidecar_log": log_entry,
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse local GTFS-RT Service Alert snapshots.")
    parser.add_argument(
        "--snapshot-path",
        action="append",
        type=Path,
        default=None,
        help="Specific snapshot path(s) to parse. Repeat flag for multiple files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional output directory for parsed artifacts.",
    )
    parser.add_argument(
        "--no-eda-snapshots",
        action="store_true",
        help="Do not include ../eda/gtfsrt_eda_outputs/raw in snapshot discovery.",
    )
    parser.add_argument(
        "--log-path",
        type=Path,
        default=None,
        help="Optional side-car log CSV path (defaults to logs/step3_alerts_sidecar_log.csv).",
    )
    parser.add_argument(
        "--overwrite-outputs",
        action="store_true",
        help="Rewrite parsed outputs from selected snapshots instead of append/dedupe mode.",
    )
    return parser


def main() -> None:
    parser = _build_argument_parser()
    args = parser.parse_args()
    result = parse_local_service_alert_snapshots(
        snapshot_paths=args.snapshot_path,
        output_dir=args.output_dir,
        include_eda_snapshots=not args.no_eda_snapshots,
        log_path=args.log_path,
        append_outputs=not args.overwrite_outputs,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
