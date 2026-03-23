"""Poll TTC GTFS-RT Service Alerts with offline-safe controls."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ttc_pulse.utils.project_setup import resolve_project_paths

DEFAULT_ALERTS_URL = "https://bustime.ttc.ca/gtfsrt/alerts"
POLL_INTERVAL_MINUTES = 30
SNAPSHOT_EXTENSIONS = {".pb", ".bin"}

RAW_MANIFEST_COLUMNS = [
    "run_id",
    "recorded_at",
    "snapshot_ts_utc",
    "poll_window_start_utc",
    "poll_window_end_utc",
    "mode",
    "status",
    "source",
    "source_path",
    "output_path",
    "output_rel_path",
    "file_size_bytes",
    "sha256",
    "notes",
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


def _append_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    has_header = path.exists() and path.stat().st_size > 0
    row_count = 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not has_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in fieldnames})
            row_count += 1
    return row_count


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_iso(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _floor_half_hour(value: datetime) -> datetime:
    aligned = _as_utc(value).replace(second=0, microsecond=0)
    minute_bucket = 0 if aligned.minute < POLL_INTERVAL_MINUTES else POLL_INTERVAL_MINUTES
    return aligned.replace(minute=minute_bucket)


def _snapshot_stamp(value: datetime) -> str:
    return _as_utc(value).strftime("%Y%m%dT%H%M%SZ")


def _build_snapshot_name(value: datetime, extension: str) -> str:
    suffix = extension.lower() if extension.lower() in SNAPSHOT_EXTENSIONS else ".pb"
    return f"alerts_{_snapshot_stamp(value)}{suffix}"


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def discover_local_alert_snapshots(include_eda_snapshots: bool = True) -> list[Path]:
    """Discover local Service Alert protobuf snapshots from known project roots."""
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


def _fetch_alerts_payload(feed_url: str, timeout_seconds: float) -> dict[str, Any]:
    request = Request(
        feed_url,
        headers={
            "User-Agent": "ttc-pulse-step3-alerts-sidecar/1.0",
            "Accept": "application/x-protobuf, application/octet-stream",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 (expected service URL)
            payload = response.read()
            http_status = getattr(response, "status", response.getcode())
            content_type = response.headers.get("Content-Type", "")
            return {
                "ok": True,
                "payload": payload,
                "http_status": int(http_status) if http_status is not None else None,
                "content_type": content_type,
            }
    except HTTPError as exc:
        return {
            "ok": False,
            "error": f"HTTPError {exc.code}: {exc.reason}",
            "http_status": exc.code,
            "content_type": "",
        }
    except (TimeoutError, URLError, OSError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "http_status": None, "content_type": ""}


def register_raw_snapshot_record(
    poll_result: dict[str, Any],
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Append one row into alerts/raw_snapshots/manifest.csv."""
    project_root = _resolve_project_root()
    resolved_manifest = (manifest_path or (project_root / "alerts" / "raw_snapshots" / "manifest.csv")).resolve()
    resolved_manifest.parent.mkdir(parents=True, exist_ok=True)

    output_path_text = str(poll_result.get("output_path") or "")
    output_rel_path = ""
    if output_path_text:
        output_rel_path = _relative_posix(Path(output_path_text), project_root)

    row = {
        "run_id": poll_result.get("run_id", ""),
        "recorded_at": _utc_iso(_utc_now()),
        "snapshot_ts_utc": poll_result.get("snapshot_ts_utc", ""),
        "poll_window_start_utc": poll_result.get("poll_window_start_utc", ""),
        "poll_window_end_utc": poll_result.get("poll_window_end_utc", ""),
        "mode": poll_result.get("mode", ""),
        "status": poll_result.get("status", ""),
        "source": poll_result.get("source", ""),
        "source_path": poll_result.get("source_path", ""),
        "output_path": output_path_text,
        "output_rel_path": output_rel_path,
        "file_size_bytes": poll_result.get("file_size_bytes", ""),
        "sha256": poll_result.get("sha256", ""),
        "notes": poll_result.get("notes", ""),
    }
    appended_rows = _append_csv_rows(resolved_manifest, RAW_MANIFEST_COLUMNS, [row])
    return {
        "manifest_path": resolved_manifest.as_posix(),
        "appended_rows": appended_rows,
        "registered_status": row["status"],
    }


def run_poll_service_alerts(
    *,
    as_of: datetime | None = None,
    feed_url: str = DEFAULT_ALERTS_URL,
    output_dir: Path | None = None,
    timeout_seconds: float = 15.0,
    dry_run: bool = False,
    test_mode: bool = False,
    allow_network: bool = False,
    fixture_path: Path | None = None,
    register_manifest: bool = False,
) -> dict[str, Any]:
    """Poll one GTFS-RT Service Alerts snapshot with explicit safety controls."""
    project_root = _resolve_project_root()
    poll_time = _as_utc(as_of or _utc_now())
    window_start = _floor_half_hour(poll_time)
    window_end = window_start + timedelta(minutes=POLL_INTERVAL_MINUTES)
    run_id = f"alerts_sidecar_{_snapshot_stamp(poll_time)}"

    resolved_output_dir = (output_dir or (project_root / "alerts" / "raw_snapshots")).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    local_snapshots = discover_local_alert_snapshots(include_eda_snapshots=True)

    mode = "test" if test_mode else "live"
    status = "pending"
    notes: list[str] = []
    source = ""
    source_path = ""
    payload: bytes | None = None
    content_type = ""
    http_status: int | None = None

    if test_mode:
        candidate: Path | None = None
        if fixture_path is not None and fixture_path.exists():
            candidate = fixture_path.resolve()
            source = "fixture_path"
        elif local_snapshots:
            candidate = sorted(local_snapshots, key=lambda value: value.stat().st_mtime)[-1]
            source = "latest_local_snapshot"

        if candidate is None:
            status = "test_mode_no_fixture"
            notes.append("No fixture and no local snapshots available.")
        else:
            source_path = candidate.as_posix()
            payload = candidate.read_bytes()
            status = "test_mode_loaded"
    elif allow_network:
        source = "live_endpoint"
        fetch_result = _fetch_alerts_payload(feed_url=feed_url, timeout_seconds=timeout_seconds)
        content_type = str(fetch_result.get("content_type") or "")
        http_status_value = fetch_result.get("http_status")
        http_status = int(http_status_value) if http_status_value is not None else None

        if fetch_result.get("ok"):
            payload = fetch_result["payload"]
            status = "fetched_live_snapshot"
        else:
            status = "network_unavailable"
            notes.append(str(fetch_result.get("error") or "Unknown network error"))
    else:
        source = "none"
        status = "safe_no_network"
        notes.append("Network disabled by allow_network=False; no outbound request attempted.")

    output_path = ""
    checksum = ""
    payload_size = 0
    if payload is not None:
        payload_size = len(payload)
        checksum = _sha256_bytes(payload)
        inferred_extension = ".pb"
        if source_path:
            source_suffix = Path(source_path).suffix.lower()
            if source_suffix in SNAPSHOT_EXTENSIONS:
                inferred_extension = source_suffix
        snapshot_name = _build_snapshot_name(poll_time, inferred_extension)
        target_path = (resolved_output_dir / snapshot_name).resolve()
        output_path = target_path.as_posix()

        if dry_run:
            status = "dry_run" if status != "network_unavailable" else "dry_run_network_unavailable"
        else:
            target_path.write_bytes(payload)
            if test_mode:
                status = "ok_test_mode"
            elif status == "fetched_live_snapshot":
                status = "ok"

    result: dict[str, Any] = {
        "run_id": run_id,
        "snapshot_ts_utc": _utc_iso(poll_time),
        "poll_window_start_utc": _utc_iso(window_start),
        "poll_window_end_utc": _utc_iso(window_end),
        "poll_interval_minutes": POLL_INTERVAL_MINUTES,
        "mode": mode,
        "status": status,
        "dry_run": dry_run,
        "test_mode": test_mode,
        "allow_network": allow_network,
        "feed_url": feed_url,
        "http_status": http_status,
        "content_type": content_type,
        "source": source,
        "source_path": source_path,
        "output_path": output_path,
        "file_size_bytes": payload_size,
        "sha256": checksum,
        "local_snapshot_candidates": len(local_snapshots),
        "notes": " | ".join(notes).strip(),
    }

    if register_manifest:
        result["raw_manifest"] = register_raw_snapshot_record(result)
    return result


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll TTC GTFS-RT Service Alerts.")
    parser.add_argument("--feed-url", default=DEFAULT_ALERTS_URL, help="Service Alerts feed URL.")
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="Network timeout in seconds.",
    )
    parser.add_argument("--allow-network", action="store_true", help="Enable outbound network poll.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate poll without writing snapshot bytes.")
    parser.add_argument("--test-mode", action="store_true", help="Load payload from local fixture/snapshot.")
    parser.add_argument(
        "--fixture-path",
        type=Path,
        default=None,
        help="Optional local protobuf fixture path for test mode.",
    )
    parser.add_argument(
        "--register-manifest",
        action="store_true",
        help="Append a row into alerts/raw_snapshots/manifest.csv.",
    )
    return parser


def main() -> None:
    parser = _build_argument_parser()
    args = parser.parse_args()
    result = run_poll_service_alerts(
        feed_url=args.feed_url,
        timeout_seconds=args.timeout_seconds,
        dry_run=args.dry_run,
        test_mode=args.test_mode,
        allow_network=args.allow_network,
        fixture_path=args.fixture_path,
        register_manifest=args.register_manifest,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
