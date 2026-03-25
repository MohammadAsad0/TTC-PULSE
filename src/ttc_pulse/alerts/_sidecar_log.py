"""Shared CSV logging for the GTFS-RT alert side-car."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import append_csv_rows, resolve_project_paths

SIDE_CAR_LOG_COLUMNS = [
    "logged_at",
    "step",
    "status",
    "row_count",
    "details",
    "artifact_path",
]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_sidecar_log_path(log_path: Path | None = None) -> Path:
    if log_path is not None:
        return log_path.expanduser().resolve()
    return (resolve_project_paths().logs_root / "step3_alerts_sidecar_log.csv").resolve()


def append_alert_sidecar_log_row(
    *,
    step: str,
    status: str,
    row_count: int,
    details: str,
    artifact_path: str = "",
    log_path: Path | None = None,
    logged_at: datetime | None = None,
) -> dict[str, Any]:
    """Append a single operational row to the shared alert side-car log."""
    resolved_log_path = resolve_sidecar_log_path(log_path)
    row = {
        "logged_at": _utc_iso(logged_at or _utc_now()),
        "step": step,
        "status": status,
        "row_count": int(row_count),
        "details": details,
        "artifact_path": artifact_path,
    }
    appended_rows = append_csv_rows(resolved_log_path, SIDE_CAR_LOG_COLUMNS, [row])
    return {
        "log_path": resolved_log_path.as_posix(),
        "appended_rows": appended_rows,
        **row,
    }
