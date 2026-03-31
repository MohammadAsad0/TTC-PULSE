"""In-app live alert polling with APScheduler for Streamlit."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Sequence

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from pandas.errors import ParserError

from ttc_pulse.alerts.parse_service_alerts import parse_local_service_alert_snapshots
from ttc_pulse.alerts.poll_service_alerts import run_poll_service_alerts
from ttc_pulse.utils.project_setup import resolve_project_display_path, resolve_project_paths

DEFAULT_FEED_URLS = [
    "https://gtfsrt.ttc.ca/alerts/subway?format=text",
    "https://gtfsrt.ttc.ca/alerts/bus?format=text",
    "https://gtfsrt.ttc.ca/alerts/streetcar?format=text",
]

POLL_TIMELINE_COLUMNS = [
    "polled_at_utc",
    "status",
    "feeds_polled",
    "snapshot_files_written",
    "parse_rows",
    "new_alert_count",
    "cumulative_distinct_alerts",
    "total_alert_count",
    "notes",
]


def _safe_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    text = str(value).strip()
    return text if text else default

def _load_existing_poll_timeline(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []

    try:
        frame = pd.read_csv(path, on_bad_lines="skip")
    except ParserError:
        # Fallback for older/corrupted rows with inconsistent field counts.
        rows: list[dict[str, Any]] = []
        with path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, restkey="_extra", restval="")
            for row in reader:
                clean = {column: row.get(column, "") for column in POLL_TIMELINE_COLUMNS}
                rows.append(clean)
        frame = pd.DataFrame(rows)
    if frame.empty:
        return []

    for column in POLL_TIMELINE_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""
    frame = frame[POLL_TIMELINE_COLUMNS].copy()
    frame["polled_at_utc"] = pd.to_datetime(frame["polled_at_utc"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["polled_at_utc"]).sort_values("polled_at_utc")
    records = frame.to_dict(orient="records")
    return records[-5000:]

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_iso(value: datetime | None = None) -> str:
    return (value or _utc_now()).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_csv_row(path: Path, columns: list[str], row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    has_header = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        if not has_header:
            writer.writeheader()
        writer.writerow({column: row.get(column, "") for column in columns})


def _read_latest_alerts_from_archive(project_root: Path) -> list[dict[str, Any]]:
    archive_path = project_root / "alerts" / "parsed" / "service_alert_entities.csv"
    if not archive_path.exists():
        return []

    frame = pd.read_csv(
        archive_path,
        usecols=lambda column: column
        in {
            "snapshot_ts_utc",
            "alert_id",
            "header_text",
            "description_text",
            "cause",
            "effect",
            "route_id",
            "stop_id",
        },
    )
    if frame.empty:
        return []

    frame["snapshot_ts_utc"] = pd.to_datetime(frame["snapshot_ts_utc"], errors="coerce", utc=True)
    frame = frame.dropna(subset=["snapshot_ts_utc"])
    if frame.empty:
        return []

    latest_snapshot_ts = frame["snapshot_ts_utc"].max()
    latest_frame = frame[frame["snapshot_ts_utc"] == latest_snapshot_ts].copy()
    if latest_frame.empty:
        return []

    for column in ["alert_id", "header_text", "description_text", "cause", "effect", "route_id", "stop_id"]:
        if column in latest_frame.columns:
            latest_frame[column] = latest_frame[column].astype("string").str.strip()
            latest_frame.loc[latest_frame[column].isin(["", "nan", "None", "<NA>"]), column] = pd.NA

    latest_frame["alert_key"] = (
        latest_frame["alert_id"]
        .fillna(latest_frame["header_text"])
        .fillna(latest_frame["description_text"])
        .fillna("unknown_alert")
    )
    latest_frame["route_id"] = latest_frame["route_id"].astype("string")
    latest_frame["stop_id"] = latest_frame["stop_id"].astype("string")

    grouped = (
        latest_frame.groupby("alert_key", as_index=False)
        .agg(
            snapshot_ts_utc=("snapshot_ts_utc", "first"),
            alert_id=("alert_id", "first"),
            header_text=("header_text", "first"),
            description_text=("description_text", "first"),
            cause=("cause", "first"),
            effect=("effect", "first"),
            route_ids=("route_id", lambda s: sorted({v for v in s.dropna().astype(str).tolist() if v})),
            stop_ids=("stop_id", lambda s: sorted({v for v in s.dropna().astype(str).tolist() if v})),
        )
        .sort_values("header_text", na_position="last")
    )

    alerts: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        alert_id_text = _safe_text(row.get("alert_id"), "")
        alert_key_text = _safe_text(row.get("alert_key"), "unknown_alert")
        stable_id = alert_id_text or alert_key_text
        alerts.append(
            {
                "id": stable_id if stable_id else alert_key_text,
                "captured_at_utc": _utc_iso(row["snapshot_ts_utc"].to_pydatetime()),
                "header_text": _safe_text(row.get("header_text"), "Service notice"),
                "description_text": _safe_text(row.get("description_text"), ""),
                "cause": _safe_text(row.get("cause"), ""),
                "effect": _safe_text(row.get("effect"), ""),
                "route_ids": row["route_ids"] if isinstance(row["route_ids"], list) else [],
                "stop_ids": row["stop_ids"] if isinstance(row["stop_ids"], list) else [],
            }
        )
    return alerts


def poll_and_parse_once(feed_urls: Sequence[str]) -> dict[str, Any]:
    project_root = resolve_project_paths().project_root
    base_ts = _utc_now()
    poll_results: list[dict[str, Any]] = []
    written_snapshot_paths: list[Path] = []

    for index, feed_url in enumerate(feed_urls):
        poll_result = run_poll_service_alerts(
            as_of=base_ts,
            feed_url=str(feed_url),
            allow_network=True,
            register_manifest=True,
            skip_if_unchanged=False,
        )
        poll_results.append(
            {
                "feed_url": str(feed_url),
                "status": str(poll_result.get("status") or "unknown"),
                "http_status": poll_result.get("http_status"),
                "notes": str(poll_result.get("notes") or "").strip(),
            }
        )
        output_path_text = str(poll_result.get("output_path") or "").strip()
        output_path_fs_text = str(poll_result.get("output_path_fs") or "").strip()
        if output_path_text:
            output_path = (
                Path(output_path_fs_text).resolve()
                if output_path_fs_text
                else resolve_project_display_path(output_path_text, project_root)
            )
            if output_path.exists():
                written_snapshot_paths.append(output_path)

    parse_rows = 0
    parse_status = "skipped"
    if written_snapshot_paths:
        parse_result = parse_local_service_alert_snapshots(
            snapshot_paths=written_snapshot_paths,
            include_eda_snapshots=False,
            append_outputs=True,
        )
        parse_rows = int(parse_result.get("rows_written", {}).get("service_alert_entities_csv", 0) or 0)
        parse_status = str(parse_result.get("status") or "completed")

    poll_statuses = {entry["status"] for entry in poll_results}
    ok_count = sum(1 for row in poll_results if row["status"] in {"ok", "no_change", "ok_test_mode"})
    return {
        "polled_at_utc": _utc_iso(base_ts),
        "feed_urls": list(feed_urls),
        "poll_results": poll_results,
        "poll_statuses": sorted(poll_statuses),
        "ok_count": ok_count,
        "snapshot_files_written": len(written_snapshot_paths),
        "parse_rows": parse_rows,
        "parse_status": parse_status,
    }


@dataclass
class LivePollingState:
    lock: Lock = field(default_factory=Lock)
    initialized: bool = False
    seen_alert_ids: set[str] = field(default_factory=set)
    current_alerts: list[dict[str, Any]] = field(default_factory=list)
    new_alert_events: list[dict[str, Any]] = field(default_factory=list)
    poll_timeline: list[dict[str, Any]] = field(default_factory=list)
    last_error: str = ""
    last_poll_utc: str = ""


class LiveAlertPollingManager:
    """APScheduler-backed polling manager with stateful new-alert detection."""

    def __init__(self, feed_urls: Sequence[str], poll_seconds: int = 30) -> None:
        self.paths = resolve_project_paths()
        self.feed_urls = [str(url).strip() for url in feed_urls if str(url).strip()] or list(DEFAULT_FEED_URLS)
        self.poll_seconds = max(5, int(poll_seconds))
        self.state = LivePollingState()
        self.timeline_log_path = self.paths.logs_root / "live_alert_poll_timeline.csv"
        self.state.poll_timeline = _load_existing_poll_timeline(self.timeline_log_path)
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self._started = False

    def start(self) -> None:
        if self._started:
            return
        self.scheduler.add_job(
            self._run_poll_cycle,
            trigger="interval",
            seconds=self.poll_seconds,
            id="ttc_pulse_live_alert_poll",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        self.scheduler.start()
        self._started = True
        self._run_poll_cycle()

    def trigger_now(self) -> dict[str, Any]:
        return self._run_poll_cycle()

    def snapshot(self) -> dict[str, Any]:
        with self.state.lock:
            return {
                "initialized": self.state.initialized,
                "current_alerts": [dict(item) for item in self.state.current_alerts],
                "new_alert_events": [dict(item) for item in self.state.new_alert_events],
                "poll_timeline": [dict(item) for item in self.state.poll_timeline],
                "last_error": self.state.last_error,
                "last_poll_utc": self.state.last_poll_utc,
                "feed_urls": list(self.feed_urls),
            }

    def handle_new_alert(self, alert: dict[str, Any]) -> None:
        event = {
            "detected_at_utc": _utc_iso(),
            "alert_id": str(alert.get("id") or "").strip(),
            "header_text": str(alert.get("header_text") or "").strip(),
            "cause": str(alert.get("cause") or "").strip(),
            "effect": str(alert.get("effect") or "").strip(),
        }
        self.state.new_alert_events.append(event)
        if len(self.state.new_alert_events) > 1000:
            self.state.new_alert_events = self.state.new_alert_events[-1000:]

    def _append_timeline_record(self, row: dict[str, Any]) -> None:
        _append_csv_row(self.timeline_log_path, POLL_TIMELINE_COLUMNS, row)

    def _run_poll_cycle(self) -> dict[str, Any]:
        with self.state.lock:
            try:
                poll_result = poll_and_parse_once(self.feed_urls)
                current_alerts = _read_latest_alerts_from_archive(self.paths.project_root)
                current_ids = {str(item.get("id") or "").strip() for item in current_alerts if str(item.get("id") or "").strip()}

                new_alerts: list[dict[str, Any]] = []
                if not self.state.initialized:
                    self.state.seen_alert_ids = set(current_ids)
                    self.state.initialized = True
                else:
                    new_alerts = [item for item in current_alerts if str(item.get("id") or "").strip() not in self.state.seen_alert_ids]
                    for alert in new_alerts:
                        self.handle_new_alert(alert)
                    self.state.seen_alert_ids.update({str(item.get("id") or "").strip() for item in new_alerts if str(item.get("id") or "").strip()})

                self.state.current_alerts = current_alerts
                self.state.last_poll_utc = str(poll_result.get("polled_at_utc") or _utc_iso())
                self.state.last_error = ""

                timeline_row = {
                    "polled_at_utc": self.state.last_poll_utc,
                    "status": "ok",
                    "feeds_polled": len(self.feed_urls),
                    "snapshot_files_written": int(poll_result.get("snapshot_files_written") or 0),
                    "parse_rows": int(poll_result.get("parse_rows") or 0),
                    "new_alert_count": len(new_alerts),
                    "cumulative_distinct_alerts": len(self.state.seen_alert_ids),
                    "total_alert_count": len(current_alerts),
                    "notes": f"parse_status={poll_result.get('parse_status', '')}",
                }
                self.state.poll_timeline.append(timeline_row)
                if len(self.state.poll_timeline) > 5000:
                    self.state.poll_timeline = self.state.poll_timeline[-5000:]
                self._append_timeline_record(timeline_row)
                return {
                    **poll_result,
                    "new_alert_count": len(new_alerts),
                    "total_alert_count": len(current_alerts),
                }
            except Exception as exc:
                error_text = f"{type(exc).__name__}: {exc}"
                self.state.last_error = error_text
                self.state.last_poll_utc = _utc_iso()
                timeline_row = {
                    "polled_at_utc": self.state.last_poll_utc,
                    "status": "error",
                    "feeds_polled": len(self.feed_urls),
                    "snapshot_files_written": 0,
                    "parse_rows": 0,
                    "new_alert_count": 0,
                    "cumulative_distinct_alerts": len(self.state.seen_alert_ids),
                    "total_alert_count": len(self.state.current_alerts),
                    "notes": error_text,
                }
                self.state.poll_timeline.append(timeline_row)
                if len(self.state.poll_timeline) > 5000:
                    self.state.poll_timeline = self.state.poll_timeline[-5000:]
                self._append_timeline_record(timeline_row)
                return {
                    "polled_at_utc": self.state.last_poll_utc,
                    "status": "error",
                    "error": error_text,
                    "new_alert_count": 0,
                    "total_alert_count": len(self.state.current_alerts),
                    "poll_results": [],
                    "snapshot_files_written": 0,
                    "parse_rows": 0,
                    "parse_status": "error",
                }



