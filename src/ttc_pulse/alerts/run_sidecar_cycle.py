"""Run one lock-safe GTFS-RT Service Alerts side-car cycle.

Cycle steps:
1. Poll one raw snapshot (network-enabled when requested).
2. Register snapshot into raw manifest (handled by poll module).
3. Parse only that snapshot into append/dedupe parsed outputs.
"""

from __future__ import annotations

import argparse
import fcntl
import json
from pathlib import Path
from typing import Any

from ttc_pulse.alerts._sidecar_log import append_alert_sidecar_log_row
from ttc_pulse.alerts.parse_service_alerts import parse_local_service_alert_snapshots
from ttc_pulse.alerts.poll_service_alerts import run_poll_service_alerts
from ttc_pulse.utils.project_setup import resolve_project_paths


def _try_acquire_cycle_lock(lock_path: Path) -> tuple[bool, Any]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        handle.seek(0)
        handle.truncate(0)
        handle.write("alerts_sidecar_cycle_lock\n")
        handle.flush()
        return True, handle
    except BlockingIOError:
        handle.close()
        return False, None


def run_sidecar_cycle(
    *,
    allow_network: bool = False,
    dry_run: bool = False,
    test_mode: bool = False,
) -> dict[str, Any]:
    paths = resolve_project_paths()
    lock_path = (paths.logs_root / "step3_alerts_sidecar.lock").resolve()
    acquired, lock_handle = _try_acquire_cycle_lock(lock_path)
    if not acquired:
        log_entry = append_alert_sidecar_log_row(
            step="alerts_sidecar_cycle",
            status="skipped_lock_held",
            row_count=0,
            details="Previous side-car cycle still running; this cycle was skipped.",
            artifact_path=lock_path.as_posix(),
        )
        return {
            "status": "skipped_lock_held",
            "lock_path": lock_path.as_posix(),
            "sidecar_log": log_entry,
        }

    try:
        poll_result = run_poll_service_alerts(
            allow_network=allow_network,
            dry_run=dry_run,
            test_mode=test_mode,
            register_manifest=True,
        )
        output_path = str(poll_result.get("output_path") or "").strip()

        parse_result: dict[str, Any] | None = None
        if output_path:
            snapshot_path = Path(output_path)
            if snapshot_path.exists():
                parse_result = parse_local_service_alert_snapshots(
                    snapshot_paths=[snapshot_path],
                    include_eda_snapshots=False,
                    append_outputs=True,
                )

        status = str(poll_result.get("status") or "")
        cycle_status = "ok" if parse_result is not None and status == "ok" else status or "completed"
        log_entry = append_alert_sidecar_log_row(
            step="alerts_sidecar_cycle",
            status=cycle_status,
            row_count=int(parse_result["rows_written"]["service_alert_entities_csv"]) if parse_result else 0,
            details=(
                f"allow_network={allow_network}; dry_run={dry_run}; test_mode={test_mode}; "
                f"poll_status={status}; parsed_snapshot={'yes' if parse_result else 'no'}"
            ),
            artifact_path=output_path or lock_path.as_posix(),
        )
        return {
            "status": cycle_status,
            "poll_result": poll_result,
            "parse_result": parse_result,
            "sidecar_log": log_entry,
        }
    finally:
        try:
            if lock_handle is not None:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
                lock_handle.close()
        except OSError:
            pass


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one GTFS-RT alert side-car cycle.")
    parser.add_argument(
        "--allow-network",
        action="store_true",
        help="Enable outbound live poll to TTC Service Alerts endpoint.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate poll without writing snapshot bytes.",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Load payload from latest local snapshot or fixture logic.",
    )
    return parser


def main() -> None:
    parser = _build_argument_parser()
    args = parser.parse_args()
    result = run_sidecar_cycle(
        allow_network=args.allow_network,
        dry_run=args.dry_run,
        test_mode=args.test_mode,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
