"""Airflow DAG: TTC GTFS-RT Service Alerts side-car poll/parse flow."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator


def _bool_from_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _ensure_src_on_pythonpath() -> None:
    project_root = Path(__file__).resolve().parents[2]
    src_root = project_root / "src"
    src_text = src_root.as_posix()
    if src_text not in sys.path:
        sys.path.insert(0, src_text)


def _poll_service_alerts_task(**_: Any) -> dict[str, Any]:
    _ensure_src_on_pythonpath()
    from ttc_pulse.alerts.poll_service_alerts import run_poll_service_alerts

    allow_network = _bool_from_env("TTC_PULSE_ALERTS_ALLOW_NETWORK", default=False)
    dry_run = _bool_from_env("TTC_PULSE_ALERTS_DRY_RUN", default=False)
    test_mode = _bool_from_env("TTC_PULSE_ALERTS_TEST_MODE", default=False)

    return run_poll_service_alerts(
        allow_network=allow_network,
        dry_run=dry_run,
        test_mode=test_mode,
        register_manifest=False,
    )


def _register_raw_snapshot_task(**context: Any) -> dict[str, Any]:
    _ensure_src_on_pythonpath()
    from ttc_pulse.alerts.poll_service_alerts import register_raw_snapshot_record

    poll_result = context["ti"].xcom_pull(task_ids="poll_service_alerts") or {}
    project_root = Path(__file__).resolve().parents[2]
    manifest_path = project_root / "alerts" / "raw_snapshots" / "manifest.csv"
    registration = register_raw_snapshot_record(poll_result=poll_result, manifest_path=manifest_path)
    registration["status"] = "ok"
    return registration


def _parse_entities_task(**context: Any) -> dict[str, Any]:
    _ensure_src_on_pythonpath()
    from ttc_pulse.alerts.parse_service_alerts import parse_local_service_alert_snapshots

    poll_result = context["ti"].xcom_pull(task_ids="poll_service_alerts") or {}
    output_path = str(poll_result.get("output_path") or "").strip()

    snapshot_paths = None
    if output_path:
        path = Path(output_path)
        if path.exists():
            snapshot_paths = [path]
    return parse_local_service_alert_snapshots(snapshot_paths=snapshot_paths)


def _fact_normalization_hook_task(**_: Any) -> dict[str, Any]:
    return {
        "status": "hook_pending",
        "hook_name": "fact_normalization",
        "details": "Integrate fact normalization once parsed alerts are mapped into silver/fact contracts.",
    }


def _gold_validation_refresh_hook_task(**_: Any) -> dict[str, Any]:
    return {
        "status": "hook_pending",
        "hook_name": "gold_alert_validation_refresh",
        "details": "Integrate gold alert validation refresh once normalization hook is wired.",
    }


default_args = {"owner": "ttc_pulse"}

with DAG(
    dag_id="poll_gtfsrt_alerts",
    description="30-minute Service Alerts side-car flow: poll, register, parse, and hook refresh steps.",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["ttc", "gtfsrt", "alerts", "sidecar"],
) as dag:
    poll_service_alerts = PythonOperator(
        task_id="poll_service_alerts",
        python_callable=_poll_service_alerts_task,
    )

    register_raw_snapshot = PythonOperator(
        task_id="register_raw_snapshot",
        python_callable=_register_raw_snapshot_task,
    )

    parse_entities = PythonOperator(
        task_id="parse_entities",
        python_callable=_parse_entities_task,
    )

    hook_fact_normalization = PythonOperator(
        task_id="hook_fact_normalization",
        python_callable=_fact_normalization_hook_task,
    )

    hook_gold_alert_validation_refresh = PythonOperator(
        task_id="hook_gold_alert_validation_refresh",
        python_callable=_gold_validation_refresh_hook_task,
    )

    (
        poll_service_alerts
        >> register_raw_snapshot
        >> parse_entities
        >> hook_fact_normalization
        >> hook_gold_alert_validation_refresh
    )
