"""Skeleton DAG for TTC GTFS-RT alerts pipeline."""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def poll_alerts_placeholder(**_: object) -> None:
    """Placeholder ingestion task."""
    print("poll_alerts_placeholder: replace with GTFS-RT polling logic.")


def process_alerts_placeholder(**_: object) -> None:
    """Placeholder transform/load task."""
    print("process_alerts_placeholder: replace with processing logic.")


default_args = {"owner": "ttc_pulse"}

with DAG(
    dag_id="ttc_gtfsrt_alerts_pipeline",
    description="Legacy TTC GTFS-RT alerts scaffold (disabled schedule).",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ttc", "gtfsrt", "alerts", "legacy"],
) as dag:
    poll_alerts = PythonOperator(
        task_id="poll_alerts",
        python_callable=poll_alerts_placeholder,
    )

    process_alerts = PythonOperator(
        task_id="process_alerts",
        python_callable=process_alerts_placeholder,
    )

    poll_alerts >> process_alerts
