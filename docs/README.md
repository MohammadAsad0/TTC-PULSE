# TTC Pulse Docs

## Execution Root

`/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse`

## Core Documents

- Detailed architecture: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/docs/final_detailed_architecture.md`
- Schema DDL: `/Users/om-college/Work/2 Canada/York/Winter26/DataViz/Project/ttc_pulse/docs/schema_ddl.sql`

## Execution Pointers

- Launch Streamlit app:
  `streamlit run /Users/om-college/Work/2\ Canada/York/Winter26/DataViz/Project/ttc_pulse/app/streamlit_app.py`
- Poll GTFS-RT alerts once:
  `python -m ttc_pulse.alerts.poll_alerts`
- Trigger Airflow DAG (from Airflow env):
  `airflow dags trigger ttc_gtfsrt_alerts_pipeline`
