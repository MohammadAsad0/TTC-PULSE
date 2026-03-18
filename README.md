# Interactive Visualization & Analytics of TTC Ridership and Service Dynamics

This repository contains exploratory analysis and an interactive dashboard pipeline for TTC service delay and congestion insights using open transit datasets.

## Repository Structure

- `Exploratory Data Analysis/`: notebooks used for GTFS and delay data exploration.
- `TTC-Dashboard-Asad/`: self-contained dashboard project with scripts and generated HTML dashboards.

## TTC Dashboard Module

The `TTC-Dashboard-Asad/` module is now part of this repository and includes:

- `scripts/build_dashboard.py`
- `scripts/build_drilldown_dashboard.py`
- `dist/index.html`
- `dist/drilldown_dashboard.html`

To run the dashboard builders from repository root:

```bash
python3 TTC-Dashboard-Asad/scripts/build_dashboard.py
python3 TTC-Dashboard-Asad/scripts/build_drilldown_dashboard.py
```
