# Interactive Visualization & Analytics of TTC Ridership and Service Dynamics

This project turns the provided TTC CSV archive into a polished, self-contained interactive dashboard. It combines:

- TTC delay history across bus, streetcar, and subway modes
- Recent-period incident drill-downs for 2025-01-01 through 2026-01-31
- GTFS route structure to benchmark disruption against scheduled service exposure

## Deliverables

- Original dashboard: [dist/index.html](/Users/muhammadasad/Documents/TTC-PULSE/dist/index.html)
- Advanced drilldown dashboard: [dist/drilldown_dashboard.html](/Users/muhammadasad/Documents/TTC-PULSE/dist/drilldown_dashboard.html)
- Original generator: [scripts/build_dashboard.py](/Users/muhammadasad/Documents/TTC-PULSE/scripts/build_dashboard.py)
- Drilldown generator: [scripts/build_drilldown_dashboard.py](/Users/muhammadasad/Documents/TTC-PULSE/scripts/build_drilldown_dashboard.py)

## What The Dashboard Shows

- All-time monthly disruption trends by mode
- GTFS-informed route exposure versus disruption load
- Recent hotspot locations
- Day-hour delay heatmaps
- Dominant delay codes
- Coverage summaries and analyst insights

## What The Drilldown Dashboard Adds

- Year-wise and month-wise transport-mode analysis
- Delay frequency versus delay-time combination charts
- Clickable route explorer with separate yearly and monthly route visuals
- Clickable location explorer with separate yearly and monthly location visuals
- Delay-band distributions for both selected routes and selected locations
- Route weekday-hour heatmaps for deeper operational pattern analysis

## Key Findings

- The historical archive covers bus from 2014-01-01 to 2024-12-31, streetcar from 2014-01-02 to 2024-12-31, and subway from 2014-01-01 to 2024-12-31.
- In the recent monitoring window from 2025-01-01 to 2026-01-31, the three modes combined logged 113,013 incidents and 1,794,515 total delay minutes.
- Bus delays dominate disruption volume in both the long-run archive and the recent window, with 707,388 historical incidents and 69,037 recent incidents.
- January 2026 is the highest recent-pressure month across all modes, with 9,852 incidents and 183,715 total delay minutes.
- `KENNEDY STATION` is the strongest recent hotspot overall, driven by bus operations with 2,158 incidents and 28,462 delay minutes.
- In the GTFS-normalized snapshot benchmark, Line 1 `Yonge-University` shows the highest disruption load relative to scheduled service exposure.

## Methodology Note

The supplied CSVs do not contain direct ridership counts. To stay honest with the available data, the dashboard treats GTFS scheduled trips and stop coverage as a service-demand proxy rather than literal passenger boardings. That means the project is strongest for:

- service pressure analysis
- disruption hotspot detection
- route benchmarking
- schedule-exposure comparisons

It should not be interpreted as an exact rider-count dashboard.

## How To Regenerate

The dataset already includes a Python environment with `pandas`, so the simplest command is:

```bash
"/Users/muhammadasad/Library/CloudStorage/OneDrive-YorkUniversity/Omkumar Patel's files - 6414-Data-Visualization/Code/Dataset/.venv/bin/python" scripts/build_dashboard.py
```

To rebuild the advanced drilldown dashboard without disturbing the original file:

```bash
"/Users/muhammadasad/Library/CloudStorage/OneDrive-YorkUniversity/Omkumar Patel's files - 6414-Data-Visualization/Code/Dataset/.venv/bin/python" scripts/build_drilldown_dashboard.py
```

If your dataset path changes, pass it explicitly:

```bash
python3 scripts/build_dashboard.py --data-root "/path/to/Dataset" --output dist/index.html
```

Then open either [dist/index.html](/Users/muhammadasad/Documents/TTC-PULSE/dist/index.html) or [dist/drilldown_dashboard.html](/Users/muhammadasad/Documents/TTC-PULSE/dist/drilldown_dashboard.html) in a browser.

## Notes

- The HTML file is self-contained, but the visual layer uses Plotly and Google Fonts from CDNs at runtime.
- TTC code-description files appear cross-labeled across source folders, so the script remaps them by observed code overlap before display.
- The advanced drilldown dashboard keeps the original dashboard untouched and caps the location catalog to the strongest hotspot set per mode so the file stays usable.
