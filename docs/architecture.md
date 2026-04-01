# TTC Pulse Architecture

## Purpose
System-level architecture for TTC Pulse: local-first transit reliability analytics using bus/subway delay logs, static GTFS, and GTFS-RT alerts. Streetcar and AI features exist as extensions; bus + subway + GTFS + alerts remain the core.

## Simple Diagram
```mermaid
flowchart LR
    A["Source Data\nBus, Subway, GTFS, Alerts"] --> B["Bronze\nRow-preserving load"]
    B --> C["Silver\nNormalize and link"]
    C --> D["Gold\nDashboard marts"]
    D --> E["Streamlit\nDashboard and AI helpers"]
```

## Detailed Diagram
```mermaid
flowchart TD
    subgraph Sources["Sources"]
        S1["Bus delay files"]
        S2["Subway delay files"]
        S3["Streetcar delay files"]
        S4["Static GTFS"]
        S5["GTFS-RT alerts"]
    end
    subgraph Raw["Raw"]
        R1["Source registries"]
        R2["Alert manifests"]
    end
    subgraph Bronze["Bronze"]
        B1["DuckDB bronze tables"]
        B2["Lineage fields"]
    end
    subgraph Silver["Silver"]
        SV1["Normalized events"]
        SV2["Dimensions"]
        SV3["Alias dimensions"]
        SV4["Bridge"]
        SV5["Facts"]
        SV6["Review tables"]
    end
    subgraph Gold["Gold"]
        G1["delay_core"]
        G2["route_time_metrics"]
        G3["station_time_metrics"]
        G4["time_reliability"]
        G5["top_offender_ranking"]
        G6["alert_validation"]
        G7["spatial_hotspot"]
        G8["linkage_quality"]
    end
    subgraph App["App"]
        A1["Streamlit pages"]
        A2["Loaders/charts"]
        A3["AI Explain"]
        A4["AI Chat"]
    end
    subgraph Runtime["Runtime"]
        D1["DuckDB"]
        D2["Parquet"]
    end
    S1 --> R1
    S2 --> R1
    S3 --> R1
    S4 --> R1
    S5 --> R2
    R1 --> B1
    R2 --> B1
    B1 --> B2
    B2 --> SV1
    B1 --> SV2
    B1 --> SV3
    B1 --> SV4
    SV1 --> SV5
    SV2 --> SV5
    SV3 --> SV5
    SV4 --> SV5
    SV3 --> SV6
    SV5 --> G1
    SV5 --> G2
    SV5 --> G3
    SV5 --> G4
    SV5 --> G5
    SV5 --> G6
    SV5 --> G7
    SV5 --> G8
    G1 --> A2
    G2 --> A2
    G3 --> A2
    G4 --> A2
    G5 --> A2
    G6 --> A2
    G7 --> A2
    G8 --> A2
    A2 --> A1
    A2 --> A3
    A2 --> A4
    D1 --- B1
    D1 --- SV5
    D1 --- G1
    D2 --- B1
    D2 --- SV1
    D2 --- G1
```

## Layer Definitions
- Raw: registries/manifests for traceability.
- Bronze: row-preserving load into DuckDB with lineage.
- Silver: canonical modeling (events, dimensions, aliases, bridge, facts, review tables).
- Gold: presentation marts (rankings, trends, alert validation, spatial, linkage QA).

## Key Outputs
Silver: `fact_delay_events_norm`, `fact_gtfsrt_alerts_norm`, `dim_route_gtfs`, `dim_stop_gtfs`, `dim_route_alias`, `dim_station_alias`, `bridge_route_direction_stop`.

Gold: `gold_delay_events_core`, `gold_route_time_metrics`, `gold_station_time_metrics`, `gold_time_reliability`, `gold_top_offender_ranking`, `gold_alert_validation`, `gold_spatial_hotspot`, `gold_linkage_quality`.

## Runtime Summary
- DuckDB for SQL; Parquet for artifacts
- Streamlit for UI
- Local sidecar for GTFS-RT polling; 

