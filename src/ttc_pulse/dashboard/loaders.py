"""DuckDB-backed data loading helpers for the Step 4 dashboard."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import pandas as pd

from ttc_pulse.utils.project_setup import resolve_project_paths

GOLD_TABLE_FILES: dict[str, str] = {
    "gold_linkage_quality": "gold/gold_linkage_quality.parquet",
    "gold_delay_events_core": "gold/gold_delay_events_core.parquet",
    "gold_route_time_metrics": "gold/gold_route_time_metrics.parquet",
    "gold_station_time_metrics": "gold/gold_station_time_metrics.parquet",
    "gold_time_reliability": "gold/gold_time_reliability.parquet",
    "gold_top_offender_ranking": "gold/gold_top_offender_ranking.parquet",
    "gold_alert_validation": "gold/gold_alert_validation.parquet",
    "gold_spatial_hotspot": "gold/gold_spatial_hotspot.parquet",
}

DATASET_FILES: dict[str, str] = {
    "bus": "silver/silver_bus_events.parquet",
    "streetcar": "silver/silver_streetcar_events.parquet",
    "subway": "silver/silver_subway_events.parquet",
}


def _debug_errors_enabled() -> bool:
    value = os.getenv("TTC_PULSE_DEBUG_ERRORS")
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _safe_error_message(context: str, table_name: str, exc: Exception) -> str:
    if _debug_errors_enabled():
        return f"{context}: {type(exc).__name__}: {exc}"
    return f"{context} for `{table_name}`. Enable debug flag `TTC_PULSE_DEBUG_ERRORS=1` for detailed traceback context."


@dataclass(frozen=True)
class QueryResult:
    """Container for table-backed query results."""

    table_name: str
    status: str
    source: str
    row_count: int
    message: str
    frame: pd.DataFrame
    total_row_count: int | None = None


@dataclass(frozen=True)
class TableSnapshot:
    """Point-in-time status for a single Gold mart."""

    table_name: str
    status: str
    source: str
    row_count: int
    message: str


@dataclass(frozen=True)
class _TableSource:
    table_name: str
    from_sql: str | None
    source: str
    message: str


def _sql_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def resolve_duckdb_path(db_path: str | Path | None = None) -> Path:
    """Resolve database path from explicit arg, env, or project defaults."""
    if db_path is not None:
        return Path(db_path).expanduser().resolve()

    env_path = os.getenv("TTC_PULSE_DUCKDB_PATH")
    if env_path:
        return Path(env_path).expanduser().resolve()

    return resolve_project_paths().db_path.resolve()


def resolve_project_root() -> Path:
    """Resolve project root from package utilities."""
    return resolve_project_paths().project_root.resolve()


def open_connection(db_path: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open read-only DuckDB when possible with in-memory fallback."""
    resolved_db_path = resolve_duckdb_path(db_path)
    if not resolved_db_path.exists():
        return duckdb.connect(":memory:")

    try:
        return duckdb.connect(resolved_db_path.as_posix(), read_only=True)
    except duckdb.Error:
        try:
            return duckdb.connect(resolved_db_path.as_posix())
        except duckdb.Error:
            return duckdb.connect(":memory:")


def _table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    exists_row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
            AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(exists_row and int(exists_row[0]) > 0)


def _resolve_table_source(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    project_root: Path | None = None,
) -> _TableSource:
    root = project_root or resolve_project_root()
    if _table_exists(connection, table_name):
        return _TableSource(
            table_name=table_name,
            from_sql=_sql_identifier(table_name),
            source="duckdb",
            message="Loaded from DuckDB table.",
        )

    relative_path = GOLD_TABLE_FILES.get(table_name)
    if relative_path is None:
        return _TableSource(
            table_name=table_name,
            from_sql=None,
            source="missing",
            message="No registered fallback path for this table.",
        )

    parquet_path = (root / relative_path).resolve()
    if parquet_path.exists():
        return _TableSource(
            table_name=table_name,
            from_sql=f"read_parquet({_sql_literal(parquet_path.as_posix())})",
            source="parquet",
            message=f"Loaded from parquet fallback: {relative_path}",
        )

    return _TableSource(
        table_name=table_name,
        from_sql=None,
        source="missing",
        message=f"Table missing in DuckDB and fallback parquet not found: {relative_path}",
    )


def query_table(
    table_name: str,
    query_template: str = "SELECT * FROM {source}",
    params: Sequence[object] | None = None,
    db_path: str | Path | None = None,
) -> QueryResult:
    """
    Run a SQL query against a Gold table or parquet fallback.

    The `query_template` must include a `{source}` placeholder.
    """
    if "{source}" not in query_template:
        raise ValueError("query_template must contain a {source} placeholder.")

    connection = open_connection(db_path=db_path)
    try:
        source = _resolve_table_source(connection=connection, table_name=table_name)
        if source.from_sql is None:
            return QueryResult(
                table_name=table_name,
                status="missing",
                source=source.source,
                row_count=0,
                message=source.message,
                frame=pd.DataFrame(),
            )

        query_sql = query_template.replace("{source}", source.from_sql)
        frame = connection.execute(query_sql, list(params or [])).df()
        status = "ok" if not frame.empty else "empty"
        return QueryResult(
            table_name=table_name,
            status=status,
            source=source.source,
            row_count=int(len(frame)),
            message=source.message,
            frame=frame,
        )
    except Exception as exc:
        return QueryResult(
            table_name=table_name,
            status="error",
            source="error",
            row_count=0,
            message=_safe_error_message("Query execution failed", table_name, exc),
            frame=pd.DataFrame(),
        )
    finally:
        connection.close()


def get_table_snapshot(table_name: str, db_path: str | Path | None = None) -> TableSnapshot:
    """Return status metadata for a single Gold table."""
    connection = open_connection(db_path=db_path)
    try:
        source = _resolve_table_source(connection=connection, table_name=table_name)
        if source.from_sql is None:
            return TableSnapshot(
                table_name=table_name,
                status="missing",
                source=source.source,
                row_count=0,
                message=source.message,
            )

        row_count = int(connection.execute(f"SELECT COUNT(*) FROM {source.from_sql}").fetchone()[0])
        status = "ok" if row_count > 0 else "empty"
        return TableSnapshot(
            table_name=table_name,
            status=status,
            source=source.source,
            row_count=row_count,
            message=source.message,
        )
    except Exception as exc:
        return TableSnapshot(
            table_name=table_name,
            status="error",
            source="error",
            row_count=0,
            message=_safe_error_message("Snapshot failed", table_name, exc),
        )
    finally:
        connection.close()


def get_gold_table_status_frame(
    table_names: Iterable[str] | None = None,
    db_path: str | Path | None = None,
) -> pd.DataFrame:
    """Build a DataFrame summarizing all requested Gold table statuses."""
    selected_tables = list(table_names or GOLD_TABLE_FILES.keys())
    snapshots = [get_table_snapshot(table_name=name, db_path=db_path) for name in selected_tables]
    records = [
        {
            "table_name": snapshot.table_name,
            "status": snapshot.status,
            "source": snapshot.source,
            "row_count": snapshot.row_count,
            "message": snapshot.message,
        }
        for snapshot in snapshots
    ]
    return pd.DataFrame(records)


def resolve_dataset_path(mode: str, project_root: Path | None = None) -> Path:
    """Resolve the parquet path for a row-level dataset explorer mode."""
    relative_path = DATASET_FILES.get(mode)
    if relative_path is None:
        raise ValueError(f"Unsupported dataset mode: {mode}")
    root = project_root or resolve_project_root()
    return (root / relative_path).resolve()


def get_dataset_coverage(mode: str, project_root: Path | None = None) -> QueryResult:
    """Return min/max service date coverage for a dataset explorer mode."""
    dataset_path = resolve_dataset_path(mode=mode, project_root=project_root)
    if not dataset_path.exists():
        return QueryResult(
            table_name=mode,
            status="missing",
            source="missing",
            row_count=0,
            message=f"Dataset parquet not found: {dataset_path.as_posix()}",
            frame=pd.DataFrame(),
        )

    connection = duckdb.connect(":memory:")
    try:
        frame = connection.execute(
            f"""
            SELECT
                MIN(service_date) AS min_service_date,
                MAX(service_date) AS max_service_date,
                COUNT(*) AS row_count
            FROM read_parquet({_sql_literal(dataset_path.as_posix())})
            """
        ).df()
        return QueryResult(
            table_name=mode,
            status="ok",
            source="parquet",
            row_count=int(frame["row_count"].iloc[0]) if not frame.empty else 0,
            message=f"Loaded dataset coverage from parquet: {dataset_path.name}",
            frame=frame,
        )
    except Exception as exc:
        return QueryResult(
            table_name=mode,
            status="error",
            source="error",
            row_count=0,
            message=_safe_error_message("Dataset coverage query failed", mode, exc),
            frame=pd.DataFrame(),
        )
    finally:
        connection.close()


def load_dataset_rows(
    mode: str,
    start_date: str,
    end_date: str,
    limit: int = 1000,
    project_root: Path | None = None,
) -> QueryResult:
    """Load row-level dataset records for the selected mode and date window."""
    dataset_path = resolve_dataset_path(mode=mode, project_root=project_root)
    if not dataset_path.exists():
        return QueryResult(
            table_name=mode,
            status="missing",
            source="missing",
            row_count=0,
            message=f"Dataset parquet not found: {dataset_path.as_posix()}",
            frame=pd.DataFrame(),
        )

    connection = duckdb.connect(":memory:")
    try:
        total_row_count = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM read_parquet({_sql_literal(dataset_path.as_posix())})
                WHERE service_date BETWEEN ? AND ?
                """,
                [start_date, end_date],
            ).fetchone()[0]
        )
        frame = connection.execute(
            f"""
            SELECT *
            FROM read_parquet({_sql_literal(dataset_path.as_posix())})
            WHERE service_date BETWEEN ? AND ?
            ORDER BY service_date ASC, event_ts ASC NULLS LAST, source_row_id ASC NULLS LAST
            LIMIT ?
            """,
            [start_date, end_date, int(limit)],
        ).df()
        status = "ok" if not frame.empty else "empty"
        return QueryResult(
            table_name=mode,
            status=status,
            source="parquet",
            row_count=int(len(frame)),
            message=f"Loaded dataset rows from parquet: {dataset_path.name}",
            frame=frame,
            total_row_count=total_row_count,
        )
    except Exception as exc:
        return QueryResult(
            table_name=mode,
            status="error",
            source="error",
            row_count=0,
            message=_safe_error_message("Dataset row query failed", mode, exc),
            frame=pd.DataFrame(),
        )
    finally:
        connection.close()


__all__ = [
    "DATASET_FILES",
    "GOLD_TABLE_FILES",
    "QueryResult",
    "TableSnapshot",
    "get_dataset_coverage",
    "get_gold_table_status_frame",
    "get_table_snapshot",
    "load_dataset_rows",
    "open_connection",
    "query_table",
    "resolve_dataset_path",
    "resolve_duckdb_path",
    "resolve_project_root",
]
