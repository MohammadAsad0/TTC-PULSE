"""Raw registry ingestion for static GTFS source files."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import (
    append_csv_rows,
    ensure_csv_header,
    file_checksum,
    relative_posix,
    resolve_project_paths,
)

GTFS_REQUIRED_FILE_CANDIDATES = {
    "routes": ["routes.txt", "routes.csv", "csv/routes.csv"],
    "trips": ["trips.txt", "trips.csv", "csv/trips.csv"],
    "stop_times": ["stop_times.txt", "stop_times.csv", "csv/stop_times.csv"],
    "stops": ["stops.txt", "stops.csv", "csv/stops.csv"],
    "calendar": ["calendar.txt", "calendar.csv", "csv/calendar.csv"],
    "calendar_dates": ["calendar_dates.txt", "calendar_dates.csv", "csv/calendar_dates.csv"],
}

GTFS_OPTIONAL_FILE_CANDIDATES = {
    "shapes": ["shapes.txt", "shapes.csv", "csv/shapes.csv"],
}

REGISTRY_COLUMNS = [
    "ingest_run_id",
    "ingested_at",
    "source_dataset",
    "gtfs_table_name",
    "source_path",
    "source_rel_path",
    "file_name",
    "file_extension",
    "file_size_bytes",
    "file_modified_at",
    "file_checksum_sha256",
]


def _ensure_registry_table(connection: Any, table_name: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ingest_run_id VARCHAR,
            ingested_at TIMESTAMP,
            source_dataset VARCHAR,
            gtfs_table_name VARCHAR,
            source_path VARCHAR,
            source_rel_path VARCHAR,
            file_name VARCHAR,
            file_extension VARCHAR,
            file_size_bytes BIGINT,
            file_modified_at TIMESTAMP,
            file_checksum_sha256 VARCHAR
        )
        """
    )


def _pick_first_existing(base_root: Path, relative_candidates: list[str]) -> Path | None:
    for relative_path in relative_candidates:
        candidate = (base_root / relative_path).resolve()
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def discover_gtfs_files() -> dict[str, Any]:
    """Resolve required and optional GTFS files from data/gtfs (legacy fallback supported)."""
    paths = resolve_project_paths()
    source_root = paths.data_root / "gtfs"
    if not source_root.exists():
        source_root = paths.datasets_root / "01_gtfs_merged"

    required: dict[str, Path] = {}
    missing_required: list[str] = []
    for table_name, candidates in GTFS_REQUIRED_FILE_CANDIDATES.items():
        selected = _pick_first_existing(source_root, candidates)
        if selected is None:
            missing_required.append(table_name)
        else:
            required[table_name] = selected

    optional: dict[str, Path] = {}
    for table_name, candidates in GTFS_OPTIONAL_FILE_CANDIDATES.items():
        selected = _pick_first_existing(source_root, candidates)
        if selected is not None:
            optional[table_name] = selected

    all_tables = {**required, **optional}
    return {
        "source_root": source_root.resolve().as_posix(),
        "required_files": {key: value.resolve().as_posix() for key, value in required.items()},
        "optional_files": {key: value.resolve().as_posix() for key, value in optional.items()},
        "missing_required_tables": missing_required,
        "all_table_files": all_tables,
        "registry_table": "raw_gtfs_file_registry",
    }


def ingest_gtfs_registry(connection: Any, run_id: str, ingested_at: str) -> dict[str, Any]:
    """Register static GTFS files into raw registry storage."""
    paths = resolve_project_paths()
    discovered = discover_gtfs_files()
    registry_table = discovered["registry_table"]
    all_table_files: dict[str, Path] = discovered["all_table_files"]

    rows: list[dict[str, Any]] = []
    for table_name, path in sorted(all_table_files.items()):
        stat = path.stat()
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rows.append(
            {
                "ingest_run_id": run_id,
                "ingested_at": ingested_at,
                "source_dataset": "gtfs_static",
                "gtfs_table_name": table_name,
                "source_path": path.resolve().as_posix(),
                "source_rel_path": relative_posix(path, paths.workspace_root),
                "file_name": path.name,
                "file_extension": path.suffix.lower(),
                "file_size_bytes": stat.st_size,
                "file_modified_at": modified_at,
                "file_checksum_sha256": file_checksum(path),
            }
        )

    registry_csv_path = paths.raw_root / "gtfs" / "gtfs_file_registry.csv"
    if rows:
        appended_rows = append_csv_rows(registry_csv_path, REGISTRY_COLUMNS, rows)
    else:
        ensure_csv_header(registry_csv_path, REGISTRY_COLUMNS)
        appended_rows = 0

    _ensure_registry_table(connection, registry_table)
    if rows:
        insert_sql = f"""
            INSERT INTO {registry_table} (
                ingest_run_id,
                ingested_at,
                source_dataset,
                gtfs_table_name,
                source_path,
                source_rel_path,
                file_name,
                file_extension,
                file_size_bytes,
                file_modified_at,
                file_checksum_sha256
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        connection.executemany(
            insert_sql,
            [
                (
                    row["ingest_run_id"],
                    row["ingested_at"],
                    row["source_dataset"],
                    row["gtfs_table_name"],
                    row["source_path"],
                    row["source_rel_path"],
                    row["file_name"],
                    row["file_extension"],
                    row["file_size_bytes"],
                    row["file_modified_at"],
                    row["file_checksum_sha256"],
                )
                for row in rows
            ],
        )

    table_row_count = connection.execute(
        f"SELECT COUNT(*) FROM {registry_table}"
    ).fetchone()[0]
    return {
        "source_name": "gtfs",
        "source_root": discovered["source_root"],
        "registry_table": registry_table,
        "registry_csv_path": registry_csv_path.resolve().as_posix(),
        "required_files": discovered["required_files"],
        "optional_files": discovered["optional_files"],
        "missing_required_tables": discovered["missing_required_tables"],
        "table_files": {table: path.as_posix() for table, path in all_table_files.items()},
        "discovered_files": len(all_table_files),
        "appended_registry_rows": appended_rows,
        "registry_table_row_count": table_row_count,
    }
