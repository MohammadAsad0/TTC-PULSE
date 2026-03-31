"""Raw registry ingestion for GTFS-RT snapshot artifacts."""

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

REGISTRY_COLUMNS = [
    "ingest_run_id",
    "ingested_at",
    "source_dataset",
    "snapshot_source_dir",
    "source_path",
    "source_rel_path",
    "file_name",
    "file_extension",
    "file_size_bytes",
    "file_modified_at",
    "file_checksum_sha256",
]

SNAPSHOT_SUFFIXES = {".pb", ".bin", ".json", ".jsonl"}


def _ensure_registry_table(connection: Any, table_name: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ingest_run_id VARCHAR,
            ingested_at TIMESTAMP,
            source_dataset VARCHAR,
            snapshot_source_dir VARCHAR,
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


def discover_gtfsrt_snapshots() -> dict[str, Any]:
    """Find GTFS-RT snapshot files from known project locations."""
    paths = resolve_project_paths()
    candidate_roots = [
        paths.project_root / "alerts" / "raw_snapshots",
        paths.workspace_root / "eda" / "gtfsrt_eda_outputs" / "raw",
    ]

    files: list[Path] = []
    source_lookup: dict[str, str] = {}
    for root in candidate_roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            if path.suffix.lower() not in SNAPSHOT_SUFFIXES:
                continue
            resolved = path.resolve()
            files.append(resolved)
            source_lookup[resolved.as_posix()] = root.resolve().as_posix()

    unique_files = sorted({path.as_posix(): path for path in files}.values(), key=lambda p: p.as_posix())
    return {
        "candidate_roots": [root.resolve().as_posix() for root in candidate_roots],
        "files": unique_files,
        "source_lookup": source_lookup,
        "registry_table": "raw_gtfsrt_snapshot_registry",
    }


def register_gtfsrt_snapshots(connection: Any, run_id: str, ingested_at: str) -> dict[str, Any]:
    """Register GTFS-RT snapshot artifacts into raw registry storage."""
    paths = resolve_project_paths()
    discovered = discover_gtfsrt_snapshots()
    registry_table = discovered["registry_table"]
    files: list[Path] = discovered["files"]
    source_lookup: dict[str, str] = discovered["source_lookup"]

    rows: list[dict[str, Any]] = []
    for path in files:
        stat = path.stat()
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        rows.append(
            {
                "ingest_run_id": run_id,
                "ingested_at": ingested_at,
                "source_dataset": "gtfsrt_snapshots",
                "snapshot_source_dir": source_lookup.get(path.as_posix(), ""),
                "source_path": path.as_posix(),
                "source_rel_path": relative_posix(path, paths.workspace_root),
                "file_name": path.name,
                "file_extension": path.suffix.lower(),
                "file_size_bytes": stat.st_size,
                "file_modified_at": modified_at,
                "file_checksum_sha256": file_checksum(path),
            }
        )

    registry_csv_path = paths.raw_root / "gtfsrt" / "gtfsrt_snapshot_registry.csv"
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
                snapshot_source_dir,
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
                    row["snapshot_source_dir"],
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
        "source_name": "gtfsrt",
        "candidate_roots": discovered["candidate_roots"],
        "registry_table": registry_table,
        "registry_csv_path": registry_csv_path.resolve().as_posix(),
        "files": [path.as_posix() for path in files],
        "discovered_files": len(files),
        "appended_registry_rows": appended_rows,
        "registry_table_row_count": table_row_count,
    }
