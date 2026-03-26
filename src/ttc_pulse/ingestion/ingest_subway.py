"""Raw registry ingestion for TTC subway delay sources."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ttc_pulse.utils.project_setup import (
    append_csv_rows,
    discover_files,
    ensure_csv_header,
    file_checksum,
    load_yaml,
    relative_posix,
    resolve_project_paths,
)

REGISTRY_COLUMNS = [
    "ingest_run_id",
    "ingested_at",
    "source_dataset",
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


def _resolve_source_root(config: dict[str, Any], workspace_root: Path, project_root: Path) -> Path:
    configured = Path(str(config.get("source_root", "datasets/03_subway_delay/csv")))
    if configured.is_absolute():
        return configured

    project_candidate = (project_root / configured).resolve()
    if project_candidate.exists():
        return project_candidate
    return (workspace_root / configured).resolve()


def discover_subway_files(config_path: Path | None = None) -> dict[str, Any]:
    """Find subway CSV files from schema config."""
    paths = resolve_project_paths()
    schema_path = config_path or (paths.configs_root / "schema_subway.yml")
    config = load_yaml(schema_path)
    source_root = _resolve_source_root(config, paths.workspace_root, paths.project_root)

    files = discover_files(
        source_root=source_root,
        include_patterns=config.get("include_patterns", ["*.csv"]),
        exclude_patterns=config.get("exclude_patterns", []),
        suffixes=config.get("file_suffixes", [".csv"]),
    )
    return {
        "config_path": schema_path.resolve().as_posix(),
        "source_root": source_root.resolve().as_posix(),
        "registry_table": str(config.get("raw_registry_table", "raw_subway_file_registry")),
        "files": [path.resolve() for path in files],
    }


def ingest_subway_registry(
    connection: Any,
    run_id: str,
    ingested_at: str,
    config_path: Path | None = None,
) -> dict[str, Any]:
    """Register subway source files into raw registry storage."""
    paths = resolve_project_paths()
    discovered = discover_subway_files(config_path=config_path)
    registry_table = discovered["registry_table"]
    files: list[Path] = discovered["files"]

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
                "source_dataset": "subway_delay",
                "source_path": path.resolve().as_posix(),
                "source_rel_path": relative_posix(path, paths.workspace_root),
                "file_name": path.name,
                "file_extension": path.suffix.lower(),
                "file_size_bytes": stat.st_size,
                "file_modified_at": modified_at,
                "file_checksum_sha256": file_checksum(path),
            }
        )

    registry_csv_path = paths.raw_root / "subway" / "subway_file_registry.csv"
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
                source_path,
                source_rel_path,
                file_name,
                file_extension,
                file_size_bytes,
                file_modified_at,
                file_checksum_sha256
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        connection.executemany(
            insert_sql,
            [
                (
                    row["ingest_run_id"],
                    row["ingested_at"],
                    row["source_dataset"],
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
        "source_name": "subway",
        "source_root": discovered["source_root"],
        "config_path": discovered["config_path"],
        "registry_table": registry_table,
        "registry_csv_path": registry_csv_path.resolve().as_posix(),
        "files": [path.as_posix() for path in files],
        "discovered_files": len(files),
        "appended_registry_rows": appended_rows,
        "registry_table_row_count": table_row_count,
    }
