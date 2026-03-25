"""Project bootstrap and shared ingestion helpers for Step 1."""

from __future__ import annotations

import csv
import fnmatch
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import duckdb
import yaml

HASH_BUFFER_SIZE = 1024 * 1024


@dataclass(frozen=True)
class ProjectPaths:
    """Canonical paths used by ingestion and bronze modules."""

    project_root: Path
    workspace_root: Path
    datasets_root: Path
    raw_root: Path
    bronze_root: Path
    data_root: Path
    configs_root: Path
    docs_root: Path
    logs_root: Path
    db_path: Path


def resolve_project_paths() -> ProjectPaths:
    """Resolve project/workspace roots from this module location."""
    here = Path(__file__).resolve()
    project_root: Path | None = None

    for parent in here.parents:
        if (
            parent.name == "ttc_pulse"
            and (parent / "src").exists()
            and (parent / "raw").exists()
            and (parent / "data").exists()
        ):
            project_root = parent
            break

    if project_root is None:
        raise RuntimeError(f"Could not resolve ttc_pulse project root from {here}")

    workspace_root = project_root.parent
    return ProjectPaths(
        project_root=project_root,
        workspace_root=workspace_root,
        datasets_root=workspace_root / "datasets",
        raw_root=project_root / "raw",
        bronze_root=project_root / "bronze",
        data_root=project_root / "data",
        configs_root=project_root / "configs",
        docs_root=project_root / "docs",
        logs_root=project_root / "logs",
        db_path=project_root / "data" / "ttc_pulse.duckdb",
    )


def ensure_project_layout(paths: ProjectPaths) -> None:
    """Create required folders for Step 1 artifacts."""
    for directory in [
        paths.raw_root / "bus",
        paths.raw_root / "subway",
        paths.raw_root / "gtfs",
        paths.raw_root / "gtfsrt",
        paths.bronze_root,
        paths.data_root,
        paths.configs_root,
        paths.logs_root,
        paths.docs_root,
    ]:
        directory.mkdir(parents=True, exist_ok=True)


def utc_now() -> datetime:
    """UTC timestamp for ingestion metadata."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """UTC timestamp in compact ISO format."""
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_duckdb_connection(db_path: Path) -> duckdb.DuckDBPyConnection:
    """Open DuckDB; recreate file if an invalid placeholder exists."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        try:
            probe = duckdb.connect(str(db_path))
            probe.close()
        except duckdb.IOException:
            db_path.unlink()

    connection = duckdb.connect(str(db_path))
    connection.execute("PRAGMA disable_progress_bar")
    return connection


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML config as a dictionary."""
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Expected mapping at {path}, got {type(loaded).__name__}")
    return loaded


def relative_posix(path: Path, root: Path) -> str:
    """Path relative to root (fallback: absolute path)."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def file_checksum(path: Path, algorithm: str = "sha256") -> str:
    """Streaming checksum for file-level lineage."""
    digest = hashlib.new(algorithm)
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(HASH_BUFFER_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _matches_any_pattern(path: Path, patterns: Iterable[str], root: Path) -> bool:
    relative_value = relative_posix(path, root)
    absolute_value = path.resolve().as_posix()
    name_value = path.name
    for pattern in patterns:
        if (
            fnmatch.fnmatch(relative_value, pattern)
            or fnmatch.fnmatch(name_value, pattern)
            or fnmatch.fnmatch(absolute_value, pattern)
        ):
            return True
    return False


def discover_files(
    source_root: Path,
    include_patterns: Iterable[str],
    exclude_patterns: Iterable[str],
    suffixes: Iterable[str] | None = None,
) -> list[Path]:
    """Find files under source_root using include/exclude glob-like patterns."""
    if not source_root.exists():
        return []

    include = list(include_patterns)
    exclude = list(exclude_patterns)
    suffix_set = {suffix.lower() for suffix in (suffixes or [])}

    discovered: dict[str, Path] = {}
    for pattern in include:
        for path in source_root.rglob(pattern):
            if not path.is_file():
                continue
            if suffix_set and path.suffix.lower() not in suffix_set:
                continue
            if exclude and _matches_any_pattern(path, exclude, source_root):
                continue
            discovered[path.resolve().as_posix()] = path.resolve()
    return [discovered[key] for key in sorted(discovered)]


def ensure_csv_header(path: Path, fieldnames: list[str]) -> None:
    """Create a CSV file with headers when missing/empty."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def append_csv_rows(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> int:
    """Append rows to CSV and create header on first write."""
    ensure_csv_header(path, fieldnames)
    row_count = 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        for row in rows:
            writer.writerow({column: row.get(column) for column in fieldnames})
            row_count += 1
    return row_count


def quote_identifier(name: str) -> str:
    """Safely quote SQL identifiers for DuckDB."""
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    """Safely quote SQL string literals."""
    return "'" + value.replace("'", "''") + "'"


def sql_file_array(paths: Iterable[Path | str]) -> str:
    """Build a DuckDB SQL list literal for file paths."""
    literals = []
    for path in paths:
        text = str(path)
        literals.append(sql_literal(text))
    return "[" + ", ".join(literals) + "]"


def write_log_rows(log_path: Path, rows: Iterable[dict[str, Any]]) -> int:
    """Append ingestion step rows into logs/ingestion_log.csv."""
    fieldnames = [
        "run_id",
        "logged_at",
        "step",
        "status",
        "row_count",
        "details",
    ]
    return append_csv_rows(log_path, fieldnames, rows)
