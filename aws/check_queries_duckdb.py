"""Run the Athena query files against the local gold Parquet files with DuckDB.

Athena is not needed for this check. Each gold/*.parquet file becomes a view
with the same name as its Athena table, so the query text is the same in both
places. The check proves the SQL runs and gives sensible rows. It does not
replace the Athena results in aws/results/.

Run from the repository root:
    python3 aws/check_queries_duckdb.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = REPO_ROOT / "gold"
QUERY_DIR = Path(__file__).resolve().parent / "queries"
PREVIEW_ROWS = 12


def main() -> None:
    connection = duckdb.connect()
    for parquet_path in sorted(GOLD_DIR.glob("*.parquet")):
        connection.execute(
            f"CREATE VIEW {parquet_path.stem} AS SELECT * FROM read_parquet('{parquet_path.as_posix()}')"
        )
    for query_path in sorted(QUERY_DIR.glob("*.sql")):
        statement = query_path.read_text(encoding="utf-8").strip().rstrip(";")
        result = connection.execute(statement)
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
        print(f"== {query_path.name}: {len(rows)} rows")
        print(" | ".join(columns))
        for row in rows[:PREVIEW_ROWS]:
            print(" | ".join(str(value) for value in row))
        print()


if __name__ == "__main__":
    main()
