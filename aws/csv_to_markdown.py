"""Write an Athena result CSV as a Markdown page with the SQL and the run details.

Called by aws/run_queries.sh:
    python3 aws/csv_to_markdown.py RESULT.csv RESULT.md QUERY.sql QUERY_ID BYTES_SCANNED
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def main() -> None:
    csv_path, md_path, sql_path = (Path(arg) for arg in sys.argv[1:4])
    query_id, bytes_scanned = sys.argv[4], int(sys.argv[5])

    with csv_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.reader(handle))
    header, body = rows[0], rows[1:]

    lines = [
        f"# {md_path.stem}",
        "",
        f"Athena query id: `{query_id}`",
        "",
        f"Data scanned: {bytes_scanned:,} bytes",
        "",
        "```sql",
        sql_path.read_text(encoding="utf-8").rstrip(),
        "```",
        "",
        "| " + " | ".join(header) + " |",
        "| " + " | ".join("---" for _ in header) + " |",
    ]
    lines += ["| " + " | ".join(row) + " |" for row in body]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
