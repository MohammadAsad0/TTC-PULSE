# Results

This folder is empty until you run `aws/run_queries.sh` against your own AWS account.
That script then writes one CSV and one Markdown page for each query in `aws/queries/`.
No results are committed yet, because the queries have not been run on AWS.

The local check in `aws/check_queries_duckdb.py` runs the same SQL on the gold Parquet files.
Its output is not an Athena result, so it is not saved here.
