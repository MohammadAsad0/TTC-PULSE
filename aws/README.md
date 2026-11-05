# AWS publish kit (S3, Glue, Athena)

This kit publishes the 8 gold Parquet marts to Amazon S3. It then defines them as Athena tables in the Glue Data Catalog and runs four example queries.

**Status: not run on AWS.** The scripts pass a local syntax check and a dry run with a stub `aws` command. No bucket, table or query result exists yet. `aws/results/` stays empty until you run `run_queries.sh`.

## Cost

- The gold marts total 22.0 MB.
- S3 storage for 22 MB costs well under one US cent per month.
- Athena bills per TB scanned, with a 10 MB minimum per query. At $5 per TB, the four queries cost about 0.02 US cents in total. Check the current price for `ca-central-1`.
- The Glue catalog holds 8 table definitions. That is far below the Glue free tier.

## Permissions

`iam_policy.json` gives the minimum set of actions for this kit. Replace these placeholders first:

- `YOUR_BUCKET`: your S3 bucket name
- `REGION`: `ca-central-1` or your region
- `ACCOUNT_ID`: your 12-digit AWS account ID

If you change `PREFIX` from the default `ttc-pulse/gold`, change the `ttc-pulse/*` resource in the policy too. This policy has not been tested against a live account. If a command returns `AccessDenied`, add the action named in the error.

## Steps

Run these from the repository root. Set the same values in every step.

```bash
export BUCKET=YOUR_BUCKET
export AWS_REGION=ca-central-1

# 0. Install the AWS CLI v2, then sign in. Create the bucket once.
aws configure            # or: aws configure sso
aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION" \
  --create-bucket-configuration LocationConstraint="$AWS_REGION"

# 1. Upload the 8 gold marts, one folder per table.
aws/publish_gold_to_s3.sh

# 2. Create the Glue database ttc_pulse and the 8 tables with Athena DDL.
aws/glue_athena_setup.sh

# 3. Run the four queries in aws/queries/. Save the results.
aws/run_queries.sh

# 4. Commit the results as proof. The .gitignore allows aws/results/*.csv.
git add aws/results && git commit -m "Add Athena query results"
```

The DDL in `aws/ddl/` comes from the gold Parquet schemas. Run `python3 aws/generate_ddl.py` again only if a gold schema changes.

To check the queries without AWS, run `python3 aws/check_queries_duckdb.py`. It runs the same SQL on the local Parquet files with DuckDB.

## Teardown

Remove the data and the catalog entries:

```bash
aws s3 rm "s3://$BUCKET/ttc-pulse" --recursive
aws s3 rm "s3://$BUCKET/athena-results" --recursive
aws athena start-query-execution --region "$AWS_REGION" \
  --query-string "DROP DATABASE IF EXISTS ttc_pulse CASCADE" \
  --result-configuration "OutputLocation=s3://$BUCKET/athena-results/"
```

Delete the bucket only if it holds nothing else: `aws s3 rb "s3://$BUCKET"`.
