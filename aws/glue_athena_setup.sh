#!/usr/bin/env bash
# Create the Glue database ttc_pulse and the 8 gold tables. Athena writes each
# table to the Glue Data Catalog, so no crawler or crawler role is needed.
# Run aws/publish_gold_to_s3.sh first. The tables point at that S3 location.
#
# Usage:
#   BUCKET=my-bucket [PREFIX=ttc-pulse/gold] [AWS_REGION=ca-central-1] aws/glue_athena_setup.sh
set -euo pipefail

BUCKET="${BUCKET:?Set BUCKET to your S3 bucket name}"
PREFIX="${PREFIX:-ttc-pulse/gold}"
AWS_REGION="${AWS_REGION:-ca-central-1}"
WORKGROUP="${WORKGROUP:-primary}"
PREFIX="${PREFIX%/}"
DATABASE="ttc_pulse"
RESULTS_LOCATION="s3://$BUCKET/athena-results/"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DDL_DIR="$REPO_ROOT/aws/ddl"

# run_athena SQL [DATABASE]
# Runs one statement and waits for it. Returns 1 if Athena reports FAILED or CANCELLED.
run_athena() {
  local sql="$1"
  local context_database="${2:-}"
  local query_id state reason

  if [ -n "$context_database" ]; then
    query_id=$(aws athena start-query-execution \
      --region "$AWS_REGION" \
      --work-group "$WORKGROUP" \
      --result-configuration "OutputLocation=$RESULTS_LOCATION" \
      --query-execution-context "Database=$context_database" \
      --query-string "$sql" \
      --query QueryExecutionId \
      --output text)
  else
    query_id=$(aws athena start-query-execution \
      --region "$AWS_REGION" \
      --work-group "$WORKGROUP" \
      --result-configuration "OutputLocation=$RESULTS_LOCATION" \
      --query-string "$sql" \
      --query QueryExecutionId \
      --output text)
  fi

  while true; do
    state=$(aws athena get-query-execution \
      --region "$AWS_REGION" \
      --query-execution-id "$query_id" \
      --query QueryExecution.Status.State \
      --output text)
    case "$state" in
      SUCCEEDED)
        return 0
        ;;
      FAILED|CANCELLED)
        reason=$(aws athena get-query-execution \
          --region "$AWS_REGION" \
          --query-execution-id "$query_id" \
          --query QueryExecution.Status.StateChangeReason \
          --output text)
        echo "Athena query $query_id $state: $reason" >&2
        return 1
        ;;
    esac
    sleep 2
  done
}

echo "Checking AWS credentials"
aws sts get-caller-identity --query Arn --output text

shopt -s nullglob
ddl_files=("$DDL_DIR"/*.sql)
if [ "${#ddl_files[@]}" -eq 0 ]; then
  echo "No DDL files found in $DDL_DIR. Run: python3 aws/generate_ddl.py" >&2
  exit 1
fi

echo "Creating database $DATABASE"
run_athena "CREATE DATABASE IF NOT EXISTS $DATABASE"
echo "database $DATABASE is ready"

for ddl_file in "${ddl_files[@]}"; do
  table_name="$(basename "$ddl_file" .sql)"
  sql="$(sed -e "s|__BUCKET__|$BUCKET|g" -e "s|__PREFIX__|$PREFIX|g" "$ddl_file")"
  run_athena "$sql" "$DATABASE"
  echo "table $DATABASE.$table_name is ready"
done

echo "All ${#ddl_files[@]} tables are in $DATABASE"
