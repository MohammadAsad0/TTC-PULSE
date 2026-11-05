#!/usr/bin/env bash
# Run each query in aws/queries/ on Athena. Save each result as CSV and as a
# Markdown page in aws/results/. Run aws/glue_athena_setup.sh first.
#
# Usage:
#   BUCKET=my-bucket [AWS_REGION=ca-central-1] aws/run_queries.sh
set -euo pipefail

BUCKET="${BUCKET:?Set BUCKET to your S3 bucket name}"
AWS_REGION="${AWS_REGION:-ca-central-1}"
WORKGROUP="${WORKGROUP:-primary}"
DATABASE="ttc_pulse"
RESULTS_LOCATION="s3://$BUCKET/athena-results/"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
QUERY_DIR="$REPO_ROOT/aws/queries"
RESULTS_DIR="$REPO_ROOT/aws/results"

echo "Checking AWS credentials"
aws sts get-caller-identity --query Arn --output text

shopt -s nullglob
query_files=("$QUERY_DIR"/*.sql)
if [ "${#query_files[@]}" -eq 0 ]; then
  echo "No queries found in $QUERY_DIR" >&2
  exit 1
fi

for query_file in "${query_files[@]}"; do
  name="$(basename "$query_file" .sql)"

  query_id=$(aws athena start-query-execution \
    --region "$AWS_REGION" \
    --work-group "$WORKGROUP" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$RESULTS_LOCATION" \
    --query-string "$(cat "$query_file")" \
    --query QueryExecutionId \
    --output text)

  while true; do
    state=$(aws athena get-query-execution \
      --region "$AWS_REGION" \
      --query-execution-id "$query_id" \
      --query QueryExecution.Status.State \
      --output text)
    case "$state" in
      SUCCEEDED|FAILED|CANCELLED) break ;;
    esac
    sleep 2
  done

  if [ "$state" != "SUCCEEDED" ]; then
    reason=$(aws athena get-query-execution \
      --region "$AWS_REGION" \
      --query-execution-id "$query_id" \
      --query QueryExecution.Status.StateChangeReason \
      --output text)
    echo "$name: Athena $state: $reason" >&2
    exit 1
  fi

  bytes_scanned=$(aws athena get-query-execution \
    --region "$AWS_REGION" \
    --query-execution-id "$query_id" \
    --query QueryExecution.Statistics.DataScannedInBytes \
    --output text)

  # Athena writes the result CSV to the results location, named by query id.
  aws s3 cp "s3://$BUCKET/athena-results/$query_id.csv" "$RESULTS_DIR/$name.csv" \
    --region "$AWS_REGION" --only-show-errors
  python3 "$REPO_ROOT/aws/csv_to_markdown.py" \
    "$RESULTS_DIR/$name.csv" "$RESULTS_DIR/$name.md" "$query_file" "$query_id" "$bytes_scanned"

  echo "$name: query $query_id, $bytes_scanned bytes scanned -> aws/results/$name.csv"
done
