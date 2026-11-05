#!/usr/bin/env bash
# Upload the gold marts to S3. Each table gets its own folder, because Athena
# reads one folder as one table.
#
# Usage:
#   BUCKET=my-bucket [PREFIX=ttc-pulse/gold] [AWS_REGION=ca-central-1] aws/publish_gold_to_s3.sh
set -euo pipefail

BUCKET="${BUCKET:?Set BUCKET to your S3 bucket name}"
PREFIX="${PREFIX:-ttc-pulse/gold}"
AWS_REGION="${AWS_REGION:-ca-central-1}"
PREFIX="${PREFIX%/}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GOLD_DIR="$REPO_ROOT/gold"

echo "Checking AWS credentials"
aws sts get-caller-identity --query Arn --output text

shopt -s nullglob
gold_files=("$GOLD_DIR"/*.parquet)
if [ "${#gold_files[@]}" -eq 0 ]; then
  echo "No gold/*.parquet files found in $GOLD_DIR" >&2
  exit 1
fi

for gold_file in "${gold_files[@]}"; do
  file_name="$(basename "$gold_file")"
  table_name="${file_name%.parquet}"
  destination="s3://$BUCKET/$PREFIX/$table_name/$file_name"
  aws s3 cp "$gold_file" "$destination" --region "$AWS_REGION" --only-show-errors
  echo "uploaded $file_name -> $destination"
done

echo
echo "Objects under s3://$BUCKET/$PREFIX/:"
aws s3 ls "s3://$BUCKET/$PREFIX/" --recursive --human-readable --region "$AWS_REGION"
