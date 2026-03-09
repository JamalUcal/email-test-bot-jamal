#!/bin/bash
# Create BigQuery scheduled query for SCD Type 2 merge
# 
# This script creates a scheduled query that runs the merge_pending_to_canonical
# stored procedure daily at 2:00 AM to merge staging data into canonical_prices.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - BigQuery Data Transfer API enabled
#   - Service account with BigQuery Admin permissions
#
# Usage:
#   ./04_create_scheduled_query.sh [project_id] [location]
#
# Example:
#   ./04_create_scheduled_query.sh pricing-email-bot asia-south1

set -e

PROJECT_ID="${1:-pricing-email-bot}"
LOCATION="${2:-asia-south1}"
DATASET_ID="PRICING"
SCHEDULE_NAME="scd_type2_merge_nightly"
SCHEDULE_CRON="0 2 * * *"  # Daily at 2:00 AM

echo "Creating scheduled query for SCD Type 2 merge..."
echo "  Project: $PROJECT_ID"
echo "  Location: $LOCATION"
echo "  Dataset: $DATASET_ID"
echo "  Schedule: $SCHEDULE_CRON"
echo ""

# Check if the scheduled query already exists
EXISTING=$(bq ls --transfer_config --transfer_location="$LOCATION" --project_id="$PROJECT_ID" 2>/dev/null | grep "$SCHEDULE_NAME" || true)

if [ -n "$EXISTING" ]; then
    echo "Scheduled query '$SCHEDULE_NAME' already exists. Skipping creation."
    echo "To update, delete the existing query first using the BigQuery console."
    exit 0
fi

# Create the scheduled query
# Note: bq mk --transfer_config requires the BigQuery Data Transfer API to be enabled
bq mk \
    --transfer_config \
    --project_id="$PROJECT_ID" \
    --data_source="scheduled_query" \
    --target_dataset="$DATASET_ID" \
    --display_name="$SCHEDULE_NAME" \
    --schedule="every day 02:00" \
    --location="$LOCATION" \
    --params='{
        "query": "CALL `'"$PROJECT_ID"'.'"$DATASET_ID"'.merge_pending_to_canonical`()"
    }'

echo ""
echo "Scheduled query created successfully!"
echo ""
echo "To verify, run:"
echo "  bq ls --transfer_config --transfer_location=$LOCATION --project_id=$PROJECT_ID"
echo ""
echo "To run manually:"
echo "  bq query --use_legacy_sql=false 'CALL \`$PROJECT_ID.$DATASET_ID.merge_pending_to_canonical\`()'"
