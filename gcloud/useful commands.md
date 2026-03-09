# ============================================================================
# EMAIL PRICING BOT - USEFUL GCLOUD COMMANDS
# ============================================================================

## ENVIRONMENT SETUP
# Set environment variables (run these first in each new terminal session)
export PROJECT_ID="pricing-email-bot"
export BUCKET_NAME="pricing-email-bot-bucket"
export SA_EMAIL="email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com"
export FUNCTION_URL=$(gcloud functions describe email-pricing-bot --region=asia-south1 --gen2 --format="value(serviceConfig.uri)")

## DEPLOYMENT COMMANDS

# Deploy the function
cd src/
gcloud functions deploy email-pricing-bot \
    --gen2 \
    --runtime=python311 \
    --region=asia-south1 \
    --source=. \
    --entry-point=main \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=2GB \
    --timeout=540s \
    --service-account=$SA_EMAIL \
    --set-env-vars=GCS_BUCKET=$BUCKET_NAME,PROJECT_ID=$PROJECT_ID

# Check deployment status
gcloud functions describe email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --format="value(state,updateTime)"

# View function details
gcloud functions describe email-pricing-bot --region=asia-south1 --gen2

# Check environment variables
gcloud functions describe email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --format="value(serviceConfig.environmentVariables)"

## TESTING COMMANDS

# Test with dry-run (recommended for testing - won't modify state or send emails)
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"dry_run": true, "force_execution": true}'

# Production run (processes emails for real)
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"force_execution": true}'

# Test with curl (alternative method)
TOKEN=$(gcloud auth print-identity-token)
curl -X POST $FUNCTION_URL \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"dry_run": true, "force_execution": true}'

## LOGGING COMMANDS

# View recent logs (last 50 entries)
gcloud functions logs read email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --limit=50

# View logs from specific time (adjust timestamp as needed)
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND timestamp>=\"2025-10-12T13:00:00Z\"" \
    --limit=100 \
    --project=pricing-email-bot

# View only error logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND severity>=ERROR" \
    --limit=20 \
    --project=pricing-email-bot

# View logs with specific message pattern
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.message=~'SUPPLIER'" \
    --limit=50 \
    --project=pricing-email-bot

# View logs in JSON format (full details)
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot" \
    --limit=10 \
    --format=json \
    --project=pricing-email-bot

# View logs in table format
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot" \
    --limit=50 \
    --format="table(timestamp,severity,jsonPayload.message)" \
    --project=pricing-email-bot

# Tail logs in real-time (during execution)
gcloud logging tail "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot" \
    --project=pricing-email-bot

# View logs for specific execution (replace execution_id)
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.execution_id='20251012_133034'" \
    --format=json \
    --project=pricing-email-bot

## CLOUD STORAGE COMMANDS

# List files in bucket
gsutil ls gs://$BUCKET_NAME/

# List config files
gsutil ls gs://$BUCKET_NAME/config/

# View config file
gsutil cat gs://$BUCKET_NAME/config/core_config.json

# View config file formatted
gsutil cat gs://$BUCKET_NAME/config/core_config.json | python3 -m json.tool

# Upload config file
gsutil cp config/core_config.json gs://$BUCKET_NAME/config/core_config.json

# Update delegated user email
gsutil cat gs://$BUCKET_NAME/config/core_config.json > /tmp/core_config.json && python3 -c "
import json
with open('/tmp/core_config.json', 'r') as f:
    config = json.load(f)
print(f'Current delegated user: {config[\"gmail\"][\"delegated_user_email\"]}')
config['gmail']['delegated_user_email'] = 'gopika@ucalexports.com'  # Change this
with open('/tmp/core_config.json', 'w') as f:
    json.dump(config, f, indent=2)
print(f'Updated delegated user to: {config[\"gmail\"][\"delegated_user_email\"]}')
" && gsutil cp /tmp/core_config.json gs://$BUCKET_NAME/config/core_config.json && echo "✅ Config updated"

# View state file
gsutil cat gs://$BUCKET_NAME/state/last_processed.json

# Update state file
echo '{"last_processed_timestamp": "2025-01-01T00:00:00Z", "last_execution_timestamp": "2025-01-01T00:00:00Z"}' > /tmp/state.json
gsutil cp /tmp/state.json gs://$BUCKET_NAME/state/last_processed.json

## CLOUD SCHEDULER COMMANDS

# List scheduled jobs
gcloud scheduler jobs list --location=asia-south1

# View job details
gcloud scheduler jobs describe email-pricing-bot-hourly --location=asia-south1

# Pause scheduled job
gcloud scheduler jobs pause email-pricing-bot-hourly --location=asia-south1

# Resume scheduled job
gcloud scheduler jobs resume email-pricing-bot-hourly --location=asia-south1

# Manually trigger scheduled job
gcloud scheduler jobs run email-pricing-bot-hourly --location=asia-south1

# Update schedule (change to every 2 hours)
gcloud scheduler jobs update http email-pricing-bot-hourly \
    --location=asia-south1 \
    --schedule="0 */2 * * *"

## SERVICE ACCOUNT COMMANDS

# View service account details
gcloud iam service-accounts describe $SA_EMAIL

# Get service account unique ID (for domain-wide delegation)
gcloud iam service-accounts describe $SA_EMAIL --format="value(uniqueId)"

# List service account keys
gcloud iam service-accounts keys list --iam-account=$SA_EMAIL

## USEFUL LOG QUERIES

# View all email processing sessions
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.message=~'PROCESSING.*EMAILS'" \
    --limit=10 \
    --format="value(timestamp,jsonPayload.count)" \
    --project=pricing-email-bot

# View all supplier emails
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.status='supplier'" \
    --limit=50 \
    --format="value(timestamp,jsonPayload.supplier,jsonPayload.from_address,jsonPayload.subject)" \
    --project=pricing-email-bot

# View all unknown domain emails (with count)
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.status='unknown_domain'" \
    --limit=500 \
    --format="value(jsonPayload.from_domain)" \
    --project=pricing-email-bot | sort | uniq -c | sort -rn

# Count emails by status
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.status!=null" \
    --limit=1000 \
    --format="value(jsonPayload.status)" \
    --project=pricing-email-bot | sort | uniq -c

# View emails with attachments
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.total_attachments>0" \
    --limit=50 \
    --format="value(jsonPayload.from_address,jsonPayload.total_attachments,jsonPayload.attachment_files)" \
    --project=pricing-email-bot

## WEB CONSOLE URLS

# Function details
https://console.cloud.google.com/functions/details/asia-south1/email-pricing-bot?project=pricing-email-bot

# Logs viewer
https://console.cloud.google.com/logs/query?project=pricing-email-bot

# Cloud Storage browser
https://console.cloud.google.com/storage/browser/pricing-email-bot-bucket?project=pricing-email-bot

# Cloud Scheduler
https://console.cloud.google.com/cloudscheduler?project=pricing-email-bot

# Service accounts
https://console.cloud.google.com/iam-admin/serviceaccounts?project=pricing-email-bot

## TROUBLESHOOTING COMMANDS

# Check if function is accessible
curl -I $FUNCTION_URL

# Verify IAM permissions
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:$SA_EMAIL"

# Check Cloud Run service
gcloud run services describe email-pricing-bot --region=asia-south1

# View build logs
gcloud builds list --limit=5

# Check function revisions
gcloud run revisions list --service=email-pricing-bot --region=asia-south1