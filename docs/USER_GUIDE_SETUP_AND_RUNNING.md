# User Guide: Setup and Running

Complete guide for setting up and running the Email Pricing Bot locally and on Google Cloud Platform.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [GCP Infrastructure Setup](#gcp-infrastructure-setup)
3. [Local Development Setup](#local-development-setup)
4. [Configuration Files](#configuration-files)
5. [Deployment](#deployment)
6. [Running the System](#running-the-system)
7. [Scheduling](#scheduling)
8. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
9. [Quick Reference](#quick-reference)

## Prerequisites

- Google Cloud Project with billing enabled
- Google Workspace domain with admin access
- `gcloud` CLI installed and authenticated
- Project owner or editor permissions
- Python 3.11+ (for local development)

## GCP Infrastructure Setup

### Step 1: Enable Required APIs

```bash
# Set your project ID
export PROJECT_ID="pricing-email-bot"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable gmail.googleapis.com
gcloud services enable drive.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable cloudscheduler.googleapis.com
gcloud services enable secretmanager.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable iam.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable cloudbuild.googleapis.com
```

### Step 2: Create Service Account

```bash
# Create service account
gcloud iam service-accounts create email-pricing-bot \
    --display-name="Email Pricing Bot Service Account" \
    --description="Service account for automated email processing"

# Get the service account email
export SA_EMAIL="email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com"
echo "Service Account Email: $SA_EMAIL"

# Create and download service account key
gcloud iam service-accounts keys create $HOME/email-pricing-bot-key.json \
    --iam-account=$SA_EMAIL

echo "Service account key saved to $HOME/email-pricing-bot-key.json"
echo "⚠️  Keep this file secure and never commit it to version control!"
```

### Step 3: Configure IAM Permissions

The application uses a **dedicated service account** that needs specific IAM roles to function. This section explains the permission model and how to configure it.

#### 3.1 Understanding the Service Account Architecture

Your application uses **two service accounts** working together:

| Service Account | Purpose |
|----------------|---------|
| `email-pricing-bot@PROJECT_ID.iam.gserviceaccount.com` | **Runtime SA** - Used by Cloud Run/Cloud Functions to access GCP resources |
| SA key stored in Secret Manager (`email-pricing-bot-sa-key`) | **Application SA** - Used by Python code for Gmail/Drive APIs with domain-wide delegation |

In most setups, these are the **same service account** - the runtime SA retrieves its own key from Secret Manager to use for APIs that require explicit credentials (Gmail, Drive with impersonation).

#### 3.2 Grant Required IAM Roles

The service account needs these roles to function:

| Role | Purpose |
|------|---------|
| `roles/secretmanager.secretAccessor` | Read SA key and scraper credentials from Secret Manager |
| `roles/storage.objectAdmin` | Read/write GCS bucket (config, state, temp files) |
| `roles/bigquery.dataEditor` | Read/write BigQuery tables |
| `roles/bigquery.jobUser` | Run BigQuery queries and jobs |
| `roles/run.invoker` | Invoke Cloud Run services (if using Cloud Run) |

**Grant all required permissions:**

```bash
export PROJECT_ID="pricing-email-bot"
export SA_EMAIL="email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com"

# Secret Manager access (for SA key and scraper credentials)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/secretmanager.secretAccessor"

# GCS bucket access (for config, state, and temp files)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectAdmin"

# BigQuery data access (for price list reconciliation)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

# BigQuery job execution (required to run queries)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

# Cloud Run invoker (if using Cloud Run deployment)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker"

echo "✓ IAM permissions granted to $SA_EMAIL"
```

#### 3.3 Verify IAM Permissions

Always verify permissions are correctly assigned:

```bash
# List all roles assigned to the service account
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$SA_EMAIL" \
  --format="table(bindings.role)"
```

**Expected output:**
```
ROLE
roles/bigquery.dataEditor
roles/bigquery.jobUser
roles/run.invoker
roles/secretmanager.secretAccessor
roles/storage.objectAdmin
```

If the output is **empty**, the service account has no permissions and the application will fail with `403 Access Denied` errors.

#### 3.4 Verify Service Account Exists

```bash
# Check the service account exists and is enabled
gcloud iam service-accounts describe $SA_EMAIL --project=$PROJECT_ID
```

**Expected output includes:**
```
disabled: false
email: email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com
```

#### 3.5 Troubleshooting Permission Errors

**Common Error: `403 Access Denied: User does not have bigquery.jobs.create permission`**

This means the service account lacks BigQuery permissions. Fix with:
```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"
```

**Common Error: `Permission denied on resource project`**

Verify the service account exists and you're using the correct email:
```bash
gcloud iam service-accounts list --project=$PROJECT_ID
```

**Note on Default Service Accounts:**

GCP automatically creates default service accounts (e.g., `PROJECT_NUMBER-compute@developer.gserviceaccount.com`, `PROJECT_ID@appspot.gserviceaccount.com`). These are **NOT used by our application**. You can safely reduce their permissions or disable them if not used by other services.

### Step 4: Configure Domain-Wide Delegation

#### 4.1 Get Service Account Unique ID

```bash
# Get the service account's unique ID
gcloud iam service-accounts describe $SA_EMAIL \
    --format="value(uniqueId)"
```

#### 4.2 Configure in Google Workspace Admin Console

1. Go to [Google Workspace Admin Console](https://admin.google.com) (must be signed in as super administrator)
2. Navigate to: **Menu** → **Security** → **Access and data control** → **API controls** → **Manage Domain Wide Delegation**
   - Direct link: https://admin.google.com/ac/owl/domainwidedelegation
3. Click **Add new**
4. Enter the **Client ID** (unique ID from previous step)
5. Add the following OAuth Scopes:
   ```
   https://www.googleapis.com/auth/gmail.readonly
   https://www.googleapis.com/auth/gmail.send
   https://www.googleapis.com/auth/drive.file
   ```
6. Click **Authorize**

#### 4.3 Note the Delegated User Email

Choose a user account that has access to the Google Group mailbox. This will be the email address the service account impersonates.

```bash
export DELEGATED_USER="pricing@ucalexports.com"
```

### Step 5: Create GCS Bucket for Configuration and State

```bash
# Create bucket (use a globally unique name)
export BUCKET_NAME="${PROJECT_ID}-bucket"
gsutil mb -l asia-south1 gs://$BUCKET_NAME

# Grant service account access
gsutil iam ch serviceAccount:$SA_EMAIL:objectAdmin gs://$BUCKET_NAME

echo "GCS Bucket created: gs://$BUCKET_NAME"
echo "Note: Folder structure (config/, state/, temp/) will be created automatically when files are uploaded"
```

### Step 6: Store Service Account Key in Secret Manager

```bash
# Create secret from the service account key file
gcloud secrets create email-pricing-bot-sa-key \
    --data-file=$HOME/email-pricing-bot-key.json \
    --replication-policy="automatic"

# Grant service account access to the secret
gcloud secrets add-iam-policy-binding email-pricing-bot-sa-key \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/secretmanager.secretAccessor"

echo "Service account key stored in Secret Manager"
echo "Secret name: email-pricing-bot-sa-key"

# Securely delete the local key file (optional, but recommended)
# shred -u ~/email-pricing-bot-key.json  # Linux
# rm -P ~/email-pricing-bot-key.json     # macOS
```

### Step 7: Create Initial Configuration Files

#### 7.1 Create Core Config

Create `core_config.json` with the following structure:

```json
{
  "version": "1.0.0",
  "environment": "production",
  "gmail": {
    "delegated_user_email": "pricing-group@yourdomain.com",
    "own_domain": "yourdomain.com"
  },
  "gcp": {
    "project_id": "your-project-id",
    "secret_name": "email-pricing-bot-sa-key",
    "bucket_name": "your-project-id-email-pricing-bot",
    "state_file": "state/last_processed.json"
  },
  "execution": {
    "schedule_hour": 9,
    "schedule_minute": 0,
    "timezone": "Asia/Kolkata"
  },
  "defaults": {
    "expiry_duration_days": 90
  },
  "notification": {
    "summary_email_recipients": [
      "team@yourdomain.com"
    ],
    "summary_from_email": "pricing@yourdomain.com",
    "summary_mode": "daily",
    "daily_summary_hour": 19,
    "daily_summary_minute": 0
  },
  "drive": {
    "impersonation_email": "gopika@ucalexports.com"
  }
}
```

#### 7.2 Upload Config to GCS

```bash
# Upload core config
gsutil cp config/core/core_config.json gs://$BUCKET_NAME/config/core/core_config.json

# Upload supplier config (after merging individual files)
python config/supplier/merge_suppliers.py
gsutil cp config/supplier/supplier_config.json gs://$BUCKET_NAME/config/supplier/supplier_config.json

# Upload brand config
gsutil cp config/brand/brand_config.json gs://$BUCKET_NAME/config/brand/brand_config.json

# Upload scraper config
gsutil cp config/scraper/scraper_config.json gs://$BUCKET_NAME/config/scraper/scraper_config.json

# Upload column mapping config
gsutil cp config/core/column_mapping_config.json gs://$BUCKET_NAME/config/core/column_mapping_config.json
```

### Step 8: Set Up Google Drive Folders

#### 8.1 Create Brand Folders

For each brand in your `brand_config.json`, create a folder in Google Drive and note the folder ID.

**To get folder ID**:
1. Open the folder in Google Drive
2. The URL will look like: `https://drive.google.com/drive/folders/FOLDER_ID_HERE`
3. Copy the `FOLDER_ID_HERE` part

#### 8.2 Grant Access to Folders

**Important**: With domain-wide delegation, the service account accesses Drive **as the delegated user**. Therefore, share folders with the **delegated user**, not the service account.

For each folder:
1. Right-click folder → Share
2. Add the **delegated user email**: `pricing@ucalexports.com` (or your configured delegated user)
3. Set permission to **Editor**
4. Click Share

**Note**: The service account (`email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com`) does NOT need direct access to the folders. It will access them by impersonating the delegated user.

#### 8.3 Update Brand Config

Update `brand_config.json` with the correct folder IDs and re-upload:

```bash
gsutil cp config/brand/brand_config.json gs://$BUCKET_NAME/config/brand/brand_config.json
```

### Step 9: Deploy Cloud Function

```bash
# Navigate to source directory
cd src/

# Deploy function
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

echo "Cloud Function deployed!"
echo "Note: Function requires authentication. Cloud Scheduler will use service account to invoke it."
```

### Step 10: Create Cloud Scheduler Job

```bash
# Get the function URL
export FUNCTION_URL=$(gcloud functions describe email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --format="value(serviceConfig.uri)")

# Create scheduler job (runs every hour)
# Note: --oidc-service-account-email provides authentication to invoke the function
gcloud scheduler jobs create http email-pricing-bot-hourly \
    --location=asia-south1 \
    --schedule="0 * * * *" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --oidc-service-account-email=$SA_EMAIL \
    --oidc-token-audience=$FUNCTION_URL \
    --time-zone="Asia/Kolkata" \
    --description="Triggers email pricing bot every hour"

echo "Cloud Scheduler job created!"
echo "Function URL: $FUNCTION_URL"
```

**Security Note**: The function is secured with authentication. Only requests with valid OIDC tokens from the service account can invoke it. Cloud Scheduler automatically generates these tokens.

### Step 11: Initialize State File

```bash
# Create initial state file
echo '{
  "last_processed_timestamp": "2025-01-01T00:00:00Z",
  "last_execution_timestamp": "2025-01-01T00:00:00Z",
  "suppliers": {},
  "pending_results": [],
  "version": "1.0.0"
}' > last_processed.json

# Upload to GCS
gsutil cp last_processed.json gs://$BUCKET_NAME/state/last_processed.json

# Clean up local file
rm last_processed.json
```

### Step 12: Set Up Scraper Credentials (If Using Web Scrapers)

Store credentials in Secret Manager for all scrapers:

```bash
# Create secrets for each supplier (replace with actual credentials)
# Example: NEOPARTA
echo -n "your@email.com" | gcloud secrets create scraper-neoparta-username --data-file=-
echo -n "yourpassword" | gcloud secrets create scraper-neoparta-password --data-file=-

# Grant service account access to secrets
for secret in scraper-neoparta-username scraper-neoparta-password; do
    gcloud secrets add-iam-policy-binding $secret \
        --member="serviceAccount:$SA_EMAIL" \
        --role="roles/secretmanager.secretAccessor"
done
```

**Note**: 
- Use `echo -n` (no newline) to avoid trailing newline in secrets
- Secret names use lowercase-hyphen format (e.g., `scraper-neoparta-username`)
- Environment variables use uppercase-underscore format (e.g., `SCRAPER_NEOPARTA_USERNAME`)

### Step 13: Test the Setup

#### Manual Test Trigger

**Option 1: Using gcloud (Recommended)**
```bash
# Test with dry-run mode (won't send emails or modify state)
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"dry_run": true, "force_execution": true}'
```

**Option 2: Using curl with authentication**
```bash
# Get identity token
TOKEN=$(gcloud auth print-identity-token)

# Trigger with dry-run
curl -X POST $FUNCTION_URL \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"dry_run": true, "force_execution": true}'
```

**Note**: Both methods require you to be authenticated with gcloud. The function will reject unauthenticated requests.

#### Check Logs

```bash
# View function logs
gcloud functions logs read email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --limit=50
```

### Step 14: Set Up BigQuery (For Price List Reconciliation)

BigQuery is used for supersession reconciliation of price lists and historical price tracking using an SCD Type 2 data model.

#### 14.1 Create BigQuery Dataset and Tables

```bash
# Navigate to the bigquery scripts directory
cd scripts/bigquery/

# Create the dataset (run in BigQuery console or via bq command)
bq mk --dataset --location=asia-south1 pricing-email-bot:PRICING

# Or run the SQL script
bq query --use_legacy_sql=false < 01_create_dataset.sql
```

#### 14.2 Create Tables

```bash
# Create tables (run in BigQuery console)
bq query --use_legacy_sql=false < 02_create_tables.sql
```

This creates:
- `price_lists` - Metadata for each uploaded file
- `canonical_prices` - SCD Type 2 price history table
- `processing_errors` - Error logging

#### 14.3 Create Stored Procedures

```bash
# Create the reconciliation stored procedures
bq query --use_legacy_sql=false < 03_create_stored_procedure.sql
```

This creates two procedures:
- `reconcile_supersessions_staging` - Runs during file processing (de-duplication + supersession chain resolution)
- `merge_pending_to_canonical` - Runs nightly to merge staging data into canonical tables

#### 14.4 Create Scheduled Query (Nightly Merge)

The scheduled query runs the `merge_pending_to_canonical` procedure nightly at 2:00 AM to consolidate staging data into the `canonical_prices` table.

```bash
# Create the scheduled query
./04_create_scheduled_query.sh
```

Or manually create via BigQuery console:
1. Go to BigQuery → Scheduled Queries → Create
2. Query: `CALL PRICING.merge_pending_to_canonical()`
3. Schedule: Daily at 02:00 (Asia/Kolkata)
4. Service account: `email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com`

#### 14.5 Verify BigQuery Setup

```bash
# List tables in the dataset
bq ls pricing-email-bot:PRICING

# Expected output:
#   price_lists
#   canonical_prices
#   processing_errors
#   supersession_audit
```

#### 14.6 Manual Merge (Optional)

To manually run the nightly merge procedure:

```bash
bq query --use_legacy_sql=false 'CALL `pricing-email-bot.PRICING.merge_pending_to_canonical`()'
```

#### 14.7 Query Current Prices

```sql
-- Get current active prices for a supplier/brand
SELECT part_number, effective_price, valid_from, status
FROM `pricing-email-bot.PRICING.canonical_prices`
WHERE supplier = 'APF' 
  AND brand = 'VAG' 
  AND status = 'ACTIVE'
ORDER BY part_number;
```

### Step 15: Verify Configuration

Run through this checklist:

- [ ] Service account created
- [ ] **IAM permissions granted** (secretmanager, storage, bigquery, run.invoker)
- [ ] Domain-wide delegation configured with correct OAuth scopes
- [ ] GCS bucket created with proper permissions
- [ ] Service account key stored in Secret Manager
- [ ] Configuration files uploaded to GCS
- [ ] Google Drive folders created and shared with delegated user
- [ ] **BigQuery dataset and tables created**
- [ ] Cloud Function/Cloud Run deployed successfully
- [ ] Cloud Scheduler job created
- [ ] State file initialized
- [ ] Manual test successful

#### Verify IAM Permissions

```bash
# This should list 5 roles - if empty, permissions are missing!
gcloud projects get-iam-policy $PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:$SA_EMAIL" \
  --format="table(bindings.role)"
```

## Local Development Setup

### Prerequisites

```bash
# Install Python dependencies
cd src
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# Install Playwright browsers (for web scrapers)
playwright install chromium
```

### Environment Variables

Create a `.env` file in the project root (this file is gitignored):

```bash
# Google Cloud credentials
GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
GCS_BUCKET="pricing-email-bot-bucket"
PROJECT_ID="pricing-email-bot"

# Scraper credentials (if using web scrapers)
SCRAPER_NEOPARTA_USERNAME="your@email.com"
SCRAPER_NEOPARTA_PASSWORD="yourpassword"
SCRAPER_APF_USERNAME="your@email.com"
SCRAPER_APF_PASSWORD="yourpassword"
```

### Running Locally

#### Email Processing

```bash
# Simple dry-run test (recommended for testing)
python scripts/run_email_local.py --dry-run

# Force execution with dry-run (process all emails)
python scripts/run_email_local.py --force --dry-run

# Process only 5 emails for quick testing
python scripts/run_email_local.py --force --dry-run --max-emails 5

# Test with test config (uses test Drive folders)
python scripts/run_email_local.py --use-test-config --dry-run

# Full run with test config (uploads to test folders, updates state)
python scripts/run_email_local.py --use-test-config --send-summary
```

#### Web Scraping

```bash
# Test specific supplier with dry-run (no Drive upload)
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# Full run with downloads
python scripts/run_scraper_local.py --supplier NEOPARTA

# Run with visible browser (for debugging)
python scripts/run_scraper_local.py --supplier APF --headful

# Run all enabled suppliers
python scripts/run_scraper_local.py --all --dry-run

# Test with test config (uses test Drive folders)
python scripts/run_scraper_local.py --supplier NEOPARTA --force --use-test-config 
```

#### Historical Price Loading

Load archived pricing files from Google Drive into BigQuery for historical analysis.

**Prerequisites**:
- BigQuery tables and stored procedures created (Step 14)
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable set
- Files on Drive must follow standard naming format: `{Brand}_{Supplier}_{Currency}_{Location}_{MMMDD_YYYY}.csv`

```bash
# List all historical files without processing (dry run)
python scripts/load_historical_prices.py --dry-run

# Process specific brand only
python scripts/load_historical_prices.py --brand VAG

# Limit number of files to process
python scripts/load_historical_prices.py --max-files 10

# Resume from previous run (skip already processed)
python scripts/load_historical_prices.py --resume

# Continue processing after errors
python scripts/load_historical_prices.py --continue-on-error

# Combine options
python scripts/load_historical_prices.py --brand BMW --max-files 5 --continue-on-error

# Use test configuration
python scripts/load_historical_prices.py --use-test-config --dry-run
```

**Options**:
| Option | Description |
|--------|-------------|
| `--dry-run` | List files without processing |
| `--brand BRAND` | Process only files for specific brand (e.g., VAG, BMW) |
| `--max-files N` | Maximum number of files to process |
| `--resume` | Resume from tracking file (skip already processed) |
| `--continue-on-error` | Continue processing after errors |
| `--use-test-config` | Use test configurations |
| `--tracking-file PATH` | Custom tracking file path (default: `./state/historical_load_tracking.json`) |
| `--verbose` | Enable verbose logging |

**Tracking File**:

Progress is saved to `./state/historical_load_tracking.json`. Use `--resume` to continue from where you left off after interruption.

**Filename Format**:

Files must follow this naming convention:
```
{Brand}_{Supplier}_{Currency}_{Location}_{MMMDD_YYYY}.csv
```

Examples:
- `VAG_APF_EUR_BELGIUM_SEP18_2025.csv`
- `BMW_MATEROM_EUR_ROMANIA_OCT15_2024.csv`
- `VAG_OIL_YANXIN_USD_CHINA_JAN05_2026.csv` (brand with underscore)

Files with non-standard names are skipped and logged.

## Configuration Files

### Configuration Environment Management

**CRITICAL**: The Cloud Function loads configs from **FIXED PATHS** in GCS:
```
gs://pricing-email-bot-bucket/config/core/core_config.json
gs://pricing-email-bot-bucket/config/scraper/scraper_config.json
gs://pricing-email-bot-bucket/config/brand/brand_config.json
gs://pricing-email-bot-bucket/config/supplier/supplier_config.json
```

**IMPORTANT**: The function does NOT automatically use `*_test.json` or `*_production.json` files.  
**You must EXPLICITLY upload the correct config files to the standard paths.**

### Switching Between Test and Production

**RECOMMENDED: Use the safe switching script:**
```bash
# Switch to test
./scripts/switch_config.sh test

# Switch to production
./scripts/switch_config.sh production
```

The script will:
- Show current environment
- Confirm before switching
- Upload correct configs
- Verify the switch succeeded

#### Manual Method (if needed):

**To Use TEST Configuration:**
```bash
# Upload test configs to the active paths
gsutil cp config/core/core_config_test.json gs://pricing-email-bot-bucket/config/core/core_config.json
gsutil cp config/scraper/scraper_config.json gs://pricing-email-bot-bucket/config/scraper/scraper_config.json
gsutil cp config/brand/brand_config_test.json gs://pricing-email-bot-bucket/config/brand/brand_config.json
gsutil cp config/supplier/supplier_config.json gs://pricing-email-bot-bucket/config/supplier/supplier_config.json
```

**Test config characteristics**:
- `environment: "test"`
- `summary_mode: "immediate"` → sends email after EACH run
- Sends to test email: `robin.ashford@ucalexports.com`
- Only AUTOCAR scraper enabled
- Uses test state file: `state/last_processed_test.json`

**To Use PRODUCTION Configuration:**
```bash
# Upload production configs to the active paths
gsutil cp config/core/core_config_production.json gs://pricing-email-bot-bucket/config/core/core_config.json
gsutil cp config/scraper/scraper_config.json gs://pricing-email-bot-bucket/config/scraper/scraper_config.json
gsutil cp config/brand/brand_config.json gs://pricing-email-bot-bucket/config/brand/brand_config.json
gsutil cp config/supplier/supplier_config.json gs://pricing-email-bot-bucket/config/supplier/supplier_config.json
```

**Production config characteristics**:
- `environment: "production"`
- `summary_mode: "daily"` → sends ONE email per day at configured time
- Sends to production emails: `pricing@ucalexports.com`, `robin.ashford@ucalexports.com`
- All scrapers enabled
- Uses production state file: `state/last_processed.json`

### Verify Active Configuration

**ALWAYS verify which config is active before running:**
```bash
# Check what's currently in GCS
gsutil cat gs://pricing-email-bot-bucket/config/core/core_config.json | grep environment
```

Expected output:
- `"environment": "test"` → Test mode active
- `"environment": "production"` → Production mode active

**DO NOT ASSUME. ALWAYS VERIFY.**

### Configuration File Management

#### Local Files (NOT uploaded to GCS)

The following files are maintained **locally only** for easier editing:
- `config/supplier/suppliers/*.json` - Individual supplier config files (216 files)
- `config/supplier/merge_suppliers.py` - Script to merge individual files

**Workflow**:
1. Edit individual supplier files in `config/supplier/suppliers/`
2. Run merge script: `python config/supplier/merge_suppliers.py`
3. Upload merged file: `gsutil cp config/supplier/supplier_config.json gs://pricing-email-bot-bucket/config/supplier/`

#### GCS Files (uploaded to cloud)

The following files **MUST be uploaded** to GCS for the Cloud Function to work:

**Core Configuration:**
```bash
config/core/core_config.json                  → gs://pricing-email-bot-bucket/config/core/
config/core/column_mapping_config.json        → gs://pricing-email-bot-bucket/config/core/
```

**Supplier Configuration:**
```bash
config/supplier/supplier_config.json          → gs://pricing-email-bot-bucket/config/supplier/
```
**Note:** This is the **merged** file generated from individual supplier files.

**Brand Configuration:**
```bash
config/brand/brand_config.json                → gs://pricing-email-bot-bucket/config/brand/
```

**Scraper Configuration:**
```bash
config/scraper/scraper_config.json            → gs://pricing-email-bot-bucket/config/scraper/
```

#### Quick Upload Commands

```bash
BUCKET="gs://pricing-email-bot-bucket"

# Upload core configs
gsutil cp config/core/core_config.json $BUCKET/config/core/
gsutil cp config/core/column_mapping_config.json $BUCKET/config/core/

# Upload merged supplier config (after running merge_suppliers.py)
gsutil cp config/supplier/supplier_config.json $BUCKET/config/supplier/

# Upload brand config
gsutil cp config/brand/brand_config.json $BUCKET/config/brand/

# Upload scraper config
gsutil cp config/scraper/scraper_config.json $BUCKET/config/scraper/
```

**⚠️ IMPORTANT:** Individual supplier files (`config/supplier/suppliers/*.json`) are **NEVER** uploaded to GCS.

## Deployment

### Pre-Deployment Checklist

Before deploying, verify:
- [ ] Configuration files updated in GCS
- [ ] Test locally if possible
- [ ] Service account has all required permissions
- [ ] Domain-wide delegation configured correctly
- [ ] Drive folders shared with delegated user

### Deployment Steps

#### 1. Set Environment Variables

```bash
export PROJECT_ID="pricing-email-bot"
export BUCKET_NAME="pricing-email-bot-bucket"
export SA_EMAIL="email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com"
```

#### 2. Navigate to Source Directory

```bash
cd /Users/robin/Desktop/UCAL\ Projects/dev/email-pricing-bot/src/
```

#### 3. Deploy the Function

```bash
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
    --set-env-vars=GCS_BUCKET=$BUCKET_NAME,PROJECT_ID=$PROJECT_ID,ENABLE_SCREENSHOTS=false
```

**Expected output**:
- Build starts
- Container image created
- Function deployed
- URL provided

**Deployment time**: ~2-3 minutes

#### 4. Verify Deployment

```bash
gcloud functions describe email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --format="value(state,updateTime)"
```

Should show: `ACTIVE` and recent timestamp

### Testing After Deployment

#### Test 1: Dry Run (Recommended First)

```bash
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"dry_run": true, "force_execution": true}'
```

**What this does**:
- Forces execution (ignores schedule)
- Processes emails but doesn't update state
- Doesn't upload to Drive (dry_run=true)
- Doesn't send summary email

**Check logs**:
```bash
gcloud functions logs read email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --limit=100
```

#### Test 2: Production Run (After Dry Run Success)

```bash
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"force_execution": true}'
```

**What this does**:
- Processes emails for real
- Uploads files to Drive
- Updates state
- Attempts to send summary email

## Running the System

### Production Execution

The system runs automatically via Cloud Scheduler:
- **Trigger**: Every hour (`0 * * * *`)
- **Execution**: Application-level logic determines what to run
- **Email Processing**: Runs on every invocation (multi-pass execution)
- **Web Scrapers**: Run based on individual schedules

### Manual Execution

#### Force Execution (Skip Schedule)

```bash
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"force_execution": true}'
```

#### Dry Run (Test Mode)

```bash
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"dry_run": true, "force_execution": true}'
```

#### Process Specific Email

```bash
./scripts/trigger_email.sh <EMAIL_ID>
```

#### Run Specific Scraper

```bash
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"workflow": "scraping", "supplier": "NEOPARTA", "force_execution": true}'
```

## Scheduling

### Overview

The Email Pricing Bot uses two independent scheduling systems:

1. **Email Processing Scheduling** - Configured in core config, runs hourly
2. **Web Scraper Scheduling** - Per-supplier configuration, supports daily/weekly/monthly schedules

Both systems are triggered by a single **Cloud Scheduler job** that runs **every hour**. The application intelligently determines what work to do based on the current time and schedule configurations.

### Email Processing Schedule

**Configuration Location**: `config/core/core_config.json`

**Section**: `execution`

```json
{
  "execution": {
    "schedule_hour": 18,
    "schedule_minute": 0,
    "timezone": "Asia/Kolkata"
  }
}
```

**Note**: Email processing now runs **hourly** (multi-pass execution) to stay within Cloud Function timeout limits. The `schedule_hour` and `schedule_minute` fields are now only used for the daily summary email time.

### Notification Configuration

**Configuration Location**: `config/core/core_config.json`

**Section**: `notification`

```json
{
  "notification": {
    "summary_email_recipients": ["admin@company.com"],
    "summary_from_email": "pricing@company.com",
    "summary_mode": "daily",
    "daily_summary_hour": 19,
    "daily_summary_minute": 0
  }
}
```

**Summary Modes**:
- **`immediate`**: Send email after each run (for testing)
- **`daily`**: Store results, send one email per day (for production)

### Web Scraper Schedule

**Configuration Location**: `config/scraper/scraper_config.json`

**Per-Supplier Configuration**:
```json
{
  "supplier": "NEOPARTA",
  "enabled": true,
  "schedule": {
    "frequency": "daily",
    "time": "09:00",
    "timezone": "Asia/Kolkata",
    "detection_mode": "date_based"
  }
}
```

**Frequency Types**:
- **`daily`**: Runs once per day at specified time
- **`weekly`**: Runs on specified `day_of_week` (e.g., "monday")
- **`monthly`**: Runs on specified `day_of_month` (e.g., 1)

**Detection Modes**:
- **`date_based`**: Downloads only files with dates newer than last run
- **`full_scan`**: Downloads all files every run (relies on archiving for duplicates)

For detailed scheduling information, see [Scheduling Guide](./SCHEDULING.md) (if still needed for reference).

## Monitoring and Troubleshooting

### View Real-Time Logs

```bash
gcloud logging tail "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot" \
    --project=pricing-email-bot
```

### Check for Errors

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND severity>=ERROR" \
    --limit=20 \
    --project=pricing-email-bot
```

### View Processing Summary

```bash
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=email-pricing-bot AND jsonPayload.message=~'PROCESSING.*COMPLETE'" \
    --limit=5 \
    --format="value(timestamp,jsonPayload.message)" \
    --project=pricing-email-bot
```

### Common Issues & Solutions

#### Authentication Error

**Symptom**: `403 Forbidden` or `Invalid Credentials`

**Solutions**:
- Verify domain-wide delegation is configured
- Check OAuth scopes in Workspace Admin Console
- Ensure service account has correct permissions
- Wait 10-15 minutes for changes to propagate

#### BigQuery Permission Error

**Symptom**: `403 Access Denied: User does not have bigquery.jobs.create permission`

**Cause**: The Cloud Run/Cloud Function service account lacks BigQuery IAM roles.

**Solution**:
```bash
export SA_EMAIL="email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com"

# Grant BigQuery permissions
gcloud projects add-iam-policy-binding pricing-email-bot \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding pricing-email-bot \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/bigquery.dataEditor"

# Verify permissions were added
gcloud projects get-iam-policy pricing-email-bot \
  --flatten="bindings[].members" \
  --filter="bindings.members:$SA_EMAIL" \
  --format="table(bindings.role)"
```

#### Service Account Has No Permissions

**Symptom**: The IAM verification command returns empty results

**Cause**: The service account exists but has no IAM role bindings. This can happen if you used Google's "Reduce Permissions" feature or manually removed roles.

**Solution**: Re-grant all required permissions (see [Step 3: Configure IAM Permissions](#step-3-configure-iam-permissions))

#### Config Not Found

**Symptom**: `FileNotFoundError` or `Config file not found`

**Solutions**:
```bash
# Check bucket access
gsutil ls gs://$BUCKET_NAME/config/

# Verify service account permissions
gsutil iam get gs://$BUCKET_NAME

# Upload missing configs
gsutil cp config/*.json gs://$BUCKET_NAME/config/
```

#### Function Timeout

**Symptom**: Function times out before completing

**Solutions**:
- Increase memory: `--memory=4GB`
- Increase timeout: `--timeout=900s` (15 min max for Gen 2)
- Consider Cloud Run instead of Cloud Functions
- Reduce `max_emails_per_run` in config

#### Drive Upload Failures

**Symptom**: `403: The user does not have sufficient permissions`

**Solutions**:
- Verify Drive scope added to domain-wide delegation
- Check folders are shared with delegated user (not service account)
- Verify `drive.impersonation_email` in core config
- Check folder IDs in brand config

#### Email Skipping

**Symptom**: Emails not being processed

**Solutions**:
- Check state file timestamp format (must have +00:00)
- Verify timezone-aware datetimes
- Check duplicate prevention logic
- Verify Gmail query is correct

#### Scraper Not Running

**Symptom**: Scraper doesn't execute when expected

**Solutions**:
- Check `enabled: true` in scraper config
- Verify schedule configuration (time, timezone, frequency)
- Check if already ran in current period (check state file)
- Use `force_execution: true` to test manually

## Quick Reference

### Key Files & Locations

| What | Where |
|------|-------|
| Main entry point | `/src/main.py` |
| Configuration loader | `/src/config/config_manager.py` |
| State management | `/src/utils/state_manager.py` |
| Logging | `/src/utils/logger.py` |

### Common Commands

#### Deployment
```bash
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
```

#### Testing
```bash
# Dry run
gcloud functions call email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --data='{"dry_run": true, "force_execution": true}'

# View logs
gcloud functions logs read email-pricing-bot \
    --region=asia-south1 \
    --gen2 \
    --limit=50
```

#### Configuration Management
```bash
BUCKET="gs://pricing-email-bot-bucket"

# Upload configs
gsutil cp config/core/core_config.json $BUCKET/config/core/
gsutil cp config/supplier/supplier_config.json $BUCKET/config/supplier/
gsutil cp config/brand/brand_config.json $BUCKET/config/brand/
gsutil cp config/scraper/scraper_config.json $BUCKET/config/scraper/

# View active environment
gsutil cat $BUCKET/config/core/core_config.json | grep environment

# View state
gsutil cat $BUCKET/state/last_processed.json
```

#### State File Management

**Download state from cloud**:
```bash
gsutil cp $BUCKET/state/last_processed.json ./state/last_processed.json
```

**Upload state to cloud** (use with caution!):
```bash
gsutil cp ./state/last_processed.json $BUCKET/state/last_processed.json
```

**Backup state**:
```bash
gsutil cp $BUCKET/state/last_processed.json $BUCKET/state/backups/last_processed_$(date +%Y%m%d_%H%M%S).json
```

**Reset state** (DANGER - forces full reprocessing):
```bash
echo '{"last_processed_timestamp": "2025-01-01T00:00:00Z", "suppliers": {}, "version": "1.0.0"}' | \
  gsutil cp - $BUCKET/state/last_processed.json
```

### Local Development Commands

#### Email Processing
```bash
# Dry run
python scripts/run_email_local.py --dry-run

# Force with dry-run
python scripts/run_email_local.py --force --dry-run

# Test with test config
python scripts/run_email_local.py --use-test-config --dry-run
```

#### Web Scraping
```bash
# Test supplier
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# With screenshots
python scripts/run_scraper_local.py --supplier APF --screenshots

# Test with test config
python scripts/run_scraper_local.py --supplier NEOPARTA --use-test-config --dry-run
```

#### Historical Price Loading
```bash
# List files (dry run)
python scripts/load_historical_prices.py --dry-run

# Process specific brand
python scripts/load_historical_prices.py --brand VAG --max-files 10

# Resume after interruption
python scripts/load_historical_prices.py --resume --continue-on-error

# Check tracking status
cat ./state/historical_load_tracking.json | python -m json.tool | head -50
```

#### BigQuery Operations
```bash
# Manual merge (run nightly procedure)
bq query --use_legacy_sql=false 'CALL `pricing-email-bot.PRICING.merge_pending_to_canonical`()'

# Check pending price lists (awaiting merge)
bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `pricing-email-bot.PRICING.price_lists` WHERE merge_status = "PENDING"'

# View recent price lists
bq query --use_legacy_sql=false 'SELECT price_list_id, supplier, brand, upload_timestamp FROM `pricing-email-bot.PRICING.price_lists` ORDER BY upload_timestamp DESC LIMIT 10'
```

## Related Documentation

- [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md) - System architecture and design decisions
- [Extending Web Scraper](./EXTENDING_WEB_SCRAPER.md) - How to add new supplier scrapers
- [Extending Email Parser](./EXTENDING_EMAIL_PARSER.md) - How to add new email parsing patterns
- [BigQuery Supersession Design](./design/bigquery-supercession-design.md) - Detailed BigQuery schema and reconciliation logic

