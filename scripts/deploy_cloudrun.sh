#!/bin/bash

# Cloud Run deployment script for Email Pricing Bot with Playwright support
# This replaces Cloud Functions deployment to properly support browser automation
# Usage: ./deploy_cloudrun.sh [--entry-point=email_processor|web_scraper|main] [--dry-run-test]

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="pricing-email-bot"
REGION="asia-south1"
SERVICE_NAME="email-pricing-bot"
SA_EMAIL="email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com"
BUCKET_NAME="pricing-email-bot-bucket"

# Default entry point
ENTRY_POINT="main"
DRY_RUN_TEST=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --entry-point=*)
            ENTRY_POINT="${1#*=}"
            shift
            ;;
        --dry-run-test)
            DRY_RUN_TEST=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Usage: ./deploy_cloudrun.sh [--entry-point=email_processor|web_scraper|main] [--dry-run-test]"
            exit 1
            ;;
    esac
done

# Validate entry point
if [ "$ENTRY_POINT" != "email_processor" ] && [ "$ENTRY_POINT" != "web_scraper" ] && [ "$ENTRY_POINT" != "main" ]; then
    echo -e "${RED}ERROR: entry-point must be 'email_processor', 'web_scraper', or 'main'${NC}"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Email Pricing Bot - Cloud Run Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo "Environment:"
echo "  Project ID: $PROJECT_ID"
echo "  Region: $REGION"
echo "  Service: $SERVICE_NAME"
echo "  Entry Point: $ENTRY_POINT"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}ERROR: gcloud CLI not found${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Prerequisites checked${NC}"
echo ""

# Set project
gcloud config set project $PROJECT_ID

# Navigate to source directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."
SRC_DIR="$PROJECT_ROOT/src"

# Build and push container
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE_NAME}:latest"
echo -e "${YELLOW}Building and pushing container image...${NC}"
echo "This may take 5-10 minutes..."
echo ""

cd "$SRC_DIR"
gcloud builds submit --tag $IMAGE

if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Container build failed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Container built and pushed${NC}"
echo ""

# Deploy to Cloud Run
echo -e "${YELLOW}Deploying to Cloud Run...${NC}"
echo ""

gcloud run deploy $SERVICE_NAME \
    --image=$IMAGE \
    --region=$REGION \
    --platform=managed \
    --service-account=$SA_EMAIL \
    --memory=4Gi \
    --cpu=2 \
    --timeout=3600s \
    --no-allow-unauthenticated \
    --set-env-vars="GCS_BUCKET=${BUCKET_NAME},FUNCTION_TARGET=${ENTRY_POINT}" \
    --update-secrets="\
SCRAPER_AUTOCAR_USERNAME=scraper-autocar-username:latest,\
SCRAPER_AUTOCAR_PASSWORD=scraper-autocar-password:latest,\
SCRAPER_NEOPARTA_USERNAME=scraper-neoparta-username:latest,\
SCRAPER_NEOPARTA_PASSWORD=scraper-neoparta-password:latest,\
SCRAPER_BRECHMANN_USERNAME=scraper-brechmann-username:latest,\
SCRAPER_BRECHMANN_PASSWORD=scraper-brechmann-password:latest,\
SCRAPER_CONNEX_USERNAME=scraper-connex-username:latest,\
SCRAPER_CONNEX_PASSWORD=scraper-connex-password:latest,\
SCRAPER_TECHNOPARTS_USERNAME=scraper-technoparts-username:latest,\
SCRAPER_TECHNOPARTS_PASSWORD=scraper-technoparts-password:latest,\
SCRAPER_MATEROM_USERNAME=scraper-materom-username:latest,\
SCRAPER_MATEROM_PASSWORD=scraper-materom-password:latest" \
    --min-instances=0 \
    --max-instances=1 \
    --concurrency=1

if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}✓ Deployment Successful!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}✗ Deployment Failed${NC}"
    echo -e "${RED}========================================${NC}"
    echo ""
    exit 1
fi

# Get service URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region=$REGION --format="value(status.url)")

echo -e "${YELLOW}Service Details:${NC}"
echo "  Name: $SERVICE_NAME"
echo "  Region: $REGION"
echo "  URL: $SERVICE_URL"
echo ""

echo -e "${GREEN}✓ Deployment complete.${NC}"
echo ""

# Check if dry-run-test flag is provided
if [ "$DRY_RUN_TEST" = true ]; then
    echo -e "${YELLOW}Running dry-run test...${NC}"
    echo ""
    
    # Adjust test payload based on entry point
    if [ "$ENTRY_POINT" == "web_scraper" ]; then
        TEST_DATA='{"scraper_supplier": "AUTOCAR", "dry_run": true, "force_execution": true}'
    else
        TEST_DATA='{"dry_run": true, "force_execution": true}'
    fi
    
    # Invoke Cloud Run service
    curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
         -H "Content-Type: application/json" \
         -d "$TEST_DATA" \
         "$SERVICE_URL"
    
    echo ""
    echo ""
    echo -e "${YELLOW}Fetching recent logs...${NC}"
    echo ""
    
    sleep 3  # Wait for logs to be available
    
    gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME" \
        --limit=30 \
        --format="table(timestamp,severity,jsonPayload.message)"
    
    echo ""
fi

echo ""
echo -e "${YELLOW}Note: Update Cloud Scheduler to trigger this Cloud Run service:${NC}"
echo "  gcloud scheduler jobs update http email-pricing-bot-trigger \\"
echo "    --location=$REGION \\"
echo "    --schedule='0 * * * *' \\"
echo "    --uri='${SERVICE_URL}' \\"
echo "    --http-method=POST \\"
echo "    --oidc-service-account-email=$SA_EMAIL"
echo ""

