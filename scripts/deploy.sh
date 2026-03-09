#!/bin/bash

# Email Pricing Bot - Deployment Script
# Usage: ./deploy.sh [--entry-point=web_scraper|main] [--dry-run-test]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Email Pricing Bot - Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Parse command line arguments
ENTRY_POINT="main"  # Default to unified orchestrator
DRY_RUN_TEST=false

for arg in "$@"; do
    case $arg in
        --entry-point=*)
            ENTRY_POINT="${arg#*=}"
            ;;
        --dry-run-test)
            DRY_RUN_TEST=true
            ;;
        *)
            echo -e "${RED}Unknown argument: $arg${NC}"
            echo "Usage: ./deploy.sh [--entry-point=web_scraper|main] [--dry-run-test]"
            exit 1
            ;;
    esac
done

# Validate entry point
if [[ "$ENTRY_POINT" != "web_scraper" && "$ENTRY_POINT" != "main" ]]; then
    echo -e "${RED}Invalid entry point: $ENTRY_POINT${NC}"
    echo "Must be 'web_scraper' or 'main'"
    exit 1
fi

# Set environment variables
export PROJECT_ID="pricing-email-bot"
export BUCKET_NAME="pricing-email-bot-bucket"
export SA_EMAIL="email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com"
export REGION="asia-south1"

echo -e "${YELLOW}Environment:${NC}"
echo "  Project ID: $PROJECT_ID"
echo "  Bucket: $BUCKET_NAME"
echo "  Service Account: $SA_EMAIL"
echo "  Region: $REGION"
echo "  Entry Point: $ENTRY_POINT"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo -e "${RED}ERROR: gcloud CLI not found${NC}"
    echo "Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if logged in
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
    echo -e "${RED}ERROR: Not logged in to gcloud${NC}"
    echo "Run: gcloud auth login"
    exit 1
fi

# Check current project
CURRENT_PROJECT=$(gcloud config get-value project 2>/dev/null)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo -e "${YELLOW}WARNING: Current project is $CURRENT_PROJECT${NC}"
    echo "Setting project to $PROJECT_ID..."
    gcloud config set project $PROJECT_ID
fi

echo -e "${GREEN}✓ Prerequisites checked${NC}"
echo ""

# Navigate to source directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."
SRC_DIR="$PROJECT_ROOT/src"

if [ ! -d "$SRC_DIR" ]; then
    echo -e "${RED}ERROR: Source directory not found: $SRC_DIR${NC}"
    exit 1
fi

cd "$SRC_DIR"
echo -e "${GREEN}✓ Changed to source directory: $SRC_DIR${NC}"
echo ""

# Check if main.py exists
if [ ! -f "main.py" ]; then
    echo -e "${RED}ERROR: main.py not found in $SRC_DIR${NC}"
    exit 1
fi

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}ERROR: requirements.txt not found in $SRC_DIR${NC}"
    exit 1
fi

echo -e "${YELLOW}Deploying Cloud Function...${NC}"
echo "This will take 2-3 minutes..."
echo ""

# Deploy the function
gcloud functions deploy email-pricing-bot \
    --gen2 \
    --region=$REGION \
    --source=. \
    --entry-point=$ENTRY_POINT \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=4GB \
    --timeout=540s \
    --service-account=$SA_EMAIL \
    --set-env-vars=GCS_BUCKET=$BUCKET_NAME,PROJECT_ID=$PROJECT_ID,FUNCTION_TARGET=$ENTRY_POINT

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

# Get function URL
FUNCTION_URL=$(gcloud functions describe email-pricing-bot --region=$REGION --gen2 --format="value(serviceConfig.uri)" 2>/dev/null)

echo -e "${YELLOW}Function Details:${NC}"
echo "  Name: email-pricing-bot"
echo "  Region: $REGION"
echo "  URL: $FUNCTION_URL"
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
    
    gcloud functions call email-pricing-bot \
        --region=$REGION \
        --gen2 \
        --data="$TEST_DATA"
    
    echo ""
    echo -e "${YELLOW}Fetching recent logs...${NC}"
    echo ""
    
    sleep 3  # Wait for logs to be available
    
    gcloud functions logs read email-pricing-bot \
        --region=$REGION \
        --gen2 \
        --limit=30
    
    echo ""
fi

echo ""
echo -e "${GREEN}✓ Deployment complete. Function will be triggered hourly by Cloud Scheduler.${NC}"
echo ""
