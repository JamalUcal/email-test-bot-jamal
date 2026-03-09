#!/bin/bash

# Update and redeploy script with selective deployment options
# Usage: ./update-and-deploy.sh [-all|-config|-code]
#   -all    : Update both config and code (default)
#   -config : Update config files only
#   -code   : Update code only

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Set environment
export PROJECT_ID="pricing-email-bot"
export BUCKET_NAME="pricing-email-bot-bucket"
export REGION="asia-south1"

# Parse flags
DEPLOY_CONFIG=true
DEPLOY_CODE=true

if [ "$1" == "-config" ]; then
    DEPLOY_CODE=false
elif [ "$1" == "-code" ]; then
    DEPLOY_CONFIG=false
elif [ "$1" == "-all" ] || [ -z "$1" ]; then
    # Default: deploy both
    DEPLOY_CONFIG=true
    DEPLOY_CODE=true
else
    echo -e "${RED}Invalid flag: $1${NC}"
    echo "Usage: $0 [-all|-config|-code]"
    exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Update & Redeploy${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Upload config files
if [ "$DEPLOY_CONFIG" = true ]; then
    echo -e "${YELLOW}Uploading config files to GCS...${NC}"
    gsutil cp config/core/core_config.json gs://$BUCKET_NAME/config/core_config.json
    echo -e "${GREEN}✓ core_config.json uploaded${NC}"
    
    gsutil cp config/brand/brand_config.json gs://$BUCKET_NAME/config/brand_config.json
    echo -e "${GREEN}✓ brand_config.json uploaded${NC}"
    
    gsutil cp config/supplier/supplier_config.json gs://$BUCKET_NAME/config/supplier_config.json
    echo -e "${GREEN}✓ supplier_config.json uploaded${NC}"
    echo ""
fi

# Deploy code
if [ "$DEPLOY_CODE" = true ]; then
    echo -e "${YELLOW}Deploying updated code...${NC}"
    cd src/
    gcloud functions deploy email-pricing-bot \
        --gen2 \
        --runtime=python311 \
        --region=$REGION \
        --source=. \
        --entry-point=main \
        --trigger-http \
        --no-allow-unauthenticated \
        --memory=2GB \
        --timeout=540s \
        --service-account=email-pricing-bot@${PROJECT_ID}.iam.gserviceaccount.com \
        --set-env-vars=GCS_BUCKET=$BUCKET_NAME,PROJECT_ID=$PROJECT_ID
    cd ..
    echo ""
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Deployment Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${YELLOW}Test with:${NC}"
echo "  gcloud functions call email-pricing-bot --region=$REGION --gen2 --data='{\"dry_run\": true, \"force_execution\": true}'"
echo ""
