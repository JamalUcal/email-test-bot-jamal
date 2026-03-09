#!/bin/bash

# Script to safely switch between test and production configurations
# Usage: ./scripts/switch_config.sh [test|production] [--force]

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

BUCKET_NAME="pricing-email-bot-bucket"
FORCE=false

if [ $# -eq 0 ]; then
    echo -e "${RED}Error: Environment argument required${NC}"
    echo "Usage: ./scripts/switch_config.sh [test|production] [--force]"
    exit 1
fi

ENVIRONMENT=$1

# Parse optional --force flag
if [ "$2" == "--force" ]; then
    FORCE=true
fi

if [ "$ENVIRONMENT" != "test" ] && [ "$ENVIRONMENT" != "production" ]; then
    echo -e "${RED}Error: Environment must be 'test' or 'production'${NC}"
    exit 1
fi

echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}Configuration Environment Switch${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""

# Show current config
echo "Checking current active configuration..."
CURRENT_ENV=$(gsutil cat gs://${BUCKET_NAME}/config/core/core_config.json 2>/dev/null | grep -o '"environment": "[^"]*"' | cut -d'"' -f4 || echo "unknown")
echo -e "Current environment: ${YELLOW}${CURRENT_ENV}${NC}"
echo ""

if [ "$CURRENT_ENV" == "$ENVIRONMENT" ] && [ "$FORCE" == "false" ]; then
    echo -e "${GREEN}✓ Already using ${ENVIRONMENT} configuration${NC}"
    echo -e "${YELLOW}Tip: Use --force to re-upload updated config files${NC}"
    exit 0
fi

if [ "$FORCE" == "true" ]; then
    echo -e "${YELLOW}--force flag detected: Re-uploading ${ENVIRONMENT} configs${NC}"
    echo ""
fi

# Confirm switch
if [ "$CURRENT_ENV" == "$ENVIRONMENT" ]; then
    echo -e "${YELLOW}⚠️  You are about to re-upload ${ENVIRONMENT} configuration files${NC}"
else
    echo -e "${RED}⚠️  WARNING: You are about to switch from ${CURRENT_ENV} to ${ENVIRONMENT}${NC}"
fi
echo ""
if [ "$ENVIRONMENT" == "production" ]; then
    echo "This will:"
    echo "  - Enable ALL scrapers (as defined in scraper_config.json)"
    echo "  - Send emails to PRODUCTION recipients"
    echo "  - Use PRODUCTION state files"
    echo "  - Use DAILY summary mode (aggregated at configured time)"
else
    echo "This will:"
    echo "  - Enable scrapers defined in scraper_config.json"
    echo "  - Send emails to TEST recipients"
    echo "  - Use TEST state files"
    echo "  - Use IMMEDIATE summary mode (email sent after each run)"
fi
echo ""

read -p "Are you sure you want to continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Uploading ${ENVIRONMENT} configurations to GCS..."

if [ "$ENVIRONMENT" == "test" ]; then
    gsutil cp config/core/core_config_test.json gs://${BUCKET_NAME}/config/core/core_config.json
    gsutil cp config/scraper/scraper_config.json gs://${BUCKET_NAME}/config/scraper/scraper_config.json
    gsutil cp config/brand/brand_config_test.json gs://${BUCKET_NAME}/config/brand/brand_config.json
    gsutil cp config/supplier/supplier_config.json gs://${BUCKET_NAME}/config/supplier/supplier_config.json
else
    gsutil cp config/core/core_config_production.json gs://${BUCKET_NAME}/config/core/core_config.json
    gsutil cp config/scraper/scraper_config.json gs://${BUCKET_NAME}/config/scraper/scraper_config.json
    gsutil cp config/brand/brand_config.json gs://${BUCKET_NAME}/config/brand/brand_config.json
    gsutil cp config/supplier/supplier_config.json gs://${BUCKET_NAME}/config/supplier/supplier_config.json
fi

echo ""
echo "Verifying configuration..."
NEW_ENV=$(gsutil cat gs://${BUCKET_NAME}/config/core/core_config.json | grep -o '"environment": "[^"]*"' | cut -d'"' -f4)
SUMMARY_MODE=$(gsutil cat gs://${BUCKET_NAME}/config/core/core_config.json | grep -o '"summary_mode": "[^"]*"' | cut -d'"' -f4)

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}✓ Configuration Switch Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Active environment: ${NEW_ENV}"
echo "Summary mode: ${SUMMARY_MODE}"
echo ""

if [ "$NEW_ENV" != "$ENVIRONMENT" ]; then
    echo -e "${RED}ERROR: Verification failed! Expected ${ENVIRONMENT} but got ${NEW_ENV}${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Verified: ${ENVIRONMENT} configuration is now active${NC}"
echo ""

