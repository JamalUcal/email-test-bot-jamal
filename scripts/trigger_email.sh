#!/bin/bash

# Script to trigger Cloud Function to process a specific email ID
# Usage: ./scripts/trigger_email.sh <EMAIL_ID> [--dry-run]

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if email ID is provided
if [ -z "$1" ]; then
    echo -e "${RED}Error: Email ID is required${NC}"
    echo ""
    echo "Usage: $0 <EMAIL_ID> [--dry-run]"
    echo ""
    echo "Examples:"
    echo "  $0 190bfb3d4e34dd28"
    echo "  $0 190bfb3d4e34dd28 --dry-run"
    echo ""
    exit 1
fi

EMAIL_ID="$1"
DRY_RUN=false

# Check for dry-run flag
if [ "$2" == "--dry-run" ]; then
    DRY_RUN=true
    echo -e "${YELLOW}🔍 Running in DRY RUN mode${NC}"
fi

# Check if FUNCTION_URL is set
if [ -z "$FUNCTION_URL" ]; then
    echo -e "${RED}Error: FUNCTION_URL environment variable is not set${NC}"
    echo ""
    echo "Please set it with:"
    echo "  export FUNCTION_URL=https://your-region-your-project.cloudfunctions.net/email-pricing-bot"
    echo ""
    echo "Or find it with:"
    echo "  gcloud functions describe email-pricing-bot --region=asia-south1 --gen2 --format='value(serviceConfig.uri)'"
    echo ""
    exit 1
fi

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}🚀 Triggering Cloud Function${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${BLUE}Function URL:${NC} $FUNCTION_URL"
echo -e "  ${BLUE}Email ID:${NC} $EMAIL_ID"
echo -e "  ${BLUE}Dry Run:${NC} $DRY_RUN"
echo ""

# Build JSON payload
JSON_PAYLOAD=$(cat <<EOF
{
  "force_execution": true,
  "email_id": "$EMAIL_ID",
  "dry_run": $DRY_RUN
}
EOF
)

echo -e "${YELLOW}📤 Sending request...${NC}"
echo ""

# Make the request
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST "$FUNCTION_URL" \
  -H "Content-Type: application/json" \
  -d "$JSON_PAYLOAD")

# Extract HTTP status code (last line)
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
# Extract response body (everything except last line)
RESPONSE_BODY=$(echo "$RESPONSE" | sed '$d')

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}📥 Response${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Check HTTP status
if [ "$HTTP_CODE" -eq 200 ]; then
    echo -e "${GREEN}✅ HTTP Status: $HTTP_CODE (Success)${NC}"
    echo ""
    echo -e "${GREEN}Response:${NC}"
    echo "$RESPONSE_BODY" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE_BODY"
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✅ Request completed successfully${NC}"
    echo ""
    echo -e "${YELLOW}💡 View logs with:${NC}"
    echo "   gcloud functions logs read email-pricing-bot --region=asia-south1 --gen2 --limit=50"
    echo ""
else
    echo -e "${RED}❌ HTTP Status: $HTTP_CODE (Error)${NC}"
    echo ""
    echo -e "${RED}Response:${NC}"
    echo "$RESPONSE_BODY"
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}❌ Request failed${NC}"
    echo ""
    exit 1
fi
