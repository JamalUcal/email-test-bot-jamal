# Extending Web Scraper

Technical guide for adding new supplier web scrapers to the Email Pricing Bot system.

## Overview

The web scraper extension allows the pricing bot to automatically download price lists from supplier websites. It supports multiple scraper types and can run both locally (for development) and on Google Cloud Platform (for production).

**Key Features**:
- Multiple scraper types (API client, link downloader, WebDAV, etc.)
- Streaming file-by-file processing
- Duplicate detection before download
- Brand filtering
- State management for resume capability
- Configurable timeouts and scheduling

## Scraper Types

### 1. API Client (`api_client`)

For modern SPAs with REST APIs (e.g., NEOPARTA).

**Characteristics**:
- Uses HTTP requests to list and download files
- Supports authentication via cookies/tokens harvested from browser login
- Can convert JSON responses to Excel format automatically

**Example**: NEOPARTA - Angular SPA with REST API

### 2. Link Downloader (`link_downloader`)

For traditional websites with direct download links (e.g., APF).

**Characteristics**:
- Uses Playwright for navigation and clicking
- Handles form-based authentication
- Identifies download links via CSS selectors

**Example**: APF - Traditional website with download links

### 3. Directory Listing (`directory_listing`)

For open directory structures (e.g., CONNEX).

**Characteristics**:
- Uses HTTP Basic Auth and file listing
- Parses HTML directory listings
- Extracts dates from HTML content

**Example**: CONNEX - HTTP Basic Auth with HTML directory listing

### 4. WebDAV (`webdav`)

For WebDAV/Nextcloud-like systems (e.g., Materom).

**Characteristics**:
- Uses WebDAV protocol for file operations
- PROPFIND for directory listing
- Supports custom timeout configurations

**Example**: MATEROM - Nextcloud-based file sharing

### 5. Form Export (`form_export`)

For sites with form-based file generation (e.g., Technoparts).

**Characteristics**:
- Fills forms and triggers downloads
- Uses Playwright for form interaction

**Example**: TECHNOPARTS - Form-based file generation

### 6. Email Trigger (`email_trigger`)

For sites that email files after web action.

**Characteristics**:
- Defers to existing email processing pipeline
- Triggers email generation via web action

## Configuration System

The scraper uses a JSON-based configuration system that works for both local development and GCP deployment:

- **Local**: Credentials stored in environment variables (`.env` file)
- **GCP**: Credentials stored in Secret Manager
- **Config**: Scraper rules and metadata in `scraper_config.json`

### Core Structure

```json
{
  "supplier": "SUPPLIER_NAME",
  "type": "api_client|link_downloader|email_trigger|directory_listing|webdav|form_export",
  "enabled": true,
  "schedule": {
    "frequency": "daily|weekly|monthly",
    "day_of_week": "monday|tuesday|...",
    "time": "HH:MM",
    "timezone": "Asia/Kolkata",
    "detection_mode": "date_based|full_scan"
  },
  "execution": {
    "max_execution_time_seconds": 600,
    "timeout_buffer_seconds": 120,
    "download_timeout_seconds": 300,
    "webdav_list_timeout_seconds": 60
  },
  "authentication": { /* auth config */ },
  "metadata": { /* metadata config */ },
  "config": [
    /* Brand-specific configurations - only brands listed here will be processed */
    {"brand": "BMW"},
    {"brand": "MERCEDES-BENZ"},
    {"brand": "AUDI"}
    /* To disable a brand without removing it, set enabled: false */
    /* {"brand": "VOLKSWAGEN", "enabled": false} */
  ]
}
```

### Execution Timeouts

Control scraper execution and download timeouts:

**`max_execution_time_seconds`** (default: 600)
- Total time budget for the entire scraper run
- Must be less than Cloud Function timeout (540s for Gen 1, 900s for Gen 2)

**`timeout_buffer_seconds`** (default: 120)  
- Safety buffer before max execution time
- Scraper stops processing new files at `max_execution_time - timeout_buffer`

**`download_timeout_seconds`** (default: 300)
- Maximum time to wait for a single file download
- Increase for large files (100MB+)

**`webdav_list_timeout_seconds`** (default: 60)
- Maximum time for WebDAV directory listing
- Only used by WebDAV scrapers (e.g., Materom)

### Authentication Methods

#### Form Authentication
```json
{
  "authentication": {
    "method": "form",
    "login_url": "https://example.com/login",
    "username_field": "input[name='email']",
    "password_field": "input[name='password']", 
    "submit_button": "button[type='submit']",
    "success_indicator": "a[href='/dashboard']",
    "username_env": "SCRAPER_SUPPLIER_USERNAME",
    "password_env": "SCRAPER_SUPPLIER_PASSWORD"
  }
}
```

#### Bearer Token
```json
{
  "authentication": {
    "method": "bearer",
    "token_header": "Authorization",
    "password_env": "SCRAPER_SUPPLIER_API_KEY"
  }
}
```

**Features**:
- No browser required - uses httpx directly
- API key stored in environment variable
- Automatically formats as `Bearer {token}`
- Faster execution (no Playwright overhead)

#### HTTP Basic Auth
```json
{
  "authentication": {
    "method": "http_basic",
    "username_env": "SCRAPER_SUPPLIER_USERNAME",
    "password_env": "SCRAPER_SUPPLIER_PASSWORD"
  }
}
```

#### Token Storage (for form-based auth)
Supports extracting tokens from browser storage:
```json
{
  "authentication": {
    "method": "form",
    "token_storage": {
      "type": "localStorage",
      "path": "loginData.Token",
      "header_name": "Authorization",
      "header_format": "Bearer {token}"
    }
  }
}
```

### Type-Specific Configurations

#### API Client (`api_client`)
```json
{
  "api": {
    "base_url": "https://api.example.com",
    "list_endpoint": "/api/pricelists",
    "list_method": "GET",
    "list_params": { "page": 1, "limit": 100 },
    "list_items_path": "data.items",
    "export_endpoint": "/api/export",
    "export_method": "GET",
    "export_params_template": {
      "id": "{id}",
      "format": "xlsx"
    },
    "headers": {
      "Authorization": "Bearer ${SCRAPER_SUPPLIER_TOKEN}"
    }
  }
}
```

#### Link Downloader (`link_downloader`)
```json
{
  "links": {
    "page_url": "https://example.com/downloads",
    "link_selector": "a.download-link",
    "link_href_pattern": "/download/.*\\.(xlsx|csv|pdf)$",
    "filename_from": "href"
  }
}
```

## Implementation Requirements

### Streaming Implementation (REQUIRED)

All new scrapers MUST use the `scrape_stream()` method for file-by-file processing:

```python
async def scrape_stream(self) -> AsyncIterator[ScrapedFile]:
    """
    Stream files one-by-one for immediate processing.
    This is the REQUIRED method for new scrapers.
    """
    # 1. Authenticate
    await self.authenticate()
    
    # 2. List/discover files
    files = await self.list_files()
    
    # 3. Filter by config array brands
    filtered_files = self._filter_by_brands(files)
    
    # 4. For each file:
    for idx, file_info in enumerate(filtered_files):
        # Check state for duplicates BEFORE downloading
        if self._is_duplicate(file_info):
            continue
        
        # Download if new
        downloaded_file = await self.download_file(file_info)
        
        # yield ScrapedFile
        yield ScrapedFile(
            file_path=downloaded_file.path,
            supplier_filename=file_info.original_filename,
            brand=file_info.brand,
            valid_from_date=file_info.valid_from,
            valid_to_date=file_info.valid_to
        )
```

**Checklist**:
- [ ] Method signature is exactly `async def scrape_stream(self) -> AsyncIterator[ScrapedFile]`
- [ ] Uses `yield` not `return` to stream files
- [ ] Processes files one-by-one (not batch)

### Duplicate Detection (CRITICAL)

Duplicate detection MUST happen BEFORE downloading each file:

```python
# This MUST happen BEFORE downloading each file
supplier_filename = "original_filename_from_supplier.xlsx"
valid_from_date_str = None

if self.state_manager:
    # Extract version/date from filename
    detection_mode = self.config.get('schedule', {}).get('detection_mode', 'date_based')
    version = self.version_detector.detect_version(
        item={'filename': supplier_filename},
        detection_mode=detection_mode
    )
    if version:
        valid_from_date_str = version
        logger.info(f"[VERSION DETECTED] {valid_from_date_str}", filename=supplier_filename)
    
    logger.info(
        f"[DUPLICATE CHECK] File {idx}/{total}: supplier_filename={supplier_filename}, valid_from={valid_from_date_str}",
        supplier_filename=supplier_filename,
        valid_from_date=valid_from_date_str
    )
    
    # Check if already processed
    if supplier_filename and valid_from_date_str:
        if self.state_manager.is_file_already_processed(
            supplier=self.supplier_name,
            supplier_filename=supplier_filename,
            valid_from_date=valid_from_date_str
        ):
            logger.info(f"[DUPLICATE SKIP] Already have: {supplier_filename}")
            continue  # SKIP THIS FILE
        else:
            logger.info(f"[NEW FILE] Not in state: {supplier_filename}")
    elif supplier_filename:
        # Fallback: check by filename only (no date)
        if self.state_manager.is_file_already_processed(
            supplier=self.supplier_name,
            supplier_filename=supplier_filename
        ):
            logger.info(f"[DUPLICATE SKIP] Already have: {supplier_filename} (no date)")
            continue
```

**Checklist**:
- [ ] State check happens **BEFORE** `download_file()` call
- [ ] Uses `self.version_detector.detect_version()` to extract date
- [ ] Logs `[VERSION DETECTED]` if date found
- [ ] Logs `[DUPLICATE CHECK]` for every file
- [ ] Logs `[DUPLICATE SKIP]` when skipping
- [ ] Logs `[NEW FILE]` when downloading
- [ ] Calls `self.state_manager.is_file_already_processed()`
- [ ] Uses `continue` to skip already-processed files
- [ ] Passes `supplier_filename` (original filename) not normalized filename

### Brand Filtering

Brand filtering happens at two levels:

**Layer 1: config array (Brand Selection)**
```json
{
  "config": [
    {"brand": "BMW"},
    {"brand": "MERCEDES-BENZ"},
    {"brand": "AUDI"},
    {"brand": "VW"}
  ]
}
```
- Only brands in the config array are processed
- Brands with `enabled: false` are skipped
- Filters before download to reduce bandwidth
- Applied at link/item enumeration stage
- Case-insensitive matching

**Layer 2: brand_config.json (Validation)**
- Validates against configured brands
- Ensures proper mapping and aliases
- Applied after config array filter

**Implementation**:
```python
def _filter_by_brands(self, items: List[Dict]) -> List[Dict]:
    """Filter items by config array configuration."""
    # Get brands from config array (brands with enabled != false)
    config_brands = self.config.get('config', [])
    enabled = [
        cfg['brand'] 
        for cfg in config_brands 
        if cfg.get('enabled', True)  # enabled by default
    ]
    if not enabled:
        return items
    
    filtered = []
    for item in items:
        brand = self._extract_brand(item)
        if brand and brand.upper() in [b.upper() for b in enabled]:
            filtered.append(item)
        else:
            logger.warning(f"Brand '{brand}' not in config array - skipping")
    
    return filtered
```

## Complete Onboarding Checklist

This checklist ensures complete implementation when adding or fixing supplier scrapers.

### 1. SCRAPER CONFIGURATION

#### Config File Setup
- [ ] Supplier entry exists in `config/scraper/scraper_config.json`
- [ ] Authentication credentials configured (username, password, etc.)
- [ ] **`config` array exists with brand-specific configurations** (CRITICAL - scraper will fail without this)
- [ ] Column mappings defined for **ALL brands** in `config` array
- [ ] `detection_mode` configured (`date_based`, `full_scan`, etc.)
- [ ] Execution timeouts configured (`max_execution_time_seconds`, `download_timeout_seconds`, etc.)

#### Brand Configuration
- [ ] All brands exist in `config/brand/brand_config.json`
- [ ] All brands exist in `config/brand/brand_config_test.json`
- [ ] Brand aliases are defined in config (NOT hardcoded in scraper)
- [ ] Drive folder IDs configured for each brand
- [ ] `minimumPartLength` set appropriately

#### Special Authentication Types

**HTTP Basic Auth (e.g., CONNEX):**
- [ ] `authentication.method` set to `"http_basic"`
- [ ] `authentication.username_env` set to environment variable name (e.g., `"SCRAPER_CONNEX_USERNAME"`)
- [ ] `authentication.password_env` set to environment variable name (e.g., `"SCRAPER_CONNEX_PASSWORD"`)
- [ ] Credentials added to `.env` file locally
- [ ] Credentials configured in Google Cloud Secret Manager for production

**Form-based Auth:**
- [ ] Login URL configured
- [ ] Form selectors defined
- [ ] Success/failure detection logic implemented

#### Input File Types

**CSV Files (e.g., CONNEX):**
- [ ] File format: `"csv"` in scraper type or autodetected
- [ ] CSV encoding: UTF-8 with BOM handling (`utf-8-sig`)
- [ ] Column mappings use 1-based indexing (1 = first column)
- [ ] Header row handling configured (typically skip first row)
- [ ] `_stream_csv_to_csv()` method used in `PriceListParser`

**Excel Files (e.g., TECHNOPARTS, MATEROM):**
- [ ] File format: `"xlsx"` or `"xls"`
- [ ] Column mappings use 1-based indexing (1 = first column)
- [ ] `_stream_excel_to_csv()` method used in `PriceListParser`

**JSON API (e.g., NEOPARTA):**
- [ ] API endpoint configured
- [ ] Field mappings use JSON paths
- [ ] `_stream_json_to_csv()` method used in `PriceListParser`

#### HTML Directory Listings (e.g., CONNEX)

If supplier uses HTML directory listing (not Playwright-rendered links):
- [ ] Date extraction pattern configured for parsing HTML content
- [ ] `_extract_directory_listing_metadata()` method implemented
- [ ] Date format added to `VersionDetector.DATETIME_PATTERNS` if needed
- [ ] Example HTML format documented in scraper comments

**Common HTML patterns**:
```
10/28/2025  9:58 PM        14405 <a href="/path/file.csv">file.csv</a>
```

### 2. SCRAPER IMPLEMENTATION

#### Class Structure
- [ ] Scraper class exists in `src/scrapers/supplier_scrapers/`
- [ ] Inherits from `BaseScraper`
- [ ] Has `__init__` method that calls `super().__init__()`
- [ ] Initializes `self.version_detector = VersionDetector()` (if using version detection)

#### Core Methods Required
- [ ] `async def scrape_stream(self) -> AsyncIterator[ScrapedFile]` (PREFERRED for streaming)
  - OR `async def scrape(self) -> ScrapingResult` (legacy batch mode)
- [ ] `async def authenticate(self) -> bool`
- [ ] `async def download_files(self) -> List[ScrapedFile]` (if needed)

#### Streaming Implementation (REQUIRED)
- [ ] Method signature is exactly `async def scrape_stream(self) -> AsyncIterator[ScrapedFile]`
- [ ] Uses `yield` not `return` to stream files
- [ ] Processes files one-by-one (not batch)

### 3. DUPLICATE DETECTION (CRITICAL)

#### Infrastructure
- [ ] Scraper has `self.state_manager` available (passed in `__init__`)
- [ ] Scraper has `self.version_detector` initialized
- [ ] `ScrapedFile` objects include `supplier_filename` parameter

#### Pre-Download State Check (REQUIRED)
- [ ] State check happens **BEFORE** `download_file()` call
- [ ] Uses `self.version_detector.detect_version()` to extract date
- [ ] Logs `[VERSION DETECTED]` if date found
- [ ] Logs `[DUPLICATE CHECK]` for every file
- [ ] Logs `[DUPLICATE SKIP]` when skipping
- [ ] Logs `[NEW FILE]` when downloading
- [ ] Calls `self.state_manager.is_file_already_processed()`
- [ ] Uses `continue` to skip already-processed files
- [ ] Passes `supplier_filename` (original filename) not normalized filename

### 4. BRAND FILTERING

- [ ] Brand filtering implemented using `config` array
- [ ] Filtering happens before download (not after)
- [ ] Unmatched brands logged as warnings (not errors)
- [ ] Processing continues for other files if one brand doesn't match

### 5. STATE TRACKING

- [ ] `supplier_filename` recorded in state after successful download
- [ ] `valid_from_date` recorded if available
- [ ] `valid_to_date` recorded if available
- [ ] State updated after each file (not just at end)

### 6. ERROR HANDLING

- [ ] Individual file failures don't stop entire scrape
- [ ] Errors logged with context (supplier, brand, filename)
- [ ] Partial failures reported in scraping result
- [ ] Authentication failures handled gracefully

### 7. TESTING

#### First Run (no state):
```bash
python scripts/run_scraper_local.py --supplier [SUPPLIER_NAME] --use-test-config --force
```

Expected:
- [ ] Files discovered and logged
- [ ] Brand filtering works correctly
- [ ] Version detection extracts dates
- [ ] `[NEW FILE]` logs appear
- [ ] Files downloaded, parsed, uploaded to Drive
- [ ] State file updated with `supplier_filename` and `valid_from_date`

#### Second Run (with state):
```bash
python scripts/run_scraper_local.py --supplier [SUPPLIER_NAME] --use-test-config --force
```

Expected:
- [ ] `[DUPLICATE SKIP]` logs appear
- [ ] **NO downloads happen**
- [ ] **NO processing happens**
- [ ] Completes quickly

#### Linting:
```bash
# Check for linter errors
read_lints(paths=["src/scrapers/supplier_scrapers/[supplier]_scraper.py"])
read_lints(paths=["config/scraper/scraper_config.json"])
```

Expected:
- [ ] No linter errors in scraper file
- [ ] No linter errors in modified files
- [ ] All type hints correct

## Examples and Templates

### Supplier Onboarding Template

When adding a new supplier, use this template to gather information:

```markdown
# New Supplier Onboarding Request

I need help adding a new web scraper for supplier **[SUPPLIER_NAME]**.

## Supplier Details

### Basic Info
- **Supplier Name**: [SUPPLIER_NAME]
- **Website/Portal URL**: [URL]
- **Authentication Type**: [Form login / HTTP Basic Auth / Cookie-based / API Key / None]
- **File Type**: [Excel (.xlsx) / CSV (.csv) / JSON API / Other]
- **Scraper Type**: [link_downloader / api_client / custom / webdav]

### Authentication Details
Username: [ENV_VAR_NAME]
Password: [ENV_VAR_NAME]
Login URL: [if form-based]
Login Flow: [describe steps if complex]

### File Discovery
How to find files:
- [ ] Click links on a page
- [ ] Parse HTML directory listing
- [ ] API endpoint that lists files
- [ ] WebDAV PROPFIND
- [ ] Other: [describe]

Link/File Pattern: [e.g., "*.xlsx", "a[href$='.csv']"]
Date/Version Detection: [From filename / From page metadata / From file content / None]
Example Date Format: [e.g., "October 2025", "2025-10", "10/28/2025 9:58 PM"]

### File Structure
# Example filename(s):
BMW - Export Price list October.xlsx

# Example file content (first 5 rows):
Reference | Description | Price EUR
12345 | BRAKE PAD | 45.50

# Column mappings needed:
Part Number: Column [X]
Description: Column [X] or "null" if none
Price: Column [X]
Former Part Number: Column [X] or "null"
Supersede Part Number: Column [X] or "null"

### Brand Configuration
Enabled Brands: [BMW, FORD, HONDA, etc.]

Brand Detection:
- [ ] From filename (pattern: [regex pattern])
- [ ] From file content (which field?)
- [ ] Static (one brand per file)

### Metadata
Location: [ROMANIA / ITALY / GERMANY / etc.]
Currency: [EUR / USD / RON / etc.]
Decimal Format: [decimal (12.34) / comma (12,34) / indian (12,34,567.89)]
Default Expiry Days: [90 / 180 / etc.]
```

### Example: NEOPARTA (API Client)

**Website**: https://selfservice.neoparta.com  
**Type**: Angular SPA with REST API  
**Authentication**: Form-based login (harvests cookies for API)

```json
{
  "supplier": "NEOPARTA",
  "type": "api_client",
  "enabled": true,
  "schedule": {
    "frequency": "weekly",
    "day_of_week": "monday", 
    "time": "09:00",
    "timezone": "Asia/Kolkata",
    "detection_mode": "date_based"
  },
  "authentication": {
    "method": "form",
    "login_url": "https://selfservice.neoparta.com/login",
    "username_field": "input[formcontrolname='username']",
    "password_field": "input[formcontrolname='password']",
    "submit_button": "button[type='submit']",
    "success_indicator": "a[href='/search-parts']",
    "username_env": "SCRAPER_NEOPARTA_USERNAME",
    "password_env": "SCRAPER_NEOPARTA_PASSWORD",
    "token_storage": {
      "type": "localStorage",
      "path": "loginData.Token",
      "header_name": "Authorization",
      "header_format": "Bearer {token}"
    }
  },
  "api": {
    "base_url": "https://selfservice-backend.neoparta.com",
    "list_endpoint": "/api/Pricings/getGenuineSegmentsPriceList",
    "list_method": "GET",
    "list_params": { "PerPage": 25, "page": 1 },
    "list_items_path": "Data",
    "export_endpoint": "/api/Pricings/exportByBrandId",
    "export_method": "GET",
    "export_params_template": {
      "brandId": "{BrandId}",
      "segmentId": "{SegmentId}"
    }
  },
  "metadata": {
    "brand_field": "Brand",
    "valid_from_field": "ValidFrom",
    "valid_to_field": "ValidTo",
    "default_expiry_days": 90
  },
  "config": [
    {
      "brand": "BMW",
      "location": ["LITHUANIA"],
      "currency": ["EUR"],
      "decimalFormat": "decimal"
    },
    {
      "brand": "TOYOTA",
      "location": ["LITHUANIA"],
      "currency": ["EUR"],
      "decimalFormat": "decimal"
    },
    {
      "brand": "LEXUS",
      "location": ["LITHUANIA"],
      "currency": ["EUR"],
      "decimalFormat": "decimal"
    },
    {
      "brand": "MERCEDES-BENZ",
      "location": ["LITHUANIA"],
      "currency": ["EUR"],
      "decimalFormat": "decimal"
    },
    {
      "brand": "AUDI",
      "location": ["LITHUANIA"],
      "currency": ["EUR"],
      "decimalFormat": "decimal"
    }
  ]
}
```

### Example: APF (Link Downloader)

**Website**: https://wiuse.net  
**Type**: Traditional website with download links  
**Authentication**: Form-based login

```json
{
  "supplier": "APF",
  "type": "link_downloader", 
  "enabled": true,
  "schedule": {
    "frequency": "weekly",
    "day_of_week": "monday",
    "time": "09:30", 
    "timezone": "Asia/Kolkata",
    "detection_mode": "date_based"
  },
  "authentication": {
    "method": "form",
    "login_url": "https://wiuse.net/customer/login",
    "username_field": "input[name='Email']",
    "password_field": "input[name='Password']",
    "submit_button": "button[type='submit']"
  },
  "links": {
    "page_url": "https://wiuse.net/pricelist",
    "link_selector": "a.download-btn",
    "link_href_pattern": "/pricelist/Download\\?brandCode=.*",
    "filename_from": "href"
  },
  "metadata": {
    "brand_from_url": "brandCode",
    "location": "BELGIUM",
    "currency": "EUR",
    "decimalFormat": "comma",
    "default_expiry_days": 90
  },
  "config": [
    {
      "brand": "BMW"
      // location, currency, decimalFormat inherited from top-level
    },
    {
      "brand": "MERCEDES-BENZ"
    },
    {
      "brand": "AUDI"
    }
  ]
}
```

## Local Development Setup

### Prerequisites

```bash
# Install Python dependencies
pip install -r src/requirements.txt

# Install Playwright browsers
playwright install chromium
```

### Environment Variables

Create a `.env` file in the project root (this file is gitignored):

```bash
# NEOPARTA credentials
SCRAPER_NEOPARTA_USERNAME="your@email.com"
SCRAPER_NEOPARTA_PASSWORD="yourpassword"

# APF credentials  
SCRAPER_APF_USERNAME="your@email.com"
SCRAPER_APF_PASSWORD="yourpassword"

# Optional: Override default paths
SCRAPER_DOWNLOAD_DIR="./.local_downloads"
SCRAPER_SCREENSHOT_DIR="./.scraper_artifacts"
```

### Running Locally

```bash
# Dry run (no actual downloads)
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# Full run with downloads
python scripts/run_scraper_local.py --supplier NEOPARTA

# Run with visible browser (for debugging)
python scripts/run_scraper_local.py --supplier APF --headful

# Run all enabled suppliers
python scripts/run_scraper_local.py --all
```

## GCP Deployment Setup

### Secret Manager Setup

Store credentials in Secret Manager for all scrapers:

```bash
# Create secrets for each supplier (replace with actual credentials)
echo -n "your-username" | gcloud secrets create scraper-supplier-username --data-file=-
echo -n "your-password" | gcloud secrets create scraper-supplier-password --data-file=-

# Grant service account access to ALL secrets
for secret in scraper-supplier-username scraper-supplier-password; do
    gcloud secrets add-iam-policy-binding $secret \
        --member="serviceAccount:email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com" \
        --role="roles/secretmanager.secretAccessor"
done
```

**Note**: 
- Use `echo -n` (no newline) to avoid trailing newline in secrets
- Secret names use lowercase-hyphen format (e.g., `scraper-autocar-username`)
- Environment variables use uppercase-underscore format (e.g., `SCRAPER_AUTOCAR_USERNAME`)
- Cloud Run/Cloud Functions automatically maps secret names to env vars

### Configuration Upload

```bash
# Upload scraper config to GCS
gsutil cp config/scraper/scraper_config.json gs://pricing-email-bot-bucket/config/scraper/scraper_config.json
```

### Service Account Permissions

Ensure the service account has:
- `roles/secretmanager.secretAccessor` - Read credentials
- `roles/storage.objectAdmin` - Read/write configs and state
- `roles/run.invoker` - Invoke Cloud Functions (if using)

## Testing and Debugging

### Local Testing

```bash
# Test specific supplier with dry run
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# Test with visible browser for debugging
python scripts/run_scraper_local.py --supplier APF --headful

# Test all suppliers
python scripts/run_scraper_local.py --all --dry-run
```

### Debugging Tips

1. **Screenshots**: Automatically saved to `.scraper_artifacts/SUPPLIER/`
2. **Downloads**: Saved to `.local_downloads/SUPPLIER/`
3. **Logs**: Detailed logging with structured JSON output
4. **Headful Mode**: Use `--headful` to see browser interactions

### Common Issues

#### Authentication Failures
- Check credentials in `.env` file
- Verify login URL and field selectors
- Test login manually in browser

#### API Errors
- Check API endpoints and parameters
- Verify authentication tokens/cookies
- Test API calls with curl/Postman

#### Download Failures
- Check link selectors and patterns
- Verify file permissions
- Check network connectivity

#### Duplicate Detection Not Working
- Verify state file is being updated
- Check `supplier_filename` is being passed correctly
- Verify `valid_from_date` extraction logic
- Check state file location (test vs production)

## Production Deployment

### Cloud Function Deployment

```bash
# Deploy with scraper support
cd src/
gcloud functions deploy email-pricing-bot \
    --gen2 \
    --runtime=python311 \
    --region=asia-south1 \
    --source=. \
    --entry-point=main \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=4GB \
    --timeout=900s \
    --service-account=email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com \
    --set-env-vars=GCS_BUCKET=pricing-email-bot-bucket,PROJECT_ID=pricing-email-bot
```

### Cloud Scheduler Setup

The system uses a single Cloud Scheduler job that triggers every hour. The application intelligently determines which scrapers should run based on their individual schedules. See [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md) for scheduling configuration details.

## Security Considerations

### Credential Management

- **Local**: Use `.env` file (gitignored)
- **GCP**: Store in Secret Manager
- **Never**: Hardcode credentials in config files

### Access Control

- Service account has minimal required permissions
- Scraper configs stored in private GCS bucket
- Authentication tokens rotated regularly

### Network Security

- All HTTPS connections
- No sensitive data in logs
- Screenshots excluded from production

## Maintenance

### Adding New Suppliers

1. Analyze website structure
2. Choose appropriate scraper type
3. Create configuration in `scraper_config.json`
4. Add credentials to Secret Manager
5. Test locally with `--dry-run`
6. Deploy and schedule

### Updating Configurations

1. Update `scraper_config.json`
2. Upload to GCS: `gsutil cp config/scraper/scraper_config.json gs://bucket/config/scraper/scraper_config.json`
3. Restart Cloud Function if needed

### Monitoring

- Check logs for authentication failures
- Monitor download success rates
- Track file processing metrics
- Set up alerts for critical failures

## Related Documentation

- [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md) - System architecture and design decisions
- [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md) - Setup and operational procedures

