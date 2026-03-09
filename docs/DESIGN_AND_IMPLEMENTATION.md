# Design and Implementation

This document describes what we built, why we built it, and how it works.

## Overview and Purpose

The Email Pricing Bot is a secure, low-cost automation system for reading supplier emails sent to a Google Group, parsing attachments, creating data load files on Google Drive, and sending daily summary emails. The system is designed to **avoid a paid Workspace license for automation** by using service accounts with domain-wide delegation.

**Key Benefits:**
- No paid Google Workspace license required (uses service account)
- Handles large files efficiently with streaming (up to 500,000 rows)
- Comprehensive error handling and reporting
- Low cost (~$0.62/month on GCP)
- Configurable without code changes

**Phase 1 Goal**: Parse emails to files on Google Drive  
**Phase 2 Goal**: BigQuery integration with SCD Type 2 data model for price history tracking, supersession reconciliation via stored procedures, and deferred nightly merge to canonical tables

## Architecture & Process Flow

### High-Level Architecture

```text
           Suppliers
               │
               ▼
     pricing-bot@ucalexports.com (Google Group)
               │
               │ Domain-wide delegation via service account
               │ Impersonating: automation@ucalexports.com
               ▼
      Cloud Function / Cloud Run (hourly via Cloud Scheduler)
      ┌─────────────────────────────────────────────┐
      │ 1. Authenticate service account (JSON key)  │
      │    - Stored securely in Secret Manager      │
      │ 2. Impersonate automation@ucalexports.com   │
      │ 3. Read emails from pricing-bot group       │
      │ 4. Identify supplier (3-layer detection):   │
      │    - Layer 1: SUPPLIER: tag in body         │
      │    - Layer 2: Forwarded email parsing       │
      │    - Layer 3: Direct From header            │
      │ 5. Parse attachments (CSV/XLSX)             │
      │ 6. Clean/normalize data                     │
      │ 7. Create Data Load File on Google Drive    │
      │ 8. Upload to BigQuery tables  (PHASE2)      │
      │ 9. Generate daily summary report            │
      │ 10. Send summary email (as group email)     │
      └─────────────────────────────────────────────┘
               │
               ▼
        Recipients (internal stakeholders)
```

### Components

| Component                                   | Purpose                                      | Cost Consideration                   |
| ------------------------------------------- | -------------------------------------------- | ------------------------------------ |
| Google Group                                | Receives supplier emails                     | Already existing; free               |
| Service Account with Domain-Wide Delegation | Automates Gmail API access without paid user | Free (no license)                    |
| Secret Manager                              | Stores JSON key securely                     | ~$0.06/month                         |
| Cloud Function / Cloud Run                  | Executes daily automation                    | Free to a few dollars/month          |
| Cloud Scheduler                             | Triggers function hourly                     | Free (up to 3 jobs)                  |
| BigQuery                                    | Stores parsed supplier data                  | ~$1–5/month depending on data volume |
| Optional Cloud Storage                      | Temporary file storage / config              | Minimal cost                         |

### Process Flow Details

1. **Email Reception**: Suppliers send price lists to the Google Group.

2. **Trigger Automation**: Cloud Scheduler triggers the Cloud Function every hour (`0 * * * *`).

3. **Authentication & Authorization**:
   - Service account fetches JSON key from Secret Manager.
   - Uses **domain-wide delegation** to impersonate `automation@ucalexports.com` user.
   - This user has access to read emails from the `pricing-bot@ucalexports.com` Google Group.

4. **Read & Identify Emails**:
   - Gmail query: `to:pricing-bot@ucalexports.com`
   - Reads all emails received since last run (tracked by `last_processed_timestamp`).
   - Identifies supplier using **three-layer detection** (see [Email Parser Extension Guide](./EXTENDING_EMAIL_PARSER.md)).
   - Picks correct parsing rules from **config file** (stored in GCS).

5. **Parse Attachments**:
   - Supports CSV, XLSX
   - Uses streaming for large files (up to 500,000 rows)
   - Cleans and normalizes data according to supplier-specific rules

6. **Create file on Google Drive**:
   - Creates new .csv file in the appropriate format for the pricing engine
   - Filename format: `Brand_SupplierName_Location_Currency_ValidFromDateMMYY.csv`
   - Uploads to brand-specific Drive folders

7. **Upload to BigQuery**: PHASE 2 ONLY
   - Inserts into supplier-specific tables or unified dataset.
   - Optionally handles deduplication or versioning.

8. **Generate & Send Summary**:
   - Aggregates key metrics (total items, new prices, errors).
   - Sends **daily email summary** from the **group email** using Gmail API with DWD.

9. **Logging & Error Handling**:
   - Logs all steps in Cloud Logging.
   - Sends alerts on parsing failures.

## Design Decisions

### Domain-Wide Delegation

**Why**: Avoids requiring a paid Google Workspace license for automation. Service accounts can impersonate domain users to access Gmail and Drive APIs.

**How**: 
- Service account created in GCP
- Client ID added to Google Workspace Admin Console with OAuth scopes
- Service account impersonates a domain user (e.g., `automation@ucalexports.com`)
- Accesses Gmail and Drive as that user

**OAuth Scopes Required**:
- `https://www.googleapis.com/auth/gmail.readonly` - Read emails
- `https://www.googleapis.com/auth/gmail.send` - Send summary emails
- `https://www.googleapis.com/auth/drive.file` - Upload files to Drive

### Streaming Architecture

**Why**: Original implementation loaded entire files into memory, causing 2-3GB peak usage for large price lists (600k+ rows).

**How**: Implemented end-to-end streaming pipeline:
- **Input Streaming**: Row-by-row reading for CSV, read-only mode for Excel
- **Output Streaming**: Direct write to CSV without DataFrame accumulation
- **Results**: Memory reduced from 2-3GB to 100MB (96% reduction)

**Files**: `src/parsers/price_list_parser.py`, `src/output/file_generator.py`

### Multi-Pass Execution

**Why**: Cloud Function timeout limits (540s for Gen 1, 3600s for Gen 2) require splitting long-running processes across multiple executions.

**How**:
- Cloud Scheduler triggers function every hour
- Email processing resumes from `last_processed_timestamp`
- Web scrapers resume from `last_file_index` if interrupted
- State updated after each run to prevent duplicates

**Benefits**:
- No timeouts for large email batches
- Better resource usage
- Graceful degradation with automatic resumption

### State Management

**Why**: Need to track processing progress and prevent duplicate processing.

**How**:
- State stored in GCS bucket (`state/last_processed.json`)
- Shared between email processor and web scraper
- Tracks:
  - `last_processed_timestamp`: Last email processed
  - `last_execution_timestamp`: Last overall execution
  - `suppliers.*`: Per-supplier scraper state
  - `pending_results`: Batched results for daily summaries

**Duplicate Prevention**:
- Email processing: Two-stage filtering (Gmail date query + code timestamp filter)
- Web scraping: Date-based duplicate detection or full scan with archiving

### Daily Summary Emails

**Why**: Production teams need consolidated reporting, not hourly spam.

**How**:
- **Immediate mode** (`summary_mode: "immediate"`): Send after each run (testing)
- **Daily mode** (`summary_mode: "daily"`): Store results, send one email per day
- Results aggregated from all hourly runs
- Single comprehensive email at configured time

## Implementation Details

### Memory Optimization (Streaming)

**Problem**: Original implementation loaded entire files into memory, causing 2-3GB peak usage for large price lists (600k+ rows).

**Solution**: Implemented end-to-end streaming pipeline:

**Input Streaming (CSV/Excel)**:
```python
# CSV: Row-by-row reading
with open(input_file, 'r') as f:
    reader = csv.reader(f)
    for row in reader:
        item = parse_row(row)
        items.append(item)

# Excel: openpyxl read-only mode
wb = load_workbook(filename=file_path, read_only=True, data_only=True)
for row in wb.active.iter_rows(values_only=True):
    item = parse_row(row)
    items.append(item)
```

**Output Streaming**:
```python
# Direct write to CSV (no DataFrame accumulation)
with open(output_file, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['Brand', 'Supplier Name', ...])  # Header
    
    for item in parsed_list.items:
        transformed = transform_item(item)
        writer.writerow([...])  # Write immediately
```

**Results**:
- Memory: 2-3GB → 100MB (96% reduction)
- Speed: +10% overhead (acceptable trade-off)
- Scalability: Can handle files of any size

**Files Modified**:
- `src/parsers/price_list_parser.py` - Streaming CSV/Excel parsing
- `src/output/file_generator.py` - Streaming CSV output

### Timezone Handling

**Problem**: Email dates were being stored as naive datetimes (no timezone), causing them to be interpreted as execution time instead of actual email time. This caused massive email skipping.

**Symptom**:
```
Email sent: 2025-10-07T18:03:17 UTC
State saved: 2025-10-13T02:34:06 (execution time!)
Next run: Skips all emails from Oct 7-13
```

**Root Cause**:
```python
# BEFORE (Bug)
timestamp = int(message['internalDate']) / 1000
return datetime.fromtimestamp(timestamp)  # Naive datetime
```

Gmail's `internalDate` is milliseconds since epoch (UTC), but `fromtimestamp()` without timezone creates a naive datetime that gets misinterpreted.

**Solution**:
```python
# AFTER (Fixed)
timestamp = int(message['internalDate']) / 1000
from datetime import timezone
return datetime.fromtimestamp(timestamp, tz=timezone.utc)  # UTC-aware
```

**Results**:
- All datetimes are UTC-aware
- Email dates correctly preserved
- State timestamps accurate
- No email skipping

**Files Modified**:
- `src/gmail/gmail_client.py` - `get_message_date()` returns UTC-aware datetime
- `src/main.py` - Convert datetime to ISO string before saving

### Duplicate Prevention

**Problem**: Gmail's `after:` query only supports date granularity, not timestamp:
```
after:2025/10/07  ← Returns ALL emails from Oct 7
```

If last processed was `2025-10-07T18:03:17`, it would reprocess earlier emails from the same day.

**Solution**: Two-stage filtering:

**Stage 1: Gmail Query (Date-level)**:
```python
date_str = after_date.strftime('%Y/%m/%d')
query = f"to:pricing@ucalexports.com after:{date_str}"
messages = gmail_client.list_messages(query=query)
```

**Stage 2: Code Filter (Timestamp-level)**:
```python
for message in messages:
    email_date = get_message_date(message)
    
    if email_date <= after_date:  # Already processed
        continue
    
    process_email(message)  # Only new emails
```

**Results**:
- Each email processed exactly once
- No duplicates
- Safe to run multiple times per day
- Efficient (filters before processing)

**Files Modified**:
- `src/orchestrator.py` - Added exact timestamp filtering

### Email Notifications

**Problem**: Summary emails were:
1. Sent FROM delegated user instead of pricing group
2. Too basic (just counts, no details)

**Solution**:

**1. Configurable Sender Address**:
```json
{
  "notification": {
    "summary_email_recipients": ["robin.ashford@ucalexports.com"],
    "summary_from_email": "pricing@ucalexports.com"
  }
}
```

Separates authentication user from sender address.

**2. Enhanced Summary Format**:
- Executive summary with status
- Categorized results (success/warnings/failed/skipped)
- Per-email details with Drive links
- Action items based on results
- Professional formatting with Unicode

**Files Modified**:
- `src/gmail/gmail_client.py` - Added `from_email` parameter
- `src/notification/email_sender.py` - Enhanced summary generation
- `src/main.py` - Pass `summary_from_email` from config

### Domain-Wide Delegation for Google Drive

**Problem**: Drive uploader was using service account credentials directly, which fails when uploading to folders shared within a Google Workspace domain that has external sharing restrictions.

**Error symptom**:
```
HttpError 403: The user does not have sufficient permissions for file [folder_id]
```

**Root Cause**: Google Workspace domains can restrict file sharing to domain users only. Service accounts are external to the domain, so they cannot access shared folders even when explicitly shared with the service account email.

**Solution**: Use **Domain-Wide Delegation** to impersonate a domain user (gopika@ucalexports.com) when accessing Drive, similar to how Gmail authentication works.

**Implementation**:

**1. Updated DriveUploader Class** (`src/output/drive_uploader.py`):
```python
class DriveUploader:
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    
    def __init__(self, service_account_info: Dict, delegated_user: Optional[str] = None):
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=self.SCOPES
        )
        
        # Use domain-wide delegation if delegated_user provided
        if delegated_user:
            credentials = credentials.with_subject(delegated_user)
        
        self.service = build('drive', 'v3', credentials=credentials)
```

**2. Added Configuration** (`config/core_config.json`):
```json
{
  "drive": {
    "impersonation_email": "gopika@ucalexports.com"
  }
}
```

**3. Updated Orchestrator** (`src/orchestrator.py`):
```python
drive_impersonation_email = core_config.get('drive', {}).get('impersonation_email')
self.drive_uploader = DriveUploader(
    service_account_info=service_account_info,
    delegated_user=drive_impersonation_email
)
```

**Prerequisites**:
1. **Admin must add Drive scope to Domain-Wide Delegation**:
   - Google Workspace Admin Console → Security → API Controls → Domain-wide Delegation
   - Service account: `email-pricing-bot@pricing-email-bot.iam.gserviceaccount.com`
   - Add scope: `https://www.googleapis.com/auth/drive.file`

2. **Folder Permissions**:
   - All brand folders must be shared with `gopika@ucalexports.com`
   - Permission level: Editor (to create files)

**Benefits**:
- Works with domain-restricted shared folders
- Consistent authentication pattern (same as Gmail)
- Audit trail shows actions by gopika@ucalexports.com
- No need to share folders with external service account

## Web Scraping Architecture

In addition to email-based price list processing, the system supports automated web scraping of supplier portals to download price lists directly. This complements the email processing pipeline by enabling proactive retrieval of data from supplier websites.

### Overview

Web scraping allows the system to:
- Authenticate to supplier portals using stored credentials
- Navigate to price list pages and download files automatically
- Schedule regular scraping runs (e.g., weekly, monthly)
- Process scraped files through the same parsing pipeline as email attachments

### Architecture Components

```text
Supplier Portal (Web)
        ↓
   Browser Automation (Playwright)
        ↓
   Authentication & Token Management
        ↓
   API/Download Orchestration
        ↓
   Brand Detection & Filtering
        ↓
   JSON → Excel Conversion (if needed)
        ↓
   Existing File Processing Pipeline
        ↓
   Google Drive Upload
```

### Scraper Types

1. **API Client** (`api_client`): For modern SPAs with REST APIs (e.g., NEOPARTA)
   - Logs in via browser to harvest authentication tokens
   - Makes API calls to list available price lists
   - Downloads each file via API endpoint
   - Converts JSON responses to Excel format if needed

2. **Link Downloader** (`link_downloader`): For traditional websites with direct download links (e.g., APF)
   - Logs in to supplier portal
   - Navigates to price list page
   - Identifies download links via CSS selectors
   - Clicks links to trigger downloads

3. **Directory Listing** (`directory_listing`): For open directory structures (e.g., CONNEX)
   - Uses HTTP Basic Auth and file listing
   - Parses HTML directory listings

4. **WebDAV** (`webdav`): For WebDAV/Nextcloud-like systems (e.g., Materom)
   - Uses WebDAV protocol for file operations
   - PROPFIND for directory listing

5. **Form Export** (`form_export`): For sites with form-based file generation (e.g., Technoparts)
   - Fills forms and triggers downloads

### State Management

The web scraping system uses two types of supplier file tracking:

#### Type 1: Date-Based Tracking (Incremental Downloads)

**Use Case**: Suppliers that provide date information in either:
- API response fields (e.g., NEOPARTA with `ValidFrom`, `ValidTo`)
- Filename patterns (e.g., TECHNOPARTS with `BMW_October_2024.xlsx`)

**Processing Logic**:
1. API/scraper lists all available files
2. **For each file**: Extract supplier_filename and valid_from date
3. **Check**: `is_file_already_processed(supplier_filename, valid_from_date)`
   - Compares against `downloaded_files` array
   - Matches by supplier_filename + valid_from_date combination
4. **Skip**: If exact match found (same supplier_filename, same valid_from date)
5. **Download**: If not found OR different valid_from date (new price list!)
6. **Record**: Add to `downloaded_files` with metadata (supplier_filename, valid_from_date, drive_file_id, timestamp)

**Benefits**:
- Intelligent duplicate detection
- Downloads only NEW or UPDATED price lists
- Bandwidth efficient (skips before download)
- Tracks individual brand-level files

#### Type 2: Full Scan (No Date Tracking)

**Use Case**: Suppliers with NO date information available
- No dates in API responses
- No dates in filenames
- No reliable version identifiers

**Processing Logic**:
1. Scraper lists all available files
2. **No checking**: Downloads all files every run
3. **Archiving**: Relies on Google Drive auto-archiving to prevent duplicates
   - Old file moved to `_Archive` subfolder
   - New file uploaded to main folder
4. **Record**: Does not populate `downloaded_files` array

**Benefits**:
- Simple implementation
- Works when no date information available
- Archiving prevents duplicates in active folders

**Trade-offs**:
- Downloads same files repeatedly
- Higher bandwidth usage
- Suitable for monthly suppliers with few files

### Execution Model

**Local Development**:
```bash
# Test scraper with dry-run (no Drive upload)
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# Run with full processing
python scripts/run_scraper_local.py --supplier NEOPARTA

# Enable screenshots for debugging
python scripts/run_scraper_local.py --supplier NEOPARTA --screenshots
```

**Production (Cloud Functions)**:
- Triggered by Cloud Scheduler based on scraper schedule
- Uses same configuration files from GCS
- Processes files through existing email pipeline
- Sends summary notifications

### Performance & Scheduling

The web scraping system runs within Cloud Functions (2nd gen, 60-minute timeout) with intelligent scheduling, incremental downloads, timeout handling, brand filtering, and duplicate prevention through archiving.

**Key Components**:
- **StateManager**: Tracks scraper execution state for resume capability and duplicate detection
- **ScheduleEvaluator**: Determines when scrapers should run based on configuration and state
- **ExecutionMonitor**: Tracks execution time and signals graceful shutdown before timeout
- **VersionDetector**: Extracts version/date identifiers for incremental download strategies

**Timeout Strategy**:
- **Global timeout**: 3600 seconds (Cloud Function max)
- **Global buffer**: 180 seconds (stop 3 min before timeout)
- **Per-supplier timeout**: Configurable (300-900 seconds)
- **Per-supplier buffer**: Configurable (60-180 seconds)

**Execution Time Estimates**:
| Supplier | Files | Avg Time | Max Time | Notes |
|----------|-------|----------|----------|-------|
| NEOPARTA | 3-5 | 120s | 300s | API-based, fast |
| APF | 10-20 | 300s | 600s | Link download |
| BRECHMANN | All | 600s | 900s | Full scan monthly |
| CONNEX | 5-10 | 150s | 300s | Simple CSV |
| TECHNOPARTS | 10-15 | 300s | 600s | XLSX downloads |
| MATEROM | 10-20 | 300s | 600s | Custom scraper |

For detailed information on extending web scrapers, see [Extending Web Scraper Guide](./EXTENDING_WEB_SCRAPER.md).

## BigQuery Integration Architecture

The system uses BigQuery for persistent storage of price data with advanced features including supersession reconciliation and historical price tracking using the SCD Type 2 (Slowly Changing Dimension Type 2) pattern.

### Overview

BigQuery integration provides:
- **Supersession reconciliation**: Automated resolution of part number replacement chains
- **Price history tracking**: SCD Type 2 pattern with `valid_from`/`valid_until` dates
- **Deferred merge**: Nightly scheduled consolidation for storage efficiency
- **Historical data loading**: Support for out-of-sequence price list imports

### Data Model

```text
PRICING/
├── price_lists             # Metadata for each uploaded file
├── canonical_prices        # SCD Type 2 deduplicated price history (PRIMARY)
├── processing_errors       # Circular refs, validation errors
└── supersession_audit      # Price inheritance log (optional)
```

**Key Tables**:

| Table | Purpose |
|-------|---------|
| `price_lists` | Metadata per uploaded file (supplier, brand, source, timestamps) |
| `canonical_prices` | Primary price history table with SCD Type 2 validity tracking |
| `processing_errors` | Logged errors (circular references, chain too long, duplicates) |

### SCD Type 2 Pattern

The `canonical_prices` table uses SCD Type 2 for efficient storage and historical queries:

**Status Values**:
| Status | Description | Price |
|--------|-------------|-------|
| `ACTIVE` | Current valid price | `effective_price` |
| `UNAVAILABLE` | Supplier sent price=0 (not available) | NULL |
| `HISTORY` | Superseded by newer price | `effective_price` (historical) |
| `DISCONTINUED` | Explicitly marked as discontinued | NULL |

**State Transitions**:
| Current Status | Incoming Signal | Action |
|----------------|-----------------|--------|
| None (new part) | Price > 0 | Insert new ACTIVE row |
| ACTIVE | Same price | Update `last_seen_date` only (no new row) |
| ACTIVE | Different price | Close as HISTORY, insert new ACTIVE |
| ACTIVE | Price = 0 | Close as HISTORY, insert UNAVAILABLE |
| UNAVAILABLE | Real price | Close as HISTORY, insert new ACTIVE |

This pattern avoids creating duplicate rows for unchanged prices while maintaining full history.

### Supersession Reconciliation

The stored procedure `reconcile_supersessions_staging` performs two-phase reconciliation:

**Phase 1: De-duplication**
- Identifies duplicate part numbers (exact match)
- Winner selection: highest price, then alphabetically lowest supersession
- Logs conflicts to `processing_errors` table

**Phase 2: Supersession Chain Resolution**
- Follows supersession chains (A → B → C uses C's price)
- Detects circular references (A → B → C → A) and falls back to original price
- Creates synthetic rows for supersession targets not in original file
- Handles multiple parts superseding to same non-existent part (uses highest price)

**Example Chain Resolution**:
```text
Input:
  Part A: price=$100, supersession=B
  Part B: price=$80, supersession=C
  Part C: price=$60, supersession=NULL

After reconciliation:
  Part A: effective_price=$60 (uses C's price)
  Part B: effective_price=$60 (uses C's price)
  Part C: effective_price=$60 (own price)
```

### Processing Flow

```text
Real-Time Processing (during email/scraper processing):
┌─────────────┐     ┌─────────────────────┐     ┌──────────────────────┐
│ Source File │────▶│ Python Parser       │────▶│ Upload CSV to GCS    │
│ (XLSX/CSV)  │     │ (streaming)         │     │ gs://bucket/staging/ │
└─────────────┘     └─────────────────────┘     └──────────┬───────────┘
                                                          │
                                                          ▼
                                               ┌──────────────────────┐
                                               │ BigQuery Load Job    │
                                               │ → staging table      │
                                               │ → reconciliation SP  │
                                               │ → export to GCS      │
                                               └──────────┬───────────┘
                                                          │
                                                          ▼
                                               ┌──────────────────────┐
                                               │ Upload to Drive      │
                                               │ Register for merge   │
                                               └──────────────────────┘

Scheduled Processing (nightly at 2:00 AM):
┌────────────────────────────────────────────────────────────────────┐
│ Scheduled Query: CALL merge_pending_to_canonical()                 │
│ - Merge staging tables directly → canonical_prices (dynamic UNION) │
│ - Drop processed staging tables                                    │
│ - Update merge_status = 'MERGED'                                   │
└────────────────────────────────────────────────────────────────────┘
```

### Historical Data Support

The system supports loading historical price files out of sequence (e.g., loading a 2024 file after 2025 files are already loaded). The `merge_pending_to_canonical` stored procedure includes temporal awareness:

**Actions for Historical Data**:
| Action | When | Result |
|--------|------|--------|
| `SKIP_DUPLICATE` | Same supplier+brand+part+valid_from exists | Skip (already loaded) |
| `INSERT_HISTORICAL` | Incoming date < existing active date | Insert with adjusted `valid_until` |
| `INSERT_ACTIVE` | New part or newer date | Standard insert |

This allows the historical price loader to process archived files in any order while maintaining correct SCD Type 2 history.

### Configuration

BigQuery settings in `config/core/core_config.json`:

```json
{
  "bigquery": {
    "enabled": true,
    "project_id": "pricing-email-bot",
    "dataset_id": "PRICING",
    "location": "US",
    "reconciliation": {
      "enabled": true,
      "max_chain_depth": 10,
      "add_missing_supersessions": true
    }
  }
}
```

For detailed schema definitions and SQL examples, see [BigQuery Supersession Design](./design/bigquery-supercession-design.md).

## Historical Price Loader

The Historical Price Loader is a standalone script for batch-loading archived price files from Google Drive into BigQuery. This enables analysis of historical pricing trends and backfilling data.

### Purpose

- Load archived CSV files from brand folders on Google Drive
- Parse standardized filenames to extract metadata (supplier, brand, currency, date)
- Process through BigQuery with supersession reconciliation
- Track progress locally to support resumable operations

### Components

| Component | File | Purpose |
|-----------|------|---------|
| Main Script | `scripts/load_historical_prices.py` | Orchestrates the loading process |
| Filename Parser | `src/utils/filename_parser.py` | Parses standardized filename format |
| Drive Uploader | `src/output/drive_uploader.py` | Lists and downloads files from Drive |
| BigQuery Processor | `src/storage/bigquery_processor.py` | Loads data into BigQuery |
| Tracking File | `state/historical_load_tracking.json` | Tracks processing progress |

### Filename Format

Files must follow the standard naming convention:

```
{Brand}_{Supplier}_{Currency}_{Location}_{MMMDD_YYYY}.csv
```

**Examples**:
- `VAG_APF_EUR_BELGIUM_SEP18_2025.csv`
- `BMW_MATEROM_EUR_ROMANIA_OCT15_2024.csv`
- `VAG_OIL_YANXIN_USD_CHINA_JAN05_2026.csv` (brand with underscore)

The parser works right-to-left to handle brands containing underscores:
1. Extract `.csv` extension
2. Extract year (YYYY)
3. Extract month+day (MMMDD)
4. Extract location
5. Extract currency (3-letter code)
6. Extract supplier
7. Remaining parts = brand (may contain underscores)

### Process Flow

```text
┌─────────────────────────────────────────────────────────────────────┐
│ 1. Load Configuration                                               │
│    - Brand configs (folder IDs)                                     │
│    - Supplier configs (parsing rules)                               │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. List Files from Drive                                            │
│    - For each brand folder, list .csv files                         │
│    - Filter to valid filename format                                │
│    - Add to tracking file with 'pending' status                     │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. For Each Pending File                                            │
│    a. Parse filename → brand, supplier, currency, location, date    │
│    b. Check BigQuery for existing (skip if duplicate)               │
│    c. Download to temp directory                                    │
│    d. Process through BigQueryPriceListProcessor                    │
│    e. Update tracking: completed/failed/skipped                     │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. Output Summary                                                   │
│    - Files found / processed / skipped / failed                     │
│    - Tracking state saved for resume                                │
└─────────────────────────────────────────────────────────────────────┘
```

### Tracking System

The loader maintains a JSON tracking file (`state/historical_load_tracking.json`) with:

```json
{
  "last_run": "2026-01-26T10:30:00Z",
  "total_files_found": 150,
  "files": {
    "VAG_APF_EUR_BELGIUM_SEP18_2025.csv": {
      "filename": "VAG_APF_EUR_BELGIUM_SEP18_2025.csv",
      "drive_file_id": "1abc...",
      "status": "completed",
      "processed_at": "2026-01-26T10:35:00Z",
      "price_list_id": "uuid-here",
      "brand": "VAG",
      "supplier": "APF",
      "valid_from_date": "2025-09-18"
    }
  }
}
```

**Status Values**:
- `pending`: Not yet processed
- `completed`: Successfully loaded to BigQuery
- `skipped`: Already exists in BigQuery (duplicate)
- `failed`: Error during processing (error message stored)

### Duplicate Detection

Before processing each file, the loader checks BigQuery for existing records matching:
- Supplier
- Brand  
- `valid_from_date`

If a match is found, the file is marked as `skipped` to avoid duplicate data.

## Performance Metrics

### Memory Usage (623k row file)
- Before: 2-3GB peak
- After: 100MB peak
- Reduction: 96%

### Processing Time
- CSV parsing: ~30 seconds
- Excel parsing: ~40 seconds
- File generation: ~10 seconds
- Drive upload: ~5 seconds per file
- **Total**: ~60 seconds for 10 emails

### Scalability
- Can handle files of any size (memory constant)
- Limited only by Cloud Function timeout (540s)
- Processes ~10k items per second

## Known Issues & Solutions

### Issue: Excel `chunksize` Not Supported
**Problem**: `pd.read_excel()` doesn't support `chunksize` parameter like `read_csv()`.

**Solution**: Use openpyxl's `load_workbook(read_only=True)` with `iter_rows()` for streaming.

### Issue: JSON Serialization of Datetime
**Problem**: Passing datetime objects to JSON serializer fails.

**Solution**: Always convert to ISO string before saving:
```python
last_email_date_str = last_email_date.isoformat()
state_manager.update_last_processed(last_email_date_str)
```

### Issue: State File with Wrong Timestamp
**Problem**: Existing state file has execution time instead of email time.

**Solution**: Reset state file:
```bash
echo '{"version": "1.0.0", "last_processed_timestamp": "1970-01-01T00:00:00+00:00"}' | \
gsutil cp - gs://pricing-email-bot-bucket/state/last_processed_test.json
```

## Cost Efficiency

| Item                       | Est. Monthly Cost |
| -------------------------- | ----------------- |
| Secret Manager             | ~$0.06            |
| Cloud Function / Cloud Run | ~$0–3             |
| Cloud Scheduler            | Free              |
| BigQuery                   | ~$1–5             |
| **Total**                  | **~$1–8**         |

> This setup runs **hourly**, securely, and scales to hundreds of suppliers without paying for an extra Workspace license.

## Future Optimizations

### 1. Generator Pattern for Items List
Current implementation accumulates items in memory (~100MB). Could use generators:
```python
def parse_csv_generator(file_path):
    with open(file_path) as f:
        for row in csv.reader(f):
            yield parse_row(row)

# Stream directly from parser to writer
for item in parser.parse_csv_generator(input_file):
    transformed = transform(item)
    writer.writerow([...])
```
Would reduce to ~10MB peak.

### 2. Parallel Processing
Process multiple emails in parallel (if Cloud Function supports):
```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(process_email, msg) for msg in messages]
    results = [f.result() for f in futures]
```

### 3. HTML Email Format
Add rich HTML formatting to summary emails:
- Color-coded status
- Collapsible sections
- Charts/graphs
- Brand logos

## Related Documentation

- [Extending Web Scraper Guide](./EXTENDING_WEB_SCRAPER.md) - How to add new supplier scrapers
- [Extending Email Parser Guide](./EXTENDING_EMAIL_PARSER.md) - How to add new email parsing patterns
- [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md) - Setup and operational procedures
- [BigQuery Supersession Design](./design/bigquery-supercession-design.md) - Detailed BigQuery schema and reconciliation logic

