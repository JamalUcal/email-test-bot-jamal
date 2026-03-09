# Supplier Email Automation Architecture

This document describes a secure, low-cost automation system for reading supplier emails sent to a Google Group, parsing attachments, creating a data load file on Google Drive, and sending a daily summary email. Designed to **avoid a paid Workspace license for automation**.

In phase 2 we will add the ability to upload the data to Big Query.
---

## Extending existing command line tool
The /design/original_design.md describes the requirements for the original command line tool.
The code for the original command line toiol is in /original_code
This utility will effectively extend the command line tool to replace a human reading emails, downloading files, configurating and running the command line tool to convert files sent by supplier emails into a .csv format which can be used by the pricing engine.

## Architecture & Process Flow

```text
           Suppliers
               │
               ▼
     pricing-bot@ucalexports.com (Google Group)
               │
               │ Domain-wide delegation via service account
               │ Impersonating: automation@ucalexports.com
               ▼
      Cloud Function / Cloud Run (daily via Cloud Scheduler)
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
      │ 6. Clean/normalize data                     |
      | 7. Create Data Load File on Google Drive    │
      │ 8. Upload to BigQuery tables  (PHASE2)      │
      │ 9. Generate daily summary report            │
      │ 10. Send summary email (as group email)     │
      └─────────────────────────────────────────────┘
               │
               ▼
        Recipients (internal stakeholders)
```

---

## Components

| Component                                   | Purpose                                      | Cost Consideration                   |
| ------------------------------------------- | -------------------------------------------- | ------------------------------------ |
| Google Group                                | Receives supplier emails                     | Already existing; free               |
| Service Account with Domain-Wide Delegation | Automates Gmail API access without paid user | Free (no license)                    |
| Secret Manager                              | Stores JSON key securely                     | ~$0.06/month                         |
| Cloud Function / Cloud Run                  | Executes daily automation                    | Free to a few dollars/month          |
| Cloud Scheduler                             | Triggers function daily                      | Free (up to 3 jobs)                  |
| BigQuery                                    | Stores parsed supplier data                  | ~$1–5/month depending on data volume |
| Optional Cloud Storage                      | Temporary file storage / config              | Minimal cost                         |

---

## Process Flow Details

1. **Email Reception:**

   * Suppliers send price lists to the Google Group.

2. **Trigger Automation:**

   * Cloud Scheduler triggers the Cloud Function once per day.

3. **Authentication & Authorization:**

   * Service account fetches JSON key from Secret Manager.
   * Uses **domain-wide delegation** to impersonate `automation@ucalexports.com` user.
   * This user has access to read emails from the `pricing-bot@ucalexports.com` Google Group.

4. **Read & Identify Emails:**

   * Gmail query: `to:pricing-bot@ucalexports.com`
   * Reads all emails received since last run.
   * Identifies supplier using **three-layer detection** (see Supplier Detection section below).
   * Picks correct parsing rules from **config file** (stored in GCS).

5. **Parse Attachments:**

   * Supports CSV, XLSX
   * Uses `pandas`, `openpyxl`, `tabula-py` or `pdfplumber`.
   * Cleans and normalizes data 

6. **Create file on Google Drive**
    * Using the same capability as we find in the original_code crate a new .csv file in the appropriate format for the pricing engine.


7. **Upload to BigQuery:** PHASE 2 OLY

   * Inserts into supplier-specific tables or unified dataset.
   * Optionally handles deduplication or versioning.

8. **Generate & Send Summary:**

   * Aggregates key metrics (total items, new prices, errors).
   * Sends **daily email summary** from the **group email** using Gmail API with DWD.

9. **Logging & Error Handling:**

   * Logs all steps in Cloud Logging.
   * Sends alerts on parsing failures.

---

## Supplier Detection (Three-Layer Strategy)

The system uses a three-layer strategy to identify the supplier for each email, prioritizing explicit tagging over automatic detection.

### Layer 1: Body Tag Detection (Highest Priority)

Internal team members can manually tag emails with the supplier name using this format in the email body:

```
SUPPLIER: <supplier_name>
```

**Features:**
- Case-insensitive matching
- Validates against configured suppliers in `supplier_config.json`
- Supports future extensibility with additional tags (e.g., `BRAND:`, `EXPIRY:`)
- Useful when forwarding emails from unrecognized domains or when supplier domain is ambiguous

**Example:**
```
SUPPLIER: DEXTAR

Please process the attached price list.
```

### Layer 2: Forwarded Email Parsing (Medium Priority)

When internal team members forward supplier emails to the group, the system automatically extracts the original sender.

**Supported Formats:**
- **Gmail**: `---------- Forwarded message ---------` followed by `From: email@supplier.com`
- **Outlook**: `From:` line with email address

**Process:**
1. Parse email body for forward markers
2. Extract original sender email address
3. Match domain against `email_domain` in `supplier_config.json`
4. Skip internal ucalexports.com addresses

**Example Gmail Forward:**
```
---------- Forwarded message ---------
From: sales@apf.com
Date: Mon, Nov 18, 2024 at 2:30 PM
Subject: October Price List
...
```

### Layer 3: Direct From Header (Fallback)

For emails sent directly to the group (not forwarded), use the standard From header.

**Process:**
1. Extract sender domain from From header
2. Match against `email_domain` in `supplier_config.json`

### Detection Flow

```text
Email arrives → pricing-bot@ucalexports.com
     │
     ▼
Layer 1: Check for SUPPLIER: tag
     │
     ├─ Found & Valid → Use tagged supplier ✓
     │
     ▼
Layer 2: Parse forwarded email
     │
     ├─ Found & Domain Match → Use forwarded supplier ✓
     │
     ▼
Layer 3: Direct From header
     │
     ├─ Domain Match → Use direct supplier ✓
     │
     ▼
Unknown Supplier → Report in summary email ⚠️
```

### Logging & Reporting

All emails include detection metadata in summary reports:
- **Detection Method**: `body_tag`, `forwarded`, `direct`, or `unknown`
- **Original Sender**: For forwarded emails, shows both forwarder and original sender
- **From Address**: The immediate sender (forwarder or direct)

**Example Summary Entry:**
```
From: gopika@ucalexports.com
Original Sender: sales@apf.com
Detection Method: Forwarded Email
```

---

## Security Highlights

* **Service Account**: acts as automation, no human user credentials needed.
* **Secret Manager**: stores JSON key securely, minimal cost.
* **Minimal Gmail scopes**: `read-only` + `send` only.
* **Auditable**: Gmail API logs show all actions performed by the service account.
* **No paid license needed** for automation account.

---

## Cost Efficiency

| Item                       | Est. Monthly Cost |
| -------------------------- | ----------------- |
| Secret Manager             | ~$0.06            |
| Cloud Function / Cloud Run | ~$0–3             |
| Cloud Scheduler            | Free              |
| BigQuery                   | ~$1–5             |
| Total                      | ~$1–8             |

> This setup runs **daily**, securely, and scales to hundreds of suppliers without paying for an extra Workspace license.

---

## Notes

* **Config Files**: contains parsing rules for each supplier and brand. We already have these for the original project which we need to adapt for this new project.
* **Service Account Key Rotation**: rotate keys periodically via Secret Manager.
* **Logging & Monitoring**: use Cloud Logging and optional alerts for failure handling.
* **Scalability**: supports hundreds of emails daily and multiple file formats without adding licenses.

---
## Big Query Price List Data model
PHASE 2 FEATURE
* Supplier - list of Suppliers, with the email domain name of the Supplier, % Discount to be applied automatically to all prices 
* Brands - list of known Brand names, includes a comma separated list of known alias names for the brand
* Currency - list of used ISO currencies (EUR, GBP, SAR, JPY, USD, etc), includes a comma separated list of known alias values for a currency e.g. for USD - "US, $, US$"
* Supplier Price List - instance of a Supplier Price list, linked to a Supplier, with a start date and optional validity end date
* Price List Item - identifies the following key data for each item in the price list

**Note**: Incoterms are derived later as an interpretation of the location field and are not part of Phase 1 requirements.


| Order | Column Name   | Data Type |  Restriction |
|:------|:--------------|:-----------|:--------------------|:-------------|
| 1 | **Brand** | TEXT |  Brand Name provided by the user match one in the Brand table |
| 2 | **Supplier Name** | TEXT | Supplier Name must match one of the list of Supplier table |
| 3 | **Location** | TEXT |  |
| 4 | **Currency** | TEXT |  |
| 5 | **Part Number** | TEXT |  Only alphanumeric and length is standardized. |
| 6 | **Description** | TEXT |  None |
| 7 | **Former PN** | TEXT |  Only alphanumeric and length is standardized. |
| 8 | **Supersession** | TEXT |  Only alphanumeric and length is standardized. |
| 9 | **Price** | DECIMAL |  None |


## Identifying the Brand Name
Suppliers may or may not include the Brand Name as a field in the file. 
In these cases we should look for the brand name or alias to map to the imported records in:

A. Part of the File Name
B. Part of the Subject Line e.g. GM October 2025 Price File

If the Brand cannot be found in these locations then the Import may be rejected.

## Identifying the Expiry Date
Suppliers may not include the expiry date for the Prices in the file. 
In these cases we should look for the expiry date in the Body of the email.

e.g. "Price File Expires October 23, 2025"

## Identifying Currency

The system uses a sophisticated 5-layer fallback hierarchy to detect currency codes for price lists. This enables handling of suppliers who send price lists in multiple currencies.

### Currency Detection Hierarchy (5 Layers)

**Layer 1: Email Body Tag (Highest Priority)**

Users can explicitly specify currency by adding a tag to the email body when forwarding:

```
CURRENCY: USD
```

- Case-insensitive matching
- Validates against supported currency list
- **Overrides all other sources** including supplier configuration
- Useful when forwarding price lists or when automatic detection would be ambiguous

**Layer 2: Supplier Configuration (Conditional)**

The system checks the supplier's configuration for currency entries for the detected brand:

- **If only 1 currency configured**: Uses it automatically (existing behavior)
- **If >1 currency configured**: Proceeds to Layer 3 detection (ambiguous)

Example: If `AL_ROSTAMANI` has entries for `SUZUKI` with both `AED` and `JPY`, the system considers this ambiguous and attempts detection.

**Layer 3: Subject Line & Filename**

Searches for 3-letter currency codes in:
- Email subject line
- Attachment filename

Examples:
- `BMW_Price_List_EUR_October.xlsx` → EUR
- `Price List USD 2025.csv` → USD
- Subject: "Updated prices in GBP" → GBP

Matching is case-insensitive and normalized (whitespace and special characters stripped).

**Layer 4: File Content Analysis**

If still ambiguous, the system performs a lightweight "peek" of the file without full parsing:

**4a. Price Column Header (Priority)**

Checks for parameterized currency codes in column headers:
- `UnitPriceUSD` → USD
- `Price-EUR` → EUR  
- `AED RATE` → AED
- `Unit Price GBP` → GBP

The system uses parameterized matching with `<CURRENCY_CODE>` placeholders defined in `column_mapping_config.json`:
```json
{
  "price": {
    "variants": [
      "UnitPrice<CURRENCY_CODE>",
      "<CURRENCY_CODE>RATE",
      "Price-<CURRENCY_CODE>"
    ]
  }
}
```

**4b. Excel Cell Number Format (Excel files only)**

For Excel files (.xlsx, .xls), checks the price cell's number format string for currency symbols:
- `_("$"* #,##0.00_)` → $ → USD
- `[$€-407]#,##0.00` → € → EUR
- `#,##0.00 "USD"` → USD
- `£#,##0.00` → £ → GBP

Many Excel files store currency symbols in the cell's number format rather than in the actual cell value. The system extracts the symbol/code from the format string and uses scoped detection to match it against the supplier's configured currencies.

**Scoped Symbol Matching**: When a symbol like `$` could represent multiple currencies (USD, SGD), the system finds all matching currencies and filters to only those in the supplier's configuration, ensuring correct detection.

**4c. Price Column Data (Fallback)**

If no currency found in header or format, checks the first data row's price value for currency symbols:
- `$100.00` → USD
- `€45.50` → EUR
- `£30.00` → GBP
- `¥1500` → JPY (context-dependent: could be JPY or CNY)

**Layer 5: Failure (If Still Ambiguous)**

Only triggered when:
- Supplier has >1 currency for the SAME brand
- AND no currency found in Layers 1-4

**Action**: Stop processing, report error in summary email

**Error message**: `"Currency ambiguous: Supplier X has multiple currencies for Brand Y and currency could not be detected"`

### Supported Currencies

The system supports a configurable list of currency codes defined in `config/core/currency_config.json`:

- **USD** (US Dollar): $, US$
- **EUR** (Euro): €
- **GBP** (British Pound): £
- **JPY** (Japanese Yen): ¥
- **AED** (UAE Dirham): د.إ, DHS
- **SAR** (Saudi Riyal): ﷼, SR
- **CNY** (Chinese Yuan): ¥, RMB
- **INR** (Indian Rupee): ₹, RS
- **CAD, AUD, SGD** (Canadian/Australian/Singapore Dollar): $, C$, A$, S$
- And more...

Each currency includes:
- 3-letter ISO code
- Symbol
- Aliases for matching

### Processing Flow

```
1. Email arrives with attachment
   ↓
2. Check email body for CURRENCY: tag
   ↓
3. If no tag → Check supplier config
   ↓
4. If ambiguous (>1 currency) → Check subject/filename
   ↓
5. If still not found → Peek file header/data
   ↓
6. If still not found → FAIL with error
   ↓
7. Merge config with detected/override currency
   ↓
8. Process file with correct currency
```

### Examples

**Example 1: Single Currency (Simple)**
```
Supplier: APF
Brand: BMW
Config: Only USD configured
Result: Uses USD from config automatically
```

**Example 2: Email Tag Override**
```
Email body: "CURRENCY: EUR"
Supplier: Has both USD and EUR for BMW
Result: Uses EUR (email tag overrides config)
```

**Example 3: Filename Detection**
```
Filename: "BMW_Prices_GBP_Nov2025.xlsx"
Supplier: Has USD, EUR, GBP for BMW
Result: Detects GBP from filename
```

**Example 4: Header Detection**
```
Column header: "UnitPriceEUR"
Supplier: Has multiple currencies
Result: Detects EUR from parameterized header match
```

**Example 5: Symbol Detection**
```
Price column: "$125.00"
Supplier: Has USD and EUR
Result: Detects USD from $ symbol
```

**Example 6: Ambiguous Failure**
```
Supplier: Has USD and EUR for BMW
Filename: "BMW_Nov2025.xlsx" (no currency)
Header: "Price" (no currency)
Data: "125.00" (no symbol)
Result: FAILS with error - currency cannot be determined
```

### Configuration

**Currency Config** (`config/core/currency_config.json`):
- Defines supported 3-letter currency codes
- Maps symbols to currencies
- Provides aliases for matching

**Column Mapping Config** (`config/core/column_mapping_config.json`):
- Includes parameterized variants like `UnitPrice<CURRENCY_CODE>`
- Enables dynamic currency detection from headers

**Supplier Config** (`config/supplier/supplier_config.json`):
- Each brand entry can have a currency field
- Multiple entries for same brand with different currencies trigger detection

## Column Header Detection

The system uses intelligent header detection to automatically identify column mappings in price list files, eliminating the need for hardcoded column positions. The detection system supports exact matches, parameterized variants, and wildcard patterns as fallback mechanisms.

### Detection Priority Order (Cascading Fallback)

1. **Exact Variant Matches** (Highest Priority, Fastest)
   - Pre-normalized exact text matches
   - Example: "Unit Price" → matches "unit price" variant
   - No ambiguity, fastest matching

2. **Parameterized `<BRAND>` Variants**
   - Dynamic brand name substitution
   - Example: "BMW PART NUMBER" → matches `"<BRAND> PART NUMBER"` variant when brand is BMW
   - Enables brand-specific column naming

3. **Parameterized `<CURRENCY_CODE>` Variants**
   - Dynamic currency code substitution
   - Example: "USD RATE" → matches `"<CURRENCY_CODE>RATE"` variant
   - Enables currency-specific price columns

4. **Wildcard Pattern Matching** (Last Resort, Most Flexible)
   - Pattern matching with `%` wildcards for unusual headers
   - Example: "Jebel Ali Delivery Price in USD" → matches `"%<CURRENCY_CODE>%price%"` pattern
   - Only triggered when exact and parameterized variants fail

### Wildcard Pattern Syntax

Wildcard patterns use `%` as a placeholder for zero or more characters:

- `%price%` → matches any text containing "price" (e.g., "ANYTHINGPRICEANYTHING")
- `%<CURRENCY_CODE>%price%` → matches "JEBELALIDELIVERYPRICEINUSD" when USD is detected
- `%price%<CURRENCY_CODE>%` → matches "UNITPRICEUSD"

**Normalization**: All matching is performed on normalized text (uppercase, alphanumeric only)

**Configuration Example**:
```json
{
  "price": {
    "variants": [
      "price",
      "unit price",
      "UnitPrice<CURRENCY_CODE>"
    ],
    "wildcard_variants": [
      "%<CURRENCY_CODE>%price%",
      "%price%<CURRENCY_CODE>%",
      "%price%"
    ],
    "exclusions": ["total", "totalprice", "sum", "sumprice"]
  }
}
```

### Exclusion Keywords

To prevent false matches, exclusion keywords block certain headers from matching wildcard patterns:

**Price Field Exclusions**:
- `total`, `totalprice` - Sum/aggregate prices, not unit prices
- `sum`, `sumprice` - Cumulative totals

**How Exclusions Work**:
1. Wildcard pattern matches header (e.g., "Total Price USD" matches `"%price%<CURRENCY_CODE>%"`)
2. System checks if normalized header contains exclusion keyword ("TOTALPRICEUSD" contains "TOTALPRICE")
3. If excluded, skip this match and continue searching

This ensures "Total Price USD" won't be incorrectly identified as the unit price column when "Unit Price USD" also exists in the file.

### Example Scenarios

**Scenario 1: Standard Header (Exact Match)**
```
Column Header: "Unit Price"
Detection: Matches exact variant "unit price" (Priority 1)
Result: ✓ Fast match, no currency detected
```

**Scenario 2: Parameterized Currency (Exact Parameterized)**
```
Column Header: "USD RATE"
Detection: Matches "<CURRENCY_CODE>RATE" with USD substitution (Priority 3)
Result: ✓ Currency detected: USD
```

**Scenario 3: Unusual Header (Wildcard Fallback)**
```
Column Header: "Jebel Ali Delivery Price in USD"
Normalized: "JEBELALIDELIVERYPRICEINUSD"
Detection: Matches "%<CURRENCY_CODE>%price%" wildcard (Priority 4)
Result: ✓ Currency detected: USD, matched via wildcard
```

**Scenario 4: Exclusion Prevention**
```
Column Header: "Total Price USD"
Wildcard Match: Would match "%price%<CURRENCY_CODE>%"
Exclusion Check: Contains "TOTALPRICE" → EXCLUDED
Result: ✗ Skipped, continues searching for other price columns
```

**Scenario 5: Multiple Columns, Priority Order**
```
File has both: "Unit Price USD" and "Jebel Ali Delivery Price in USD"
Detection: "Unit Price USD" matches exact parameterized variant first (Priority 3)
Result: ✓ Uses "Unit Price USD", never attempts wildcard matching
```

### Benefits

1. **Handles vendor creativity**: Unusual column names like "Jebel Ali Delivery Price in USD" work without configuration updates
2. **Maintains precision**: Exact variants match first, preventing ambiguity
3. **Prevents false positives**: Exclusion keywords block inappropriate matches
4. **Configurable**: Patterns and exclusions defined in JSON, no code changes required
5. **Future-proof**: Easy to add new patterns/exclusions as supplier formats evolve

## Avoiding duplicate Parsing
The bot will have a locally writable file which it reads the date from which it should check for emails i the shared mailbox. A successful read iteration will update the file with the date of the latest email read.
The next run will read emails recieved after that date.


## Config Files
All config files will be in JSON Format
* **Core Config** - he email mailbox to read from, service account and how to find the secret,  Big Query Connection details and names of tables, email mailbox to send results to
* **Supplier Config**  - extending  /original_code/supplierConfig.json with the domain names to read from
* **Brand Config** - matching /original_code/Bramnd_partNumber.json - tells use the length of a part number to be padded and where to write the parsed file to
* **Email Parsing Rules** - rules for how to parse Brand Name and expiry dates from emails - possibly not needed, but maybe we tie this to the 


 ## Development Stages

 ## Phase 1 - Goal: Parse emails to files on Google Drive

* **Stage 1** Create documentaton to set up the architecture for the project

* **Stage 2** Create PoC implementation that has the following features 
    * Config file for Suppliers adapted from existing config file with the domain name of suppliers we want to parse
    * Core config file with the mailbox, access to the infrastructure components, and email send the results to
    * Writable config 
    * Ability to read emails sent to the email mail box and send a list of the emails it detected it should read and the file name to the email 

* **Stage 3** Extending the PoC implementation with file parsing
    * Attempts to parse the file without any email brand and expiry date parsing
    * Exception handling
    * Sends a summary of the results of parsing to the results-reporting email 


* **Stage 3** Extending the PoC implementation with file writing
    * Writes successfully parsed files to the Google Drive
     * Sends a summary of the results of file parsing and file writing to the results-reporting email 

* **Stage 4** Extending the implementation with email parsing
    * Reads Brand names and Expiry dates from the email
    * Sends a summary of the results of email, file parsing, and file writing to the results-reporting email 

  
 ## Phase 2 - Goal: Parse emails to files on Google Drive and Big Query
 TODO

---

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

### Configuration Structure

Web scrapers are configured via `config/scraper/scraper_config.json`:

```json
{
  "supplier": "NEOPARTA",
  "type": "api_client",
  "enabled": true,
  "schedule": {
    "frequency": "weekly",
    "day_of_week": "monday", 
    "time": "09:00",
    "timezone": "Asia/Kolkata"
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
    "export_endpoint": "/api/Pricings/exportByBrandId",
    "export_method": "GET",
    "export_params_template": {
      "brandId": "{BrandId}",
      "segmentId": "{SegmentId}"
    }
  },
  "brand_detection": {
    "source": "api_response",
    "field": "Brand",
    "pattern": "^([A-Z_-]+)",
    "normalize": true
  },
  "metadata": {
    "brand_field": "Brand",
    "valid_from_field": "ValidFrom",
    "valid_to_field": "ValidTo",
    "default_expiry_days": 90
  },
  "limits": {
    "max_files": 3
  }
}
```

### Scraper Types

#### 1. API Client Scraper
For suppliers with structured APIs:
- Logs in via browser to harvest authentication tokens
- Makes API calls to list available price lists
- Downloads each file via API endpoint
- Converts JSON responses to Excel format if needed

#### 2. Link Downloader Scraper
For suppliers with download links on web pages:
- Logs in to supplier portal
- Navigates to price list page
- Identifies download links via CSS selectors
- Clicks links to trigger downloads

### Brand Detection and Filtering

Unlike email processing where brand names may be in filenames or subject lines, web scrapers extract brand information from API responses or page content.

**Brand Detection Process:**

1. **Extract brand name** from API response field (e.g., `item['Brand']`)
2. **Normalize brand name**: 
   - Convert to uppercase
   - Replace non-alphanumeric characters with underscores
   - Example: "BMW-OIL" → "BMW_OIL"
3. **Match against brand_config.json**:
   - Case-insensitive matching
   - Check brand name and aliases
   - Example: "bmw" matches brand "BMW"
4. **Filter before download**:
   - Only download files with matching brands
   - Log warnings for unmatched brands
   - Continue processing other files (don't fail entire scrape)

**Example Filtering:**

```python
# API returns 57 items
# After brand filtering: 15 items (only BMW, FCA, BENTLEY match config)
# Skipped: STOCK, VAG-OIL, DEALERSTOCKOFFER (not in brand_config.json)
```

### Authentication Methods

#### Form-Based Authentication
- Fills username/password fields using Playwright
- Submits login form
- Waits for success indicator (URL change, element present)
- Harvests cookies or tokens for API requests

#### Token Storage
Supports extracting tokens from browser storage:
- **localStorage** or **sessionStorage**
- Configurable path (e.g., `loginData.Token`)
- Flexible header format (e.g., `Bearer {token}`)

#### JWT Bearer Tokens
Example: NEOPARTA stores JWT in localStorage:
```json
{
  "type": "localStorage",
  "path": "loginData.Token",
  "header_name": "Authorization", 
  "header_format": "Bearer {token}"
}
```

#### API Key Authentication (No Browser Required)

For suppliers that provide direct API access with API keys (no website login needed), the system supports Bearer token authentication without browser automation:

**Configuration:**
```json
{
  "authentication": {
    "method": "bearer",
    "token_header": "Authorization",
    "password_env": "SCRAPER_SUPPLIER_API_KEY"
  }
}
```

**Features:**
- No browser required - uses httpx directly
- API key stored in environment variable
- Automatically formats as `Bearer {token}`
- Faster execution (no Playwright overhead)

**Example: Wiuse/APF API Integration**

The Wiuse Pricing API provides direct REST endpoints:

```json
{
  "supplier": "APF",
  "type": "api_client",
  "authentication": {
    "method": "bearer",
    "token_header": "Authorization",
    "password_env": "SCRAPER_APF_API_KEY"
  },
  "api": {
    "base_url": "https://pricing.wiuse.net",
    "list_endpoint": "/pricelists",
    "list_method": "GET",
    "list_items_path": "priceLists",
    "export_endpoint": "/download-pricelist",
    "export_method": "GET",
    "export_params_template": {
      "brandCode": "{brandCode}"
    }
  },
  "metadata": {
    "brand_field": "brandCode",
    "version_field": "version",
    "is_new_field": "isNew"
  }
}
```

**Wiuse API Endpoints:**
- `GET /pricelists` - Lists all available price lists with metadata
  - Returns: `{ success, message, priceLists: [{ brandCode, brandName, version, createdDateTime, isNew }] }`
  - The `isNew` flag indicates if a price list has been downloaded before
  - The `version` field enables intelligent duplicate detection

- `GET /download-pricelist?brandCode={code}` - Downloads a price list as file stream
  - Returns: Binary file stream (Excel/CSV)

**Version-Based Duplicate Detection:**

For APIs that provide version information (like Wiuse), the scraper uses version-based duplicate detection:
1. Checks `isNew` flag - skips download if `false`
2. Compares `version` field against previously downloaded versions
3. Falls back to `createdDateTime` if version not available

This ensures efficient operation by only downloading genuinely new price lists.

### File Processing Pipeline

#### 1. JSON to Excel Conversion
Many APIs return JSON data. The scraper automatically:
- Detects JSON content-type
- Extracts data array (e.g., `response.Data`)
- Converts to pandas DataFrame
- Saves as Excel (.xlsx) using openpyxl

```python
# API response
{"Data": [{"Brand": "BMW", "ItemCode": "12345", "Price": 45.50}, ...]}

# Converted to Excel with columns:
# Brand | ItemCode | Price
# BMW   | 12345    | 45.50
```

#### 2. File Naming Convention

**Format:** `{SupplierFilename}_{SupplierName}_{Location}_{Currency}_{ValidFromMMYY}.csv`

**Examples:**
- `BMW_PART1_NEOPARTA_LITHUANIA_EUR_1024.csv`
- `FCA_PART2_NEOPARTA_LITHUANIA_EUR_1024.csv`

**Key Difference from Email Processing:**
- Email: Brand name from brand_config.json (e.g., "BMW")
- Web Scraper: Full supplier filename, normalized (e.g., "BMW_PART1")

This preserves the supplier's original categorization while ensuring valid filenames.

#### 3. Brand Config Integration

Each file is associated with a brand from `brand_config.json`:
- Determines Drive folder location (`driveFolderId`)
- Applies part number formatting rules (`minimumPartLength`)
- Uses parsing configuration from supplier config

### Error Handling

**Non-Matching Brands:**
```
[WARNING] Brand 'STOCK' not found in brand config - skipping download
[WARNING] Brand 'VAG-OIL' not found in brand config - skipping download
[INFO] Successfully matched: BMW (2 files), FCA (1 file)
```

**Failed Authentication:**
```
[ERROR] Login failed - timeout waiting for success indicator
[INFO] Taking screenshot: error_auth_failed.png
[ERROR] Scraping failed for NEOPARTA - no files downloaded
```

**Partial Failures:**
- Process all items regardless of individual failures
- Report successes and failures in scraping result
- Continue with next scheduled run

### Comparison: Web Scraping vs Email Processing

| Aspect | Email Processing | Web Scraping |
|--------|-----------------|--------------|
| **Trigger** | Supplier sends email | Scheduled (weekly/daily) |
| **Brand Detection** | Filename, subject, or file column | API response field |
| **Filtering** | Process all attachments | Filter before download |
| **File Naming** | Brand name from config | Supplier's full filename (normalized) |
| **Authentication** | N/A | Credentials stored securely |
| **Frequency** | Event-driven (when email arrives) | Time-based schedule |
| **Best For** | Suppliers who email regularly | Suppliers with web portals |

### Execution Model

**Local Development:**
```bash
# Test scraper with dry-run (no Drive upload)
python scripts/run_scraper_local.py --supplier NEOPARTA --dry-run

# Run with full processing
python scripts/run_scraper_local.py --supplier NEOPARTA

# Enable screenshots for debugging
python scripts/run_scraper_local.py --supplier NEOPARTA --screenshots
```

**Production (Cloud Functions):**
- Triggered by Cloud Scheduler based on scraper schedule
- Uses same configuration files from GCS
- Processes files through existing email pipeline
- Sends summary notifications

### State Management for Testing

**View Current State:**
```bash
# View entire state file
cat ./state/scraper_state.json | python3 -m json.tool

# View specific supplier state
cat ./state/scraper_state.json | python3 -m json.tool | grep -A 20 "NEOPARTA"
```

**Reset State File:**

Reset entire state (all suppliers):
```bash
cat > ./state/scraper_state.json << 'EOF'
{
    "last_processed_timestamp": "2025-10-19T09:16:18.275949+00:00",
    "last_execution_timestamp": "2025-10-19T09:16:18.275949+00:00",
    "last_scraped": {},
    "suppliers": {},
    "version": "1.0.0"
}
EOF
```

Reset specific supplier (e.g., NEOPARTA):
```bash
cat > ./state/scraper_state.json << 'EOF'
{
    "last_processed_timestamp": "2025-10-19T09:16:18.275949+00:00",
    "last_execution_timestamp": "2025-10-19T09:16:18.275949+00:00",
    "last_scraped": {},
    "suppliers": {
        "NEOPARTA": {
            "last_run": null,
            "last_version": null,
            "downloaded_files": [],
            "interrupted": false,
            "last_file_index": 0
        }
    },
    "version": "1.0.0"
}
EOF
```

**State File Fields:**
- `downloaded_files` - List of previously processed files (for duplicate detection)
  - Each entry includes: `supplier_filename`, `valid_from_date`, `drive_file_id`, `timestamp`
  - `supplier_filename` - Original filename from supplier's website (used for duplicate detection)
  - `valid_from_date` - ISO date string when file becomes valid (used for duplicate detection)
  - `drive_file_id` - Google Drive file ID (for audit purposes)
  - `timestamp` - When the entry was added to state (for cleanup)
- `last_run` - Last execution timestamp
- `last_version` - Last detected version/date (if applicable)
- `interrupted` - Resume flag for interrupted runs
- `last_file_index` - Position to resume from after interruption

**Automatic Cleanup:**
To prevent the state file from growing indefinitely, the system automatically removes old file entries:
- **Retention period:** 90 days (configurable)
- **When:** At the start of each supplier run
- **What:** Removes `downloaded_files` entries older than 90 days based on their `timestamp` field
- **Why:** Keeps state file size manageable while retaining recent history for duplicate detection

Example cleanup log:
```
Cleaned up 15 old file entries for NEOPARTA (removed: 15, retained: 24, retention_days: 90)
```

**When to Reset State:**
- Testing duplicate detection (reset to re-download same files)
- Testing resume functionality (reset interrupted flag)
- Debugging state-related issues
- Starting fresh after configuration changes

### Performance Considerations

**Timing:**
- Authentication: 2-3 seconds
- API list call: <1 second
- File download: 1-10 seconds per file (depends on size)
- JSON to Excel conversion: 2-5 seconds per file
- **Total:** 3-7 minutes for 57 files (within Cloud Function limits)

**Optimization:**
- Brand filtering reduces unnecessary downloads
- max_files config for testing
- Streaming for large files
- Concurrent downloads (future enhancement)

### Security

**Credentials Management:**
- Stored in environment variables (.env for local)
- Secret Manager for production
- Never logged or exposed in errors
- Rotated regularly

**Token Handling:**
- Extracted from browser storage securely
- Used only for current session
- Not persisted beyond scraping run

---

## Requirements Clarifications

### 1. Email Processing & Identification

**1.1 Supplier Email Identification**
- **Match by**: Supplier domain only (e.g., `@apf.com`)
- **Configuration**: Extend `supplierConfig.json` with `email_domain` field for each supplier
- **Unknown suppliers**: 
  - If from unconfigured domain AND not from own domain: Report in summary email with originating email address and subject line
  - If from own domain: Ignore (internal emails)

**1.2 Attachment Handling**
- **Multiple attachments**: Yes, single email may have multiple attachments
  - Brand name typically in filename (if not in filename, reject the file)
  - Attempt to parse all CSV and XLSX files
- **File formats**:
  - **Supported**: CSV, XLSX (extend original code to handle XLSX)
  - **Warning**: PDF, XLS (add warning to email summary, do not process)
- **File size**: Handle up to 500,000 rows (design for this capacity)
- **Processing approach**: Use streaming and local temporary file location for large files

### 2. Configuration & State Management

**2.1 "Last Run" Tracking**
- **Storage**: GCS bucket
- **State file**: Track last processed email timestamp
- **Execution**: Read config every 1 hour, but only execute every 24 hours at configurable time (controlled by config)

**2.2 Config File Storage**
- **Location**: GCS bucket
- **Reload frequency**: Read config every hour
- **Execution control**: Config determines when to execute (24-hour schedule at specific time)
- **Updates**: No redeployment needed - changes take effect on next config reload

### 3. Brand & Expiry Date Parsing

**3.1 Brand Name Detection**
- **Priority order**: 
  1. Filename first
  2. Subject line second
  3. If file has brand name column, use that
- **Case sensitivity**: Case-insensitive matching
- **Multiple brands**: If multiple brands in filename/subject and file doesn't have brand column, report as error in summary
- **Fallback**: Optional fallback brand per supplier in config (report in daily summary warnings when used)

**3.2 Expiry Date Parsing**
- **Supported formats**:
  - "October 23, 2025"
  - "23/10/2025"
  - "2025-10-23"
  - "Valid until Oct 23, 2025"
  - "23 Oct 25"
  - "23 Oct"
  - "23 October"
- **Fallback hierarchy**:
  1. Email body
  2. Supplier config default (`default_expiry_days`)
  3. System default duration (from core config)
- **Validation**: Validate date is in future
  - If not in future: Use it but add warning to daily summary

### 4. Google Drive Integration

**4.1 File Naming & Organization**
- **Naming convention**: `Brand_SupplierName_Location_Currency_ValidFromDateMMYY.csv`
  - Example: `VAG_APF_EUR_BELGIUM_SEP18_2025.csv`
- **Organization**: Place file directly in brand folder, no subdivisions
- **Duplicates**: If file exists, create duplicate (Google Drive allows this)
  - Note as warning in summary output

**4.2 Service Account Permissions**
- **Access level**: Configure service account with minimum permissions to specific brand folders only
- **Setup**: Manual configuration during infrastructure setup

### 5. Error Handling & Notifications

**5.1 Summary Email Content**
- **Section 1 - Statistics**: 
  - Emails processed
  - Files processed
  - Total rows
  - Error/warning counts
- **Section 2 - Errors**: 
  - Critical failures
  - File details
  - Error messages
- **Section 3 - Warnings**: 
  - Non-critical issues
  - Fallback values used
  - Duplicate files
  - Past expiry dates
- **Section 4 - Successfully Parsed List**:
  - Format: `Email from: [address], Subject: [subject], Success - [File names]: [Written to file name]`
  - For warnings/errors: Include detailed messages

**5.2 Partial Failures**
- **Behavior**: Process all emails regardless of individual failures
- **Reporting**: Report success, warning, and failure for each in summary email
- **Email handling**: Leave emails in place (no marking or moving)

### 6. Data Transformation Rules

**6.1 Discount Application**
- **Configuration**: Add `discount_percent` field to supplier config
- **Application order**: Apply discounts AFTER GST removal

**6.2 Column Mapping Validation**
- **Missing columns**: If file doesn't match expected format, reject it and report the failure in summary

### 7. Deployment & Infrastructure

**7.1 Cloud Function vs Cloud Run**
- **Preference**: Cloud Functions preferred
- **Processing time**: Unknown at this stage
- **Graduation path**: Start with Cloud Function, graduate to Cloud Run if not sufficiently robust
- **Manual triggering**: Yes, support manual HTTP trigger with `force_execution` and `dry_run` options

**7.2 Execution Schedule**
- **Time**: Configurable time in core config
- **Timezone**: IST (Asia/Kolkata)
- **Retry on failure**: No automatic retry
  - Report failure in summary email
  - If can't send summary email, just log the failure

### 8. Testing & Validation

**8.1 Stage 2 PoC Scope**
- **Mailbox**: Read from production mailbox
  - Safe because: Read-only access to mailbox, not changing emails, not sending external emails
- **Testing approach**: Implement dry-run mode for safe testing

**8.2 Config File Migration**
- **Approach**: Create new supplier config file (parallel development)
- **Migration strategy**: Move suppliers piece by piece
- **Initial suppliers**: Start with DEXTAR supplier configuration (7 brand configurations)
  - OPEL (Netherlands, EUR)
  - FCA (EU, EUR)
  - MOPAR (USA, USD)
  - PSA (Netherlands, EUR)
  - HONDA (USA/EU, USD/EUR)
  - NISSAN (USA, USD)
  - TOYOTA (USA, USD)

### 9. Phase 1 vs Phase 2 Boundary

**9.1 BigQuery Integration**
- **Phase 1**: Focus on Google Drive output only
- **Phase 2**: Add BigQuery upload
- **Architecture**: Design to easily add BigQuery later

### 10. Incoterms & Pricing Metadata

**Incoterms**: Removed from requirements
- Incoterms are derived later as an interpretation of the location field
- Not part of Phase 1 or Phase 2 implementation
- No need to parse or store Incoterms

---

## Implementation Approach

### New Implementation Location
- **Path**: `/src` directory
- **Original code**: Leave `/original_code` entirely alone as reference
- **Reason**: Parallel development, preserve working reference

### Configuration Strategy
- **Core config**: New file with system settings
- **Supplier config**: New file starting with DEXTAR, add suppliers incrementally
- **Brand config**: Use updated `Brand_partNumber.json` as base, add aliases field

### Execution Model
- **Trigger**: Cloud Scheduler (hourly)
- **Config check**: Every hour
- **Actual execution**: Once per day at configured time (e.g., 9 AM IST)
- **Control**: Config file determines execution schedule

### Error Philosophy
- **Process all**: Continue processing even if individual emails fail
- **Report all**: Comprehensive reporting of all successes, warnings, and errors
- **No blocking**: One failure doesn't stop processing of other emails

---

## Web Scraping: Performance & Scheduling Architecture

### Overview

The web scraping system runs within Cloud Functions (2nd gen, 60-minute timeout) with intelligent scheduling, incremental downloads, timeout handling, brand filtering, and duplicate prevention through archiving.

### Architecture Components

#### 1. State Management (`StateManager`)

**Purpose**: Track scraper execution state for resume capability and duplicate detection

### Two Types of Supplier File Tracking

The system supports two distinct approaches to tracking downloaded files, based on whether suppliers provide date information:

#### Type 1: Date-Based Tracking (Incremental Downloads)

**Use Case**: Suppliers that provide date information in either:
- API response fields (e.g., NEOPARTA with `ValidFrom`, `ValidTo`)
- Filename patterns (e.g., TECHNOPARTS with `BMW_October_2024.xlsx`)

**Configuration**:
```json
{
  "schedule": {
    "detection_mode": "date_based"
  },
  "metadata": {
    "valid_from_field": "ValidFrom",
    "valid_to_field": "ValidTo"
  }
}
```

**State Structure**:
```json
{
  "NEOPARTA": {
    "last_run": "2025-10-26T16:45:00Z",
    "last_version": null,
    "downloaded_files": [
      {
        "supplier_filename": "VAG_OIL_NEOPARTA_LITHUANIA_EUR_0125.xlsx",
        "valid_from_date": "2025-10-01",
        "drive_file_id": "abc123",
        "timestamp": "2025-10-26T16:45:00+00:00"
      },
      {
        "supplier_filename": "BMW_PARTS_NEOPARTA_LITHUANIA_EUR_0125.xlsx",
        "valid_from_date": "2025-10-15",
        "drive_file_id": "def456",
        "timestamp": "2025-10-26T16:46:00+00:00"
      }
    ],
    "interrupted": false,
    "last_file_index": 0
  }
}
```

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

**Examples**: NEOPARTA (API dates), TECHNOPARTS (filename dates), APF, BRECHMANN

---

#### Type 2: Full Scan (No Date Tracking)

**Use Case**: Suppliers with NO date information available
- No dates in API responses
- No dates in filenames
- No reliable version identifiers

**Configuration**:
```json
{
  "schedule": {
    "detection_mode": "full_scan"
  }
}
```

**State Structure**:
```json
{
  "SUPPLIER_X": {
    "last_run": "2025-10-26T10:00:00Z",
    "last_version": null,
    "downloaded_files": [],
    "interrupted": false,
    "last_file_index": 0
  }
}
```

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

**Examples**: Suppliers without any date metadata

---

**Supplier-Specific State Fields**:

| Field | Type 1 (Date-Based) | Type 2 (Full Scan) |
|-------|---------------------|-------------------|
| `last_run` | Timestamp of last execution | Timestamp of last execution |
| `last_version` | Not used (null) | Not used (null) |
| `downloaded_files` | **Array of file records with supplier_filename+date** | Empty array |
| `interrupted` | Resume flag | Resume flag |
| `last_file_index` | For resume from interruption | For resume from interruption |

**Key Methods**:
- `get_supplier_state(supplier)` - Get complete state for supplier
- `update_supplier_state(supplier, state)` - Update supplier state
- `mark_supplier_interrupted(supplier, index)` - Mark for resume
- `clear_supplier_interrupted(supplier)` - Clear after completion
- `is_file_already_processed(supplier, supplier_filename, valid_from_date)` - **Check duplicates (Type 1 only)**
- `add_downloaded_file(supplier, supplier_filename, valid_from_date, drive_file_id)` - **Track downloads (Type 1 only)**

#### 2. Schedule Evaluation (`ScheduleEvaluator`)

**Purpose**: Determine when scrapers should run based on configuration and state

**Logic Flow**:
1. Check if force execution enabled (skip all checks)
2. Check if interrupted (resume immediately)
3. Check frequency (daily/weekly/monthly)
4. Check time window (1-hour window after scheduled time)
5. Check if already ran in current period

**Configuration Format**:
```json
{
  "schedule": {
    "frequency": "daily",
    "time": "09:00",
    "timezone": "Asia/Kolkata",
    "detection_mode": "date_based"
  }
}
```

**Frequency Types**:
- **daily**: Runs once per day
- **weekly**: Runs on specified day_of_week (e.g., "monday")
- **monthly**: Runs on specified day_of_month (e.g., 1)

**Time Window**: 1-hour window after scheduled time to accommodate hourly Cloud Function triggers

#### 3. Execution Monitoring (`ExecutionMonitor`)

**Purpose**: Track execution time and signal graceful shutdown before timeout

**Key Features**:
- Tracks elapsed time excluding paused periods
- Configurable timeout threshold with buffer
- Pause/resume capability for wait times
- Progress tracking and logging

**Configuration**:
```json
{
  "execution": {
    "max_execution_time_seconds": 600,
    "timeout_buffer_seconds": 120
  }
}
```

**Usage Pattern**:
```python
monitor = ExecutionMonitor(
    max_duration_seconds=600,
    buffer_seconds=120
)

# Check before each operation
if monitor.should_stop():
    # Save state and exit gracefully
    state_manager.mark_supplier_interrupted(supplier, current_index)
    break
```

#### 4. Version Detection (`VersionDetector`)

**Purpose**: Extract version/date identifiers for incremental download strategies

**Detection Modes**:
- **date_based**: Extract date from filename or metadata
  - Month format: `2024-10`
  - Date format: `2024-10-25`
  - Datetime format: `2024-10-25T10:30:00`
- **full_scan**: No version detection (download all files every time)

**Patterns Detected**:
- Filename patterns: `BMW_2024_10.xlsx`, `PriceList_October_2024.csv`
- Metadata fields: `ValidFrom`, `modified`, `created`, `version`
- URL patterns: `/pricelist/2024/10/file.csv`

**Check Strategies**:
- **incremental**: Download only files newer than last_version
- **full**: Download all files each run (used when no date info available)

#### 5. Brand Filtering

**Two-Layer Filtering**:

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

**Filtering Locations**:
- `ApiClientScraper._filter_items_by_brand()`
- `LinkDownloaderScraper._filter_links_by_brand()`

#### 6. Google Drive Archiving (`DriveUploader`)

**Purpose**: Prevent duplicates by archiving old versions before uploading new ones

**Archive Process**:
1. Check if file with same name exists in target folder
2. Get or create `_Archive` subfolder in target folder
3. Move existing file to `_Archive` folder
4. Upload new file to target folder

**Key Methods**:
- `archive_existing_file(filename, folder_id)` - Archive existing file
- `upload_file_with_archive(file_path, folder_id, brand)` - Upload with auto-archive

**Benefits**:
- No duplicate files in active folders
- Historical versions preserved in _Archive
- Clean folder organization
- Automatic process (no manual cleanup)

### Orchestration Flow

#### Hourly Trigger (Cloud Scheduler)
```text
Every hour → Cloud Function → Unified Orchestrator
                                    │
                                    ▼
                        Global Execution Monitor (60 min timeout)
                                    │
                                    ├─→ Email Processing (if scheduled)
                                    │
                                    └─→ Web Scraping (if scheduled)
                                         │
                                         ▼
                              Schedule Evaluator
                              (Check each supplier)
                                         │
                                         ▼
                              ┌──────────┴──────────┐
                              ▼                     ▼
                        Supplier 1             Supplier 2
                        (Individual Monitor)   (Individual Monitor)
                              │                     │
                              ▼                     ▼
                        Run Scraper           Run Scraper
                              │                     │
                              ├─ Brand Filter       ├─ Brand Filter
                              ├─ Version Check      ├─ Version Check
                              ├─ Download Files     ├─ Download Files
                              ├─ Archive Old        ├─ Archive Old
                              └─ Upload New         └─ Upload New
```

#### Per-Supplier Execution
1. **Schedule Check**: Use `ScheduleEvaluator.should_run_scraper()`
2. **Create Monitor**: Supplier-specific timeout monitor
3. **Run Scraper**: Execute with `run_scraper_with_timeout()`
4. **Check Timeout**: Monitor execution time
5. **Handle Interruption**:
   - Save progress to state
   - Mark as interrupted
   - Resume on next run
6. **On Completion**:
   - Clear interrupted flag
   - Update last_version
   - Update last_run timestamp

### Supplier-Specific Configurations

#### NEOPARTA (API-based, Date Detection)
```json
{
  "supplier": "NEOPARTA",
  "config": [
    {"brand": "BMW"},
    {"brand": "TOYOTA"},
    {"brand": "LEXUS"},
    {"brand": "MERCEDES-BENZ"},
    {"brand": "AUDI"}
  ],
  "schedule": {
    "frequency": "daily",
    "detection_mode": "date_based"
  },
  "execution": {
    "max_execution_time_seconds": 300,
    "timeout_buffer_seconds": 60
  }
}
```

#### BRECHMANN (Full Scan, No Dates)
```json
{
  "supplier": "BRECHMANN",
  "config": [
    {"brand": "BMW"},
    {"brand": "MERCEDES-BENZ"},
    {"brand": "AUDI"},
    {"brand": "PORSCHE"},
    {"brand": "VW"}
  ],
  "schedule": {
    "frequency": "monthly",
    "day_of_month": 1,
    "detection_mode": "full_scan"
  },
  "execution": {
    "max_execution_time_seconds": 900,
    "timeout_buffer_seconds": 180
  }
}
```

### Performance Characteristics

#### Cloud Function Limits (2nd Gen)
- **Maximum timeout**: 60 minutes (3600 seconds)
- **Memory**: Configurable (default 256MB)
- **Concurrent executions**: Configurable
- **Cold start time**: ~5-10 seconds

#### Timeout Strategy
- **Global timeout**: 3600 seconds (Cloud Function max)
- **Global buffer**: 180 seconds (stop 3 min before timeout)
- **Per-supplier timeout**: Configurable (300-900 seconds)
- **Per-supplier buffer**: Configurable (60-180 seconds)

#### Execution Time Estimates
| Supplier | Files | Avg Time | Max Time | Notes |
|----------|-------|----------|----------|-------|
| NEOPARTA | 3-5 | 120s | 300s | API-based, fast |
| APF | 10-20 | 300s | 600s | Link download |
| BRECHMANN | All | 600s | 900s | Full scan monthly |
| CONNEX | 5-10 | 150s | 300s | Simple CSV |
| TECHNOPARTS | 10-15 | 300s | 600s | XLSX downloads |
| MATEROM | 10-20 | 300s | 600s | Custom scraper |

### Resumable Downloads

#### Interruption Handling
1. **Detection**: `ExecutionMonitor.should_stop()` returns True
2. **Save State**: Store `last_file_index` in supplier state
3. **Mark Interrupted**: Set `interrupted: true` flag
4. **Next Run**: Check interrupted flag, resume from last_file_index

#### Resume Process
```python
# Check state on scraper start
supplier_state = state_manager.get_supplier_state(supplier)
if supplier_state['interrupted']:
    start_index = supplier_state['last_file_index']
    logger.info(f"Resuming from file {start_index}")
    
    # Skip already processed files
    files_to_process = all_files[start_index:]
```

### State Persistence

#### Storage Location
- **Production**: Google Cloud Storage (state bucket)
- **Development**: Local file (`src/scraper_state.json`)

#### State Update Frequency
- **During execution**: After each file processed (optional)
- **On interruption**: Immediately when timeout detected
- **On completion**: Clear interrupted flag, update version

#### State Size Management
- Keep only recent downloaded_files (90 day retention)
- Prune old entries periodically via cleanup_old_files()
- Minimal state structure (supplier_filename, valid_from_date, drive_file_id, timestamp only)

### Error Handling

#### Error Philosophy (Same as Email Processing)
- **Continue on error**: Don't stop if one supplier fails
- **Report all errors**: Include in summary email
- **Graceful degradation**: Process what we can

#### Timeout Handling
- **Not an error**: Timeouts are expected for large suppliers
- **Resume automatically**: Next run continues from last position
- **Warning in report**: "Execution interrupted - X files processed"

### Testing Strategy

#### Local Testing
```bash
# Test with short timeout to simulate interruption
python scripts/run_scraper_local.py \
    --supplier NEOPARTA \
    --dry-run \
    --max-execution-time 30
```

#### Dry Run Mode
- Simulates scraping without Drive upload
- Validates brand filtering
- Tests timeout handling
- Checks schedule evaluation

#### Production Testing
- Start with one supplier
- Monitor execution times
- Verify state persistence
- Check resume functionality

### Monitoring & Alerts

#### Key Metrics
- Execution time per supplier
- Files downloaded per supplier
- Success/failure rate
- Timeout frequency
- Resume rate

#### Logging
- Structured logging with supplier/brand context
- Execution progress tracking
- Timeout warnings
- State changes logged

#### Summary Email
- Include scraping results alongside email processing
- Report interrupted executions
- Show brand filtering statistics
- List archived files

End of document.

---

## Historical Architecture Note (Email Processing Flow)

The email automation flow summarized for reference:

```
Suppliers → Google Group (Collaborative Inbox)
          → Cloud Function / Cloud Run (scheduled)
              1) Authenticate via service account (DWD)
              2) Read emails since last processed timestamp
              3) Identify supplier rules by domain
              4) Parse attachments (CSV/XLSX)
              5) Transform and generate CSV
              6) Upload to Google Drive brand folder
              7) Send summary email
```

This mirrors the earlier high-level document and is retained here to keep architecture details in a single source.

### Historical Documents

- Original CLI tool design: `docs/design/archive/original_design.md`
