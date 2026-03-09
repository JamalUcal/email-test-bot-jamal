# Email Pricing Bot - Implementation

This directory contains the implementation of the automated email pricing bot that processes supplier price lists from a Google Group mailbox.

## Project Structure

```
src/
├── main.py                 # Cloud Function entry point
├── config/
│   ├── config_manager.py   # Configuration loading and validation
│   └── schemas.py          # Configuration schemas
├── email/
│   ├── gmail_client.py     # Gmail API integration
│   ├── email_processor.py  # Email parsing and filtering
│   └── attachment_handler.py # Attachment download and management
├── parser/
│   ├── file_parser.py      # CSV/XLSX parsing
│   ├── brand_detector.py   # Brand name detection from filename/subject
│   └── date_parser.py      # Expiry date parsing
├── transform/
│   ├── data_transformer.py # Data cleaning and transformation
│   └── validator.py        # Data validation
├── output/
│   ├── drive_uploader.py   # Google Drive upload
│   └── file_generator.py   # Output CSV generation
├── notification/
│   ├── email_sender.py     # Summary email generation and sending
│   └── report_builder.py   # Report formatting
├── utils/
│   ├── logger.py           # Logging utilities
│   ├── state_manager.py    # GCS-based state management
│   └── exceptions.py       # Custom exceptions
├── requirements.txt        # Python dependencies
└── config_samples/         # Sample configuration files
    ├── core_config.json
    ├── supplier_config.json
    └── brand_config.json
```

## Architecture Overview

### Execution Flow
1. **Scheduled Trigger**: Cloud Scheduler triggers the function every hour
2. **Config Check**: Reads config from GCS to determine if execution should proceed (24-hour check)
3. **Email Processing**: Connects to Gmail via service account with domain-wide delegation
4. **State Management**: Reads last processed timestamp from GCS
5. **Email Filtering**: Filters emails by supplier domain
6. **Attachment Processing**: Downloads and parses CSV/XLSX files
7. **Data Transformation**: Applies supplier-specific transformations
8. **Output Generation**: Creates formatted CSV files
9. **Drive Upload**: Uploads to brand-specific folders
10. **Summary Email**: Sends daily summary with statistics, errors, warnings, and successes
11. **State Update**: Updates last processed timestamp in GCS

### Key Features
- **Streaming Processing**: Handles up to 500,000 rows efficiently
- **Multiple Formats**: Supports CSV and XLSX (warns on PDF/XLS)
- **Brand Detection**: Filename → Subject line → Reject if not found
- **Date Parsing**: Multiple date formats with fallback to supplier/system defaults
- **Error Handling**: Comprehensive error tracking and reporting
- **Manual Triggering**: HTTP endpoint for manual execution

## Configuration Files

### 1. Core Config (`core_config.json`)
Stored in GCS bucket, contains:
- Gmail mailbox to monitor
- Service account details
- Secret Manager references
- Google Drive settings
- Summary email recipients
- Execution schedule (time in IST)
- System defaults (expiry duration, etc.)
- Own domain (for filtering)

### 2. Supplier Config (`supplier_config.json`)
Extended from original with:
- Email domain for matching
- Discount percentage
- Optional default brand (fallback)
- Optional default expiry duration
- Parsing rules per brand

### 3. Brand Config (`brand_config.json`)
Matches original `Brand_partNumber.json`:
- Brand name and aliases
- Minimum part number length
- Google Drive folder ID

## Development Stages

### Stage 1: Infrastructure Setup (Current)
- [x] Project structure
- [ ] GCP setup documentation
- [ ] Configuration schemas

### Stage 2: PoC - Email Detection
- [ ] Gmail API integration
- [ ] Email filtering by domain
- [ ] Attachment detection
- [ ] Basic summary email

### Stage 3: File Parsing
- [ ] CSV/XLSX parsing with streaming
- [ ] Brand detection
- [ ] Date parsing
- [ ] Exception handling

### Stage 4: File Writing
- [ ] Data transformation
- [ ] Output file generation
- [ ] Google Drive upload
- [ ] Enhanced summary email

### Stage 5: Email Parsing Enhancement
- [ ] Brand name extraction from email
- [ ] Expiry date extraction from email body
- [ ] Complete summary email with all sections

## Technology Stack

- **Runtime**: Python 3.11+
- **Cloud Platform**: Google Cloud Platform
- **Services**: Cloud Functions, Cloud Scheduler, Secret Manager, Cloud Storage
- **APIs**: Gmail API, Google Drive API
- **Libraries**: 
  - `google-api-python-client` - Gmail/Drive APIs
  - `pandas` - Data processing
  - `openpyxl` - XLSX parsing
  - `python-dateutil` - Date parsing

## Deployment

Cloud Function will be deployed with:
- **Memory**: 2GB (for large file processing)
- **Timeout**: 540s (9 minutes)
- **Trigger**: Cloud Scheduler (hourly)
- **Environment**: Python 3.11
- **Region**: Configurable (recommend same as GCS bucket)

## Security

- Service account JSON key stored in Secret Manager
- Domain-wide delegation for Gmail access
- Minimal Gmail scopes: `gmail.readonly` + `gmail.send`
- Drive scope: `drive.file` (only files created by the app)
- All credentials in Secret Manager, never in code

## Cost Estimate (Phase 1)

| Service | Usage | Monthly Cost |
|---------|-------|--------------|
| Cloud Functions | 720 invocations/month, ~2min avg | ~$0.50 |
| Cloud Scheduler | 1 job, hourly | Free |
| Secret Manager | 1 secret, ~720 accesses | ~$0.10 |
| Cloud Storage | ~1GB storage, minimal ops | ~$0.02 |
| Gmail API | Free | $0 |
| Drive API | Free | $0 |
| **Total** | | **~$0.62/month** |

Note: Actual costs may vary based on email volume and file sizes.
