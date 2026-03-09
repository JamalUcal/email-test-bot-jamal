# Email Pricing Bot - Architecture Explanation

## Overview

This is a **Python-based automation system** that runs on Google Cloud Platform (GCP). Think of it as an automated robot that:

1. **Checks emails** from suppliers containing price lists (Excel/CSV files)
2. **Parses** these files to extract part numbers and prices
3. **Uploads** the processed data to Google Drive
4. **Sends** summary reports via email

---

## High-Level Architecture (Simple View)

```
┌─────────────────────────────────────────────────────────────────────┐
│                     GOOGLE CLOUD PLATFORM                           │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────────────────┐  │
│  │ Cloud       │──▶│ Cloud       │──▶│ Email Pricing Bot      │  │
│  │ Scheduler   │   │ Function    │   │ (Python Application)   │  │
│  └─────────────┘   └─────────────┘   └───────────┬─────────────┘  │
│                                                  │                  │
│  ┌──────────────────────────────────────────────▼────────────────┐ │
│  │                    GOOGLE APIS                                  │ │
│  │  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌───────────┐  │ │
│  │  │   Gmail  │  │   Drive   │  │  Secret  │  │   Cloud   │  │ │
│  │  │    API   │  │    API    │  │ Manager  │  │  Storage  │  │ │
│  │  └────┬─────┘  └─────┬─────┘  └────┬─────┘  └─────┬─────┘  │ │
│  └───────┼──────────────┼─────────────┼──────────────┼────────┘ │
└──────────┼──────────────┼─────────────┼──────────────┼───────────┘
           │              │             │              │
           ▼              ▼             ▼              ▼
      ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
      │ Supplier │   │  Brand  │   │ Service │   │ State & │
      │  Emails │   │ Folders │   │ Account │   │  Config │
      └─────────┘   └─────────┘   └─────────┘   └─────────┘
```

---

## Key Components (VB.NET Developer Perspective)

### 1. **Entry Point** - `main.py`
Similar to an ASP.NET HTTP Handler or Azure Function
- Receives HTTP requests from Cloud Scheduler
- Routes to different workflows (email, scraping, or unified)

### 2. **Unified Orchestrator** - `unified_orchestrator.py`
Think of this as the **Main()** method or a **Workflow Engine**
- Coordinates the entire process
- Loads configuration
- Initializes all required services
- Calls email processing or web scraping as needed

### 3. **Configuration Manager** - `config/config_manager.py`
Similar to **ConfigurationManager** or **appsettings.json** in .NET
- Loads JSON configuration from Google Cloud Storage
- Manages supplier configs, brand configs, and core settings

---

## Email Processing Pipeline

This is the main workflow - like a VB.NET Windows Service processing a queue:

```
Email Received → Parse Email → Download Attachment → 
Detect Brand → Detect Currency → Parse Price List → 
Generate CSV → Upload to Drive → Send Summary Email
```

### Key Classes:

| Class | VB.NET Equivalent | Purpose |
|-------|------------------|---------|
| `GmailClient` | `SmtpClient` / Graph API | Connect to Gmail, fetch emails |
| `EmailProcessor` | `IMessageProcessor` | Parse email content, find attachments |
| `AttachmentHandler` | `FileDownloader` | Download Excel/CSV files |
| `BrandDetector` | `PatternMatcher` | Detect car brand from filename |
| `CurrencyDetector` | `CurrencyParser` | Detect USD, EUR, JPY, etc. |
| `PriceListParser` | `CSVReader` / `ExcelReader` | Parse price list files |
| `FileGenerator` | `CSVWriter` | Generate standardized CSV |
| `DriveUploader` | `OneDrive API` | Upload to Google Drive |
| `EmailSender` | `SmtpClient` | Send summary reports |

---

## Web Scraping Pipeline

Secondary workflow for websites that don't send emails:

```
Check Schedule → Open Browser → Navigate to Site → 
Login (if needed) → Download Price List → 
Process through Email Pipeline → Upload to Drive
```

### Key Classes:

| Class | Purpose |
|-------|---------|
| `WebScrapingOrchestrator` | Main coordinator for scraping |
| `BrowserManager` | Manages Chrome/Edge browser (Selenium-like) |
| `ScraperFactory` | Factory pattern - creates supplier-specific scrapers |
| `ScheduleEvaluator` | Determines when each supplier should run |

---

## Configuration System

### Three Main Config Files (JSON):

1. **Core Config** (`core_config.json`)
   - Like `web.config` - general settings
   - Gmail settings, Drive settings, secrets location

2. **Supplier Config** (`supplier_config.json`)
   - Like a database table of suppliers
   - 200+ suppliers with their settings, domains, discounts

3. **Brand Config** (`brand_config.json`)
   - Like a lookup table of car brands
   - Brand names, aliases, Drive folder IDs

---

## Data Flow Example

```
Email from "supplier@company.com" 
    ↓
GmailClient finds it (domain matches supplier config)
    ↓
AttachmentHandler downloads "prices.xlsx"
    ↓
BrandDetector finds "TOYOTA" in filename
    ↓
PriceListParser reads Excel, finds columns (PartNumber, Price)
    ↓
FileGenerator creates "TOYOTA_SUPPLIER_USD_2025.csv"
    ↓
DriveUploader uploads to "Toyota" folder in Google Drive
    ↓
EmailSender sends summary: "1 file processed, 5000 rows"
```

---

## How It's Deployed

- **Cloud Function**: Python 3.11, 2GB memory
- **Trigger**: Cloud Scheduler (runs every hour)
- **Storage**: Google Cloud Storage for configs and state
- **Auth**: Service account with domain-wide delegation

---

## Error Handling

Similar to Try-Catch-Log in VB.NET:

- **Logger**: Structured logging (like Serilog)
- **Exceptions**: Custom exception classes
- **State Management**: Tracks last processed email to avoid duplicates
- **Summary Email**: Always sends report with errors/warnings/successes

---

## Summary for VB.NET Developer

Think of this as:

- A **Windows Service** running on cloud
- Using **JSON configuration** instead of XML
- **Async/await** patterns (Python's asyncio) instead of Task-based operations
- **Factory pattern** for creating scrapers
- **Pipeline pattern** for processing (like Azure Data Factory)
- **Google Cloud Storage** instead of local files
- **Gmail API** instead of Exchange/Outlook

The concepts are the same - it's just Python syntax and Google APIs instead of .NET!
