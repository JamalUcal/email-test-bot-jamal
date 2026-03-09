# Extending Email Parser

Technical guide for adding new email parsing patterns to the Email Pricing Bot system.

## Overview

The email parser extracts structured data from supplier emails and their attachments. This guide covers how to extend the parser to handle new supplier patterns, detection strategies, and parsing rules.

**Key Parsing Components**:
- Supplier detection (3-layer strategy)
- Brand detection (filename, subject, body)
- Currency detection (5-layer hierarchy)
- Date parsing (multiple formats)
- Column header detection (intelligent matching)

## Supplier Detection

The system uses a three-layer strategy to identify the supplier for each email, prioritizing explicit tagging over automatic detection.

### Layer 1: Body Tag Detection (Highest Priority)

Internal team members can manually tag emails with the supplier name using this format in the email body:

```
SUPPLIER: <supplier_name>
```

**Features**:
- Case-insensitive matching
- Validates against configured suppliers in `supplier_config.json`
- Supports future extensibility with additional tags (e.g., `BRAND:`, `EXPIRY:`)
- Useful when forwarding emails from unrecognized domains or when supplier domain is ambiguous

**Example**:
```
SUPPLIER: DEXTAR

Please process the attached price list.
```

**Implementation**: `src/gmail/email_processor.py` - `_detect_supplier_from_body_tag()`

### Layer 2: Forwarded Email Parsing (Medium Priority)

When internal team members forward supplier emails to the group, the system automatically extracts the original sender.

**Supported Formats**:
- **Gmail**: `---------- Forwarded message ---------` followed by `From: email@supplier.com`
- **Outlook**: `From:` line with email address

**Process**:
1. Parse email body for forward markers
2. Extract original sender email address
3. Match domain against `email_domain` in `supplier_config.json`
4. Skip internal ucalexports.com addresses

**Example Gmail Forward**:
```
---------- Forwarded message ---------
From: sales@apf.com
Date: Mon, Nov 18, 2024 at 2:30 PM
Subject: October Price List
...
```

**Implementation**: `src/gmail/email_processor.py` - `_detect_supplier_from_forwarded_email()`

### Layer 3: Direct From Header (Fallback)

For emails sent directly to the group (not forwarded), use the standard From header.

**Process**:
1. Extract sender domain from From header
2. Match against `email_domain` in `supplier_config.json`

**Implementation**: `src/gmail/email_processor.py` - `_detect_supplier_from_header()`

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

**Example Summary Entry**:
```
From: gopika@ucalexports.com
Original Sender: sales@apf.com
Detection Method: Forwarded Email
```

### Adding New Detection Methods

To add a new detection method:

1. **Add detection method** in `src/gmail/email_processor.py`:
```python
def _detect_supplier_custom(self, message: Dict) -> Optional[str]:
    """Custom detection logic."""
    # Your detection logic here
    return supplier_name
```

2. **Update detection flow** in `_identify_supplier()`:
```python
def _identify_supplier(self, message: Dict) -> Tuple[Optional[str], str]:
    # Try custom method
    supplier = self._detect_supplier_custom(message)
    if supplier:
        return supplier, 'custom'
    
    # Fall back to existing methods...
```

3. **Update supplier config** if needed to support new matching criteria

## Brand Detection

Brand names are detected from multiple sources with a priority order.

### Detection Priority

1. **Filename** (Highest Priority)
   - Searches attachment filename for brand names
   - Example: `BMW_Price_List_October.xlsx` → `BMW`

2. **Subject Line** (Medium Priority)
   - Searches email subject for brand names
   - Example: `GM October 2025 Price File` → `GM`

3. **Email Body** (Low Priority)
   - Searches email body text for brand names
   - Less reliable, used as fallback

4. **Default Brand** (Fallback)
   - Uses supplier's default brand from config
   - Reported as warning in summary

### Brand Matching

Brand matching uses:
- **Exact match**: Case-insensitive comparison
- **Alias matching**: Matches against brand aliases in `brand_config.json`
- **Normalization**: Converts to uppercase, removes special characters

**Example**:
```json
{
  "brand": "MERCEDES-BENZ",
  "aliases": ["MERCEDES", "MERCEDESBENZ", "MB", "BENZ"]
}
```

The brand detector will match:
- `mercedes` → `MERCEDES-BENZ`
- `Mercedes-Benz` → `MERCEDES-BENZ`
- `MB` → `MERCEDES-BENZ`

### Implementation

**File**: `src/parsers/brand_detector.py`

**Key Method**:
```python
def detect_brand(
    self,
    filename: str,
    subject: str,
    body: Optional[str] = None,
    default_brand: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], str, bool]:
    """
    Detect brand from email content.
    
    Returns:
        Tuple of (config_brand, matched_text, source, used_fallback)
    """
```

**Usage**:
```python
from parsers.brand_detector import BrandDetector

detector = BrandDetector(brand_configs)
brand, matched_text, source, used_fallback = detector.detect_brand(
    filename="BMW_Price_List.xlsx",
    subject="October Price List",
    body=email_body,
    default_brand="BMW"  # From supplier config
)
```

### Adding New Brand Detection Patterns

1. **Add brand aliases** in `config/brand/brand_config.json`:
```json
{
  "brand": "BMW",
  "aliases": ["BMW AG", "BMW Group", "Bayerische Motoren Werke"]
}
```

2. **Custom detection logic** (if needed) in `src/parsers/brand_detector.py`:
```python
def _find_brand_in_text(self, text: str) -> Optional[Tuple[str, str]]:
    """Find brand in text with custom patterns."""
    # Add custom regex patterns here
    pattern = r'CustomPattern(\w+)'
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        matched_text = match.group(1)
        # Look up in brand_lookup
        brand_config = self.brand_lookup.get(matched_text.upper())
        if brand_config:
            return matched_text, brand_config['brand']
    return None
```

## Currency Detection

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

**4a. Currency Column Values (Priority)**

Checks if the file has a dedicated currency column (e.g., header "CURRENCY") and reads the values:
- Column header: `CURRENCY`, `CUR`, `Currency Code`
- Column values: `USD`, `EUR`, `GBP`, etc.

Example file structure:
```
Part Numbers | Description    | CURRENCY | Price
55401        | Adapter cap on | USD      | 125.80
55407        | RADIATOR TESTER| USD      | 440.68
```

The system matches column values against the supplier's allowed currencies (codes and aliases from `currency_config.json`).

**4b. Price Column Header**

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

**4c. Excel Cell Number Format (Excel files only)**

For Excel files (.xlsx, .xls), checks the price cell's number format string for currency symbols:
- `_("$"* #,##0.00_)` → $ → USD
- `[$€-407]#,##0.00` → € → EUR
- `#,##0.00 "USD"` → USD
- `£#,##0.00` → £ → GBP

Many Excel files store currency symbols in the cell's number format rather than in the actual cell value. The system extracts the symbol/code from the format string and uses scoped detection to match it against the supplier's configured currencies.

**Scoped Symbol Matching**: When a symbol like `$` could represent multiple currencies (USD, SGD), the system finds all matching currencies and filters to only those in the supplier's configuration, ensuring correct detection.

**4d. Price Column Data (Fallback)**

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

### Implementation

**File**: `src/parsers/currency_detector.py`

**Key Methods**:
```python
def detect_currency(
    self,
    email_body: Optional[str],
    subject: str,
    filename: str,
    supplier_config: Dict,
    brand: str,
    file_path: Optional[str] = None
) -> Tuple[Optional[str], str]:
    """Detect currency using 5-layer hierarchy."""
    
def detect_currency_from_tag(self, email_body: Optional[str]) -> Optional[str]:
    """Layer 1: Check for CURRENCY: tag."""
    
def detect_currency_from_text(self, text: str) -> Optional[str]:
    """Layer 3: Check subject/filename."""
    
def detect_currency_from_file(self, file_path: str) -> Optional[str]:
    """Layer 4: Check file content."""
```

### Adding New Currency Support

1. **Add currency** to `config/core/currency_config.json`:
```json
{
  "code": "CHF",
  "name": "Swiss Franc",
  "symbol": "Fr",
  "aliases": ["CHF", "Swiss Franc", "Franc"]
}
```

2. **Add symbol mapping** in `src/parsers/currency_detector.py`:
```python
self.symbol_to_code = {
    # ... existing symbols ...
    'Fr': 'CHF',  # Swiss Franc
}
```

3. **Add parameterized variants** (if needed) in `config/core/column_mapping_config.json`:
```json
{
  "price": {
    "variants": [
      "UnitPrice<CURRENCY_CODE>",
      "<CURRENCY_CODE>RATE",
      "Price-<CURRENCY_CODE>",
      "PriceCHF"  // Add specific variant if needed
    ]
  }
}
```

## Date Parsing

The system parses expiry dates and valid-from dates from email content, supporting multiple date formats.

### Expiry Date Parsing

**Priority Order**:
1. Email body text (searches for keywords)
2. Supplier config default (`default_expiry_days`)
3. System default duration (from core config)

**Supported Formats**:
- "October 23, 2025"
- "23/10/2025"
- "2025-10-23"
- "Valid until Oct 23, 2025"
- "23 Oct 25"
- "23 Oct"
- "23 October"

**Keywords Detected**:
- `valid until`
- `expires`
- `expiry`
- `valid through`
- `valid till`
- `effective until`
- `price valid until`
- `prices valid until`

**Implementation**: `src/parsers/date_parser.py` - `parse_expiry_date()`

**Example**:
```python
from parsers.date_parser import DateParser

parser = DateParser(timezone='UTC')
expiry_date, source, used_fallback = parser.parse_expiry_date(
    email_body="Price File Expires October 23, 2025",
    email_date=datetime.now(),
    default_days=90,
    system_default_days=90
)
```

### Valid-From Date Parsing

**Priority Order**:
1. Email body text (searches for "valid from" patterns)
2. Email received date (fallback)

**Supported Patterns**:
- `valid from: [date]`
- `effective from: [date]`
- `prices effective: [date]`

**Implementation**: `src/parsers/date_parser.py` - `parse_valid_from_date()`

### Adding New Date Formats

1. **Add format pattern** in `src/parsers/date_parser.py`:
```python
def _parse_date_string(self, date_str: str, reference_date: datetime) -> Optional[datetime]:
    """Parse date string with multiple format attempts."""
    formats = [
        # ... existing formats ...
        '%d-%m-%Y',  # 23-10-2025
        '%Y.%m.%d',  # 2025.10.23
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    
    # Fallback to dateutil parser
    try:
        return dateutil_parser.parse(date_str)
    except:
        return None
```

2. **Add keyword** (if needed) in `DateParser.EXPIRY_KEYWORDS`:
```python
EXPIRY_KEYWORDS = [
    # ... existing keywords ...
    'price valid through',
    'validity ends',
]
```

## Column Header Detection

The system uses intelligent header detection to automatically identify column mappings in price list files, eliminating the need for hardcoded column positions.

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

### Configuration Example

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

### Implementation

**File**: `src/parsers/header_detector.py`

**Key Class**: `HeaderDetector`

**Key Method**:
```python
def detect_headers(
    self, 
    file_path: str,
    matched_brand_text: Optional[str] = None,
    currency_detector: Optional[Any] = None
) -> DetectedHeaders:
    """
    Detect column headers in a price list file.
    
    Returns:
        DetectedHeaders with column mappings and metadata
    """
```

### Adding New Header Patterns

1. **Add exact variant** in `config/core/column_mapping_config.json`:
```json
{
  "part_number": {
    "variants": [
      "part number",
      "partno",
      "part_no",
      "SKU"  // Add new variant
    ]
  }
}
```

2. **Add parameterized variant** (if brand/currency-specific):
```json
{
  "price": {
    "variants": [
      "UnitPrice<CURRENCY_CODE>",
      "<BRAND>Price"  // Brand-specific variant
    ]
  }
}
```

3. **Add wildcard pattern** (for unusual headers):
```json
{
  "price": {
    "wildcard_variants": [
      "%price%<CURRENCY_CODE>%",
      "%cost%<CURRENCY_CODE>%"  // New wildcard pattern
    ]
  }
}
```

4. **Add exclusion keyword** (if needed):
```json
{
  "price": {
    "exclusions": [
      "total",
      "totalprice",
      "subtotal"  // New exclusion
    ]
  }
}
```

## Configuration Reference

### Supplier Configuration

Each supplier in `config/supplier/supplier_config.json` can have:

```json
{
  "supplier": "SUPPLIER_NAME",
  "email_domain": "supplier.com",
  "email_address": "supplier@gmail.com",  // Alternative to domain
  "email_addresses": ["email1@domain.com", "email2@domain.com"],  // Multiple emails
  "default_brand": "BMW",  // Fallback brand
  "default_expiry_days": 90,
  "discount_percent": 5.0,
  "metadata": {
    "location": "GERMANY",
    "currency": "EUR",
    "decimalFormat": "decimal"
  },
  "config": [
    {
      "brand": "BMW"
      // location, currency, decimalFormat inherited from metadata
      // Can override at brand level if needed
    }
  ]
}
```

### Brand Configuration

Each brand in `config/brand/brand_config.json`:

```json
{
  "brand": "BMW",
  "aliases": ["BMW AG", "BMW Group"],
  "minimumPartLength": 10,
  "driveFolderId": "FOLDER_ID_HERE"
}
```

### Currency Configuration

Currencies in `config/core/currency_config.json`:

```json
{
  "code": "USD",
  "name": "US Dollar",
  "symbol": "$",
  "aliases": ["US", "US$", "Dollar"]
}
```

### Column Mapping Configuration

Column mappings in `config/core/column_mapping_config.json`:

```json
{
  "fields": {
    "part_number": {
      "variants": ["part number", "partno", "SKU"],
      "wildcard_variants": ["%part%"],
      "exclusions": []
    },
    "price": {
      "variants": ["price", "unit price", "UnitPrice<CURRENCY_CODE>"],
      "wildcard_variants": ["%price%<CURRENCY_CODE>%"],
      "exclusions": ["total", "totalprice"]
    }
  },
  "header_detection": {
    "max_blank_rows_to_skip": 10
  }
}
```

## Adding New Parsing Rules

### Step-by-Step Process

1. **Identify the pattern**:
   - What makes this supplier/format unique?
   - What detection/parsing is needed?

2. **Choose the right component**:
   - Supplier detection → `email_processor.py`
   - Brand detection → `brand_detector.py`
   - Currency detection → `currency_detector.py`
   - Date parsing → `date_parser.py`
   - Header detection → `header_detector.py`

3. **Update configuration** (preferred):
   - Add patterns to JSON config files
   - No code changes needed for most cases

4. **Update code** (if needed):
   - Add custom logic only if config-based approach insufficient
   - Follow existing patterns and type hints

5. **Test thoroughly**:
   - Test with sample emails/files
   - Verify all detection layers work
   - Check error handling

6. **Update documentation**:
   - Document new patterns
   - Add examples

### Example: Adding New Supplier Email Pattern

**Scenario**: Supplier uses custom email format `price-list-{supplier}@domain.com`

**Solution**:

1. **Update supplier config**:
```json
{
  "supplier": "NEW_SUPPLIER",
  "email_addresses": [
    "price-list-newsupplier@domain.com",
    "pricing@newsupplier.com"
  ]
}
```

2. **No code changes needed** - existing email address matching handles this

### Example: Adding New Date Format

**Scenario**: Supplier uses format "DD.MM.YYYY" (e.g., "23.10.2025")

**Solution**:

1. **Update date parser** in `src/parsers/date_parser.py`:
```python
def _parse_date_string(self, date_str: str, reference_date: datetime) -> Optional[datetime]:
    formats = [
        # ... existing formats ...
        '%d.%m.%Y',  # 23.10.2025
    ]
    # ... rest of method
```

2. **Test with sample dates**:
```python
parser = DateParser()
result = parser._parse_date_string("23.10.2025", datetime.now())
assert result is not None
```

## Testing

### Unit Testing

Test individual parser components:

```python
# Test brand detection
from parsers.brand_detector import BrandDetector

detector = BrandDetector(brand_configs)
brand, text, source, fallback = detector.detect_brand(
    filename="BMW_List.xlsx",
    subject="Price List"
)
assert brand == "BMW"
assert source == "filename"

# Test currency detection
from parsers.currency_detector import CurrencyDetector

detector = CurrencyDetector(currency_configs)
currency, source = detector.detect_currency(
    email_body="CURRENCY: EUR",
    subject="Price List",
    filename="list.xlsx",
    supplier_config={},
    brand="BMW"
)
assert currency == "EUR"
assert source == "email_tag"
```

### Integration Testing

Test with real email samples:

```bash
# Run email processor with test email
python scripts/run_email_local.py --force --dry-run --max-emails 1
```

### Debugging

Enable debug logging:

```python
import logging
logging.getLogger('parsers').setLevel(logging.DEBUG)
```

## Related Documentation

- [Design and Implementation](./DESIGN_AND_IMPLEMENTATION.md) - System architecture
- [User Guide: Setup and Running](./USER_GUIDE_SETUP_AND_RUNNING.md) - Setup procedures

