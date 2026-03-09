# Web Scraper Architecture

## Templates vs Supplier Scrapers

The scraper architecture has two locations:

1. **`src/scrapers/templates/`** - Reusable templates for common patterns
2. **`src/scrapers/supplier_scrapers/`** - Dedicated supplier-specific implementations

### When to Use Templates

Use templates for **truly generic patterns** that:
- Apply to multiple suppliers with minimal customization
- Can be fully configured via `config/scraper/scraper_config.json`
- Don't require supplier-specific business logic
- Share the exact same authentication and API flow

**Current templates:**
- `link_downloader_scraper.py` - For suppliers with direct download links (CONNEX, TECHNOPARTS)

### When to Use Supplier Scrapers

Use dedicated supplier scrapers for:
- Unique authentication flows
- Supplier-specific business logic
- Complex integrations that don't fit a template
- Any supplier with special requirements

**When in doubt, use a dedicated supplier scraper.** It's better to have clear, maintainable code for each supplier than to create overly complex templates.

## Common Scraper Patterns

| Pattern | Authentication | API Calls | Browser | Example | Where |
|---------|---------------|-----------|---------|---------|-------|
| **Pure REST** | Static API key from env | Yes | No | APF/Wiuse | `supplier_scrapers/apf_scraper.py` |
| **Hybrid Browser+API** | Browser login → harvest token | Yes | Yes (auth only) | NEOPARTA | `supplier_scrapers/neoparta_scraper.py` |
| **Link Downloader** | Browser login | No | Yes | CONNEX, TECHNOPARTS | `templates/link_downloader_scraper.py` |

## Naming Convention

Supplier scrapers MUST follow this naming:

- **File:** `{supplier_name}_scraper.py` (lowercase)
- **Class:** `{SupplierName}Scraper` (PascalCase)
- **Example:** `apf_scraper.py` with class `ApfScraper`

The `scraper_factory.py` automatically discovers scrapers by matching supplier names.

## Key Design Principles

### 1. Don't Pollute Templates with Supplier-Specific Logic

APF (pure REST) and NEOPARTA (browser+API) have fundamentally different patterns. They should be separate implementations, not a single template with if/else branches.

### 2. Each Supplier's Authentication is Unique

- Some use static API keys from environment
- Some use browser login to harvest cookies
- Some use browser login to harvest localStorage tokens
- These can't be cleanly unified without creating a mess

### 3. Separation of Concerns Matters

- **Templates** are for reusable patterns across suppliers
- **Supplier scrapers** are for supplier-specific implementations
- Don't try to make one supplier scraper generic enough to be a template

## Implementation Guidelines

### Scraper Base Class

All scrapers inherit from `BaseScraper` which provides:
- State management
- Drive upload functionality
- Logging
- Error handling

### Required Methods

```python
class SupplierScraper(BaseScraper):
    def __init__(self, supplier_config: Dict[str, Any], ...):
        super().__init__(supplier_name="SUPPLIER", ...)
        
    def scrape(self) -> None:
        """Main scraping logic"""
        pass
        
    def cleanup(self) -> None:
        """Clean up resources (browser, sessions)"""
        pass
```

### Authentication Patterns

**Pure API (no browser):**
```python
def __init__(self, ...):
    self.api_key = os.getenv('SUPPLIER_API_KEY')
    self.session = requests.Session()
```

**Browser + API (hybrid):**
```python
def __init__(self, ...):
    self.browser_manager = BrowserManager(...)
    self.session = requests.Session()
    
def scrape(self):
    # 1. Login via browser
    self._login_via_browser()
    # 2. Extract token/cookies
    token = self._extract_auth_token()
    # 3. Use API with token
    self._fetch_via_api(token)
```

**Pure Browser (link downloader):**
```python
def __init__(self, ...):
    self.browser_manager = BrowserManager(...)
    
def scrape(self):
    # 1. Login
    self._login()
    # 2. Navigate to downloads page
    self._navigate_to_downloads()
    # 3. Download files directly
    self._download_files()
```

## File Organization

```
src/scrapers/
├── base_scraper.py              # Base class
├── scraper_factory.py           # Auto-discovery
├── templates/
│   └── link_downloader_scraper.py
└── supplier_scrapers/
    ├── apf_scraper.py           # Pure API
    ├── neoparta_scraper.py      # Browser + API
    ├── autocar_scraper.py       # Browser + HTML parsing
    └── ...
```

## Configuration

Web scraper suppliers are configured in `config/scraper/scraper_config.json`:

```json
{
  "supplier": "SUPPLIER_NAME",
  "enabled": true,
  "location": "BELGIUM",
  "currency": "EUR",
  "type": "api|link_downloader|custom",
  "custom_scraper_class": "scrapers.supplier_scrapers.supplier_scraper.SupplierScraper",
  "config": [
    {"brand": "BMW"},
    {"brand": "AUDI"}
  ]
}
```

**Note:** This is distinct from email suppliers, which are configured in `config/supplier/supplier_config.json`.

## Testing

Test scrapers locally:
```bash
python scripts/run_scraper_local.py --supplier SUPPLIER_NAME --use-test-config
```

Second run should skip all files (duplicate detection):
```bash
python scripts/run_scraper_local.py --supplier SUPPLIER_NAME
python scripts/run_scraper_local.py --supplier SUPPLIER_NAME  # Should skip all
```

## Error Handling

- Log errors with context
- Don't fail entire scrape for one brand
- Clean up resources in `cleanup()` method
- Use try/finally for browser cleanup

```python
def scrape(self) -> None:
    try:
        for brand_config in self.config:
            try:
                self._process_brand(brand_config)
            except Exception as e:
                logger.error(f"Failed to process {brand_config['brand']}: {e}")
                continue  # Don't fail entire scrape
    finally:
        self.cleanup()
```

## Enforcement

- New suppliers start with dedicated scrapers
- Only create templates when 3+ suppliers share exact same pattern
- Code reviews verify proper separation of concerns
- Test duplicate detection works correctly
