3,. # 🆕 New Supplier Onboarding - Prompt Template

**Purpose**: Copy and fill out this template to start a new AI-assisted supplier onboarding session.

---

# New Supplier Onboarding Request

I need help adding a new web scraper for supplier **[SUPPLIER_NAME]** to our email-pricing-bot system.

## 📋 Context

This is a production Python codebase that scrapes automotive parts pricing from multiple suppliers. We have 4 working scrapers:
- **NEOPARTA** - JSON API scraper (streaming)
- **TECHNOPARTS** - Excel file link downloader (streaming)
- **MATEROM** - WebDAV/Nextcloud custom scraper (streaming)
- **CONNEX** - CSV files with HTTP Basic Auth and HTML directory listings (streaming)

**CRITICAL**: We must be **extremely careful** not to break any existing working scrapers. Please verify that any changes to shared code (parsers, orchestrator, state manager, etc.) won't affect existing suppliers.

## 📖 Reference Documentation

Follow the comprehensive checklist: `SUPPLIER_ONBOARDING_CHECKLIST.md`

## 🔧 Supplier Details

### Basic Info
- **Supplier Name**: AUTOCAR
- **Website/Portal URL**: https://dashboard.autocar.nl/
- **Authentication Type**: Login Form
	- **Download Page URL**: https://dashboard.autocar.nl/customer/pricefiles/index
- **Underlying platform (if known)**: Magento
- **Price List Data Type**: HTML List
- **File download HTML:   <tr>
                <td>autocar_international_FORD_22_10_2025.xlsx</td> <-- File Name, Brand & Valid from
                <td>826240</td> <-- Number of rows (ignore)
                <td>FORD</td> <-- Brand
                <td>28 days</td> <-- Delivery duration (future usage)
                <td>22-10-2025</td> <-- Valid From
                <td>
                    <button type="button" class="btn btn-orange download-price-file" data-name="autocar_international_FORD_22_10_2025.xlsx" data-list-id="7104">
                        DOWNLOAD
                    </button>
                </td>
            </tr> 
- **File Type**: JSON (masquerading as XLSX)
- **File download URL: https://dashboard.autocar.nl/customer/pricefiles/download/?list=4059&_=1761817504913 i.e. 
https://dashboard.autocar.nl/customer/pricefiles/download/?list=<data-list-id>&_=<timestamp unix>
- **File download Response Example**: [["part","brand","description","delivery","price"],["91600K7530","Hyundai","<p>WIRING ASSY<\/p>","2 days","126.25"],["87650K70104X","Hyundai","<p>COVER ASSY<\/p>","2 days","11.89"],["00009480","HYUNDAI","HOLDER ASSY-FR T\/SIG LAMP","3 days","5.68"],["00012254","HYUNDAI","SOCKET-BULB","3 days","7.04"],["00015693","HYUNDAI","CAP-HEADLAMP DUST","3 days","4.06"],["00054744","HYUNDAI","CAP-HEADLAMP DUST","3 days","3.77"],["00062660","HYUNDAI","CAP ASSY-BOTTOM","3 days","4.68"],["00074597","HYUNDAI","INDICATOR ASSY-SHIFT LEVER (USA PART)","3 days","106.78"],["00080809","HYUNDAI","KNOB","3 days","1.82"],["00305ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","75.22"],["00305PUNCH","HYUNDAI","TOOL ASSY","3 days","39.71"],["00306ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","92.32"],["0060025023A","HYUNDAI","BOLT","3 days","0.86"],["006Y011SJ0","HYUNDAI","BEARING SET-CRANK SHAFT THRUST","3 days","16.43"],["0110000100","HYUNDAI","BRAKE OIL","3 days","15.87"],["0110EN1100","HYUNDAI","LUMBAR SUPPORT MOTOR","3 days","226.44"],["0210000100","HYUNDAI","OIL-GEAR","3 days","27.02"],["0210000110","HYUNDAI","OIL-LSD","3 days","32.77"],["0210000121","HYUNDAI","HYPOID GEAR OIL (FM PLUS)","3 days","101.28"],["0210000130","HYUNDAI","HYPOID GEAR OIL","3 days","101.24"],["0220000110","HYUNDAI","OIL-AXLE(80W\/90)","3 days","18.87"],["0220000140","HYUNDAI","FRICTION SYSTEM GEAR OIL","3 days","266.07"],["0310000100","HYUNDAI","OIL-P\/STRG","3 days","19.32"],["0310000130","HYUNDAI","OIL-P\/STRG","3 days","29.36"],["0310000140","HYUNDAI","OIL POWER STEERING PENTOSIN CHF202","3 days","23.68"],...
- **Scraper Type**: HTML List + JSON API download and parsing (direct to CSV as per Techoparts)

### Authentication Details
```
Username: .env SCRAPER_AUTOCAR_USERNAME
Password: .env SCRAPER_AUTOCAR_PASSWORD
Login URL: https://dashboard.autocar.nl/
Login Flow:
	User Name field: <input name="login[username]" value="" autocomplete="off" id="email" type="email" class="input-text" title="Email" data-validate="{required:true, 'validate-email':true}" aria-required="true">
	Password Field: <input name="login[password]" type="password" autocomplete="off" class="input-text" id="pass" title="Password" data-validate="{required:true}" aria-required="true">
	Login Button field: <button type="submit" class="action login primary" name="send" id="send2"><span>Sign In</span></button>
	Login PayLoad: form_key=8xM9z6o5H49d1ECi&login%5Busername%5D=vendorsupport%40ucalexports.com&login%5Bpassword%5D=Cp8f4myB7WUmkb3&send=

```

### File Discovery
```
How to find files:
- **Price List Data Type**: HTML List
- **File download HTML:   <tr>
                <td>autocar_international_FORD_22_10_2025.xlsx</td> <-- File Name, Brand & Valid from
                <td>826240</td> <-- Number of rows (ignore)
                <td>FORD</td> <-- Brand
                <td>28 days</td> <-- Delivery duration (future usage)
                <td>22-10-2025</td> <-- Valid From
                <td>
                    <button type="button" class="btn btn-orange download-price-file" data-name="autocar_international_FORD_22_10_2025.xlsx" data-list-id="7104">
                        DOWNLOAD
                    </button>
                </td>
            </tr> 
- **File Type**: JSON (masquerading as XLSX)
- **File download URL: https://dashboard.autocar.nl/customer/pricefiles/download/?list=4059&_=1761817504913 i.e. 
https://dashboard.autocar.nl/customer/pricefiles/download/?list=<data-list-id>&_=<timestamp unix>
- **File download Response Example**: [["part","brand","description","delivery","price"],["91600K7530","Hyundai","<p>WIRING ASSY<\/p>","2 days","126.25"],["87650K70104X","Hyundai","<p>COVER ASSY<\/p>","2 days","11.89"],["00009480","HYUNDAI","HOLDER ASSY-FR T\/SIG LAMP","3 days","5.68"],["00012254","HYUNDAI","SOCKET-BULB","3 days","7.04"],["00015693","HYUNDAI","CAP-HEADLAMP DUST","3 days","4.06"],["00054744","HYUNDAI","CAP-HEADLAMP DUST","3 days","3.77"],["00062660","HYUNDAI","CAP ASSY-BOTTOM","3 days","4.68"],["00074597","HYUNDAI","INDICATOR ASSY-SHIFT LEVER (USA PART)","3 days","106.78"],["00080809","HYUNDAI","KNOB","3 days","1.82"],["00305ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","75.22"],["00305PUNCH","HYUNDAI","TOOL ASSY","3 days","39.71"],["00306ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","92.32"],["0060025023A","HYUNDAI","BOLT","3 days","0.86"],["006Y011SJ0","HYUNDAI","BEARING SET-CRANK SHAFT THRUST","3 days","16.43"],["0110000100","HYUNDAI","BRAKE OIL","3 days","15.87"],["0110EN1100","HYUNDAI","LUMBAR SUPPORT MOTOR","3 days","226.44"],["0210000100","HYUNDAI","OIL-GEAR","3 days","27.02"],["0210000110","HYUNDAI","OIL-LSD","3 days","32.77"],["0210000121","HYUNDAI","HYPOID GEAR OIL (FM PLUS)","3 days","101.28"],["0210000130","HYUNDAI","HYPOID GEAR OIL","3 days","101.24"],["0220000110","HYUNDAI","OIL-AXLE(80W\/90)","3 days","18.87"],["0220000140","HYUNDAI","FRICTION SYSTEM GEAR OIL","3 days","266.07"],["0310000100","HYUNDAI","OIL-P\/STRG","3 days","19.32"],["0310000130","HYUNDAI","OIL-P\/STRG","3 days","29.36"],["0310000140","HYUNDAI","OIL POWER STEERING PENTOSIN CHF202","3 days","23.68"],...
- **Scraper Type**: HTML List + JSON API download and parsing (direct to CSV as per Techoparts)
```

### File Structure (provide sample)
```
# Example filename(s):
autocar_international_FORD_22_10_2025.xlsx = brand = FORD, valid from 22-Oct-25

# Example file content : [["part","brand","description","delivery","price"],["91600K7530","Hyundai","<p>WIRING ASSY<\/p>","2 days","126.25"],["87650K70104X","Hyundai","<p>COVER ASSY<\/p>","2 days","11.89"],["00009480","HYUNDAI","HOLDER ASSY-FR T\/SIG LAMP","3 days","5.68"],["00012254","HYUNDAI","SOCKET-BULB","3 days","7.04"],["00015693","HYUNDAI","CAP-HEADLAMP DUST","3 days","4.06"],["00054744","HYUNDAI","CAP-HEADLAMP DUST","3 days","3.77"],["00062660","HYUNDAI","CAP ASSY-BOTTOM","3 days","4.68"],["00074597","HYUNDAI","INDICATOR ASSY-SHIFT LEVER (USA PART)","3 days","106.78"],["00080809","HYUNDAI","KNOB","3 days","1.82"],["00305ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","75.22"],["00305PUNCH","HYUNDAI","TOOL ASSY","3 days","39.71"],["00306ACKIT","HYUNDAI","COMPRESSOR DIAGNOSIS KIT","3 days","92.32"],["0060025023A","HYUNDAI","BOLT","3 days","0.86"],["006Y011SJ0","HYUNDAI","BEARING SET-CRANK SHAFT THRUST","3 days","16.43"],["0110000100","HYUNDAI","BRAKE OIL","3 days","15.87"],["0110EN1100","HYUNDAI","LUMBAR SUPPORT MOTOR","3 days","226.44"],["0210000100","HYUNDAI","OIL-GEAR","3 days","27.02"],["0210000110","HYUNDAI","OIL-LSD","3 days","32.77"],["0210000121","HYUNDAI","HYPOID GEAR OIL (FM PLUS)","3 days","101.28"],["0210000130","HYUNDAI","HYPOID GEAR OIL","3 days","101.24"],["0220000110","HYUNDAI","OIL-AXLE(80W\/90)","3 days","18.87"],["0220000140","HYUNDAI","FRICTION SYSTEM GEAR OIL","3 days","266.07"],["0310000100","HYUNDAI","OIL-P\/STRG","3 days","19.32"],["0310000130","HYUNDAI","OIL-P\/STRG","3 days","29.36"],["0310000140","HYUNDAI","OIL POWER STEERING PENTOSIN CHF202","3 days","23.68"],...
- **Scraper Type**: HTML List + JSON API download and parsing (direct to CSV as per Techoparts)


# Column mappings needed:
Part Number: Column 1
Description: Column 3
Price: Column 5
Former Part Number: "null"
Supersede Part Number:  "null"
```

### Brand Configuration
```
Enabled Brands: FORD,HYUNDAI,SUBARU,MITSUBISHI,PORSCHE,VAG,MAZDA,BMW,MERCEDES,FCA,GM,PSA,TOYOTA,RENAULT, NISSAN

Brand Detection:
- [ ] From filename
- [ ] Static (one brand per file)

```

### Metadata
```
Location: NETHERLANDS
Currency: EUR
Decimal Format: decimal (12.34)
Default Expiry Days: 30
```

### Known Issues or Special Cases
```
[Describe any quirks, special handling needed, or challenges observed]
JSON file download -> convert directly to CSV as per NEOPARTA

```

## 🎯 Requirements

1. **Streaming Mode**: Must use `scrape_stream()` for file-by-file processing
2. **Duplicate Detection**: Must check state BEFORE downloading each file
3. **Brand Filtering**: Must respect brands in `config` array (brands with `enabled: false` are skipped)
4. **State Tracking**: Must record `supplier_filename` and `valid_from_date`
5. **Type Safety**: No `# type: ignore` comments, full type annotations
6. **Linting**: Must pass `mypy` strict mode and `read_lints`
7. **Local Cleanup**: Must delete source files after successful upload
8. **Config Array**: Must include `config` array with brand-specific configurations

## ✅ Verification Steps

After implementation, we must verify:

### 1. First Run (no state):
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

### 2. Second Run (with state):
```bash
python scripts/run_scraper_local.py --supplier [SUPPLIER_NAME] --use-test-config --force
```

Expected:
- [ ] `[DUPLICATE SKIP]` logs appear
- [ ] **NO downloads happen**
- [ ] **NO processing happens**
- [ ] Completes quickly

### 3. Linting:
```bash
# Check for linter errors
read_lints(paths=["src/scrapers/supplier_scrapers/[supplier]_scraper.py"])
read_lints(paths=["config/scraper/scraper_config.json"])
```

Expected:
- [ ] No linter errors in scraper file
- [ ] No linter errors in modified files
- [ ] All type hints correct

## 📝 Output Needed

Please help me:
1. Create/update scraper configuration in `config/scraper/scraper_config.json`
2. Implement the scraper class (or identify which template to use)
3. Add any needed brand configurations to `config/brand/brand_config_test.json`
5. Add brand aliases if needed
6. Verify duplicate detection works correctly
7. Test locally with `--use-test-config --force` flags

## 🚨 Safety Checks

Before making any changes to shared files (`price_list_parser.py`, `web_scraping_orchestrator.py`, `state_manager.py`, etc.):
1. Explain what you're changing and why
2. Confirm it won't affect existing scrapers (NEOPARTA, TECHNOPARTS, MATEROM, CONNEX)
3. Get my approval if there's any risk

## 📎 Attachments

Please attach or provide:
- [ ] Screenshots of supplier website/portal (if applicable)
- [ ] Sample file(s) from supplier
- [ ] Any existing documentation about supplier's system
- [ ] Error messages or logs if this is a fix for existing scraper

---

## 🚀 Ready to Start

**Ready to start?** Please confirm you've:
1. Reviewed `SUPPLIER_ONBOARDING_CHECKLIST.md`
2. Understood the duplicate detection requirements (Section 3 of checklist)
3. Understood the streaming requirements (Section 5 of checklist)
4. Have all the supplier details filled in above

Ask any clarifying questions about the supplier details before we begin implementation.

---

