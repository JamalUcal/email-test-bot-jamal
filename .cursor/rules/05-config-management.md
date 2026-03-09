# Configuration Management

## Two Types of Suppliers

The system has **two distinct types of suppliers** with different configuration files:

1. **Email Suppliers** → `config/supplier/supplier_config.json` (processes attachments from emails)
2. **Web Scraper Suppliers** → `config/scraper/scraper_config.json` (downloads files from websites)

## Email Supplier Config Workflow

**NEVER edit `supplier_config.json` directly - it's GENERATED from individual files.**

```bash
# Correct workflow
vim config/supplier/suppliers/apf.json      # 1. Edit individual file
python config/supplier/merge_suppliers.py   # 2. Merge to supplier_config.json
./deploy/update-and-deploy.sh -config       # 3. Deploy
```

Structure:
```
config/supplier/
├── supplier_config.json     # GENERATED - for email suppliers
├── suppliers/*.json          # SOURCE OF TRUTH - edit these
└── merge_suppliers.py       # Run to rebuild
```

## Web Scraper Config Workflow

**Edit `scraper_config.json` directly - it's a single array file.**

```bash
# Correct workflow
vim config/scraper/scraper_config.json      # 1. Edit the array directly
./deploy/update-and-deploy.sh -config       # 2. Deploy
```

Structure:
```
config/scraper/
└── scraper_config.json      # SOURCE OF TRUTH - array of scraper configs
```

## Design Principles

### 1. DRY - Use Defaults

```json
// Wrong - repeated values
{"supplier": "APF", "config": [
  {"brand": "KIA", "location": "BELGIUM", "currency": "EUR"},
  {"brand": "HYUNDAI", "location": "BELGIUM", "currency": "EUR"}
]}

// Correct - supplier-level defaults
{"supplier": "APF", "location": "BELGIUM", "currency": "EUR", "config": [
  {"brand": "KIA"},
  {"brand": "HYUNDAI"}
]}
```

### 2. Keep Flat - Avoid Unnecessary Nesting

```json
// Wrong
{"supplier": "APF", "metadata": {"location": "BELGIUM"}}

// Correct
{"supplier": "APF", "location": "BELGIUM"}
```


### 3. Field Detection in Config, Not Code

ALL field patterns go in `config/core/column_mapping_config.json`. Use `FieldNameDetector`.

```python
# Wrong
patterns = ['part', 'partnumber', 'pn']
if any(p in field.lower() for p in patterns): ...

# Correct
detector = FieldNameDetector(column_mapping_config)
mapping = detector.detect_fields(sample_record)
```

## Config Examples

**Supplier:**
```json
{
  "supplier": "APF",
  "enabled": true,
  "scraper_type": "api",
  "location": "BELGIUM",
  "currency": "EUR",
  "config": [{"brand": "BMW"}],
  "brand_aliases": {"bm": "BMW"}
}
```

**Brand:**
```json
{"brand": "BMW", "aliases": ["bmw", "bm"], "driveFolderId": "..."}
```

**Column Mapping:**
```json
{"partnumber": {"patterns": ["part", "pn"], "required": true}}
```

## What NOT To Do

❌ Edit `config/supplier/supplier_config.json` directly (edit `config/supplier/suppliers/*.json` instead)
❌ Confuse email supplier config with web scraper config (different files!)
❌ Duplicate values across brands (use defaults)
❌ Nest unnecessarily
❌ Hardcode field patterns in Python
