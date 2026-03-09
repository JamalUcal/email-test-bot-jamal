# Brand Naming Rules

## Core Principle (CRITICAL)

**ALWAYS use canonical brand names from `brand_config.json`. NEVER use supplier-specific abbreviations.**

Applies to: filenames, logs, output, code variables, state, database.

## The Rule

Convert supplier codes (HY, KI, TO) to canonical names (HYUNDAI, KIA, TOYOTA) immediately upon extraction.

```
Wrong: HY_APF_EUR_BELGIUM.csv     Right: HYUNDAI_APF_EUR_BELGIUM.csv
Wrong: brand = "KI"                Right: brand = "KIA"
```

## Implementation

Resolve using `brand_matcher`:

```python
from scrapers.brand_matcher import find_matching_brand, load_brand_configs

supplier_code = item.get('brandCode')  # "HY"
matched = find_matching_brand(supplier_code, load_brand_configs())
canonical_brand = matched.get('brand')  # "HYUNDAI"
```

## Configuration

### NEVER Add Supplier Codes to config array

```json
// Wrong - using supplier-specific codes
"config": [
  {"brand": "KIA"},
  {"brand": "HYUNDAI"},
  {"brand": "HY"},  // Supplier code - DON'T DO THIS
  {"brand": "KI"}   // Supplier code - DON'T DO THIS
]

// Correct - canonical names only
"config": [
  {"brand": "KIA"},
  {"brand": "HYUNDAI"},
  {"brand": "TOYOTA"}
]
```

### Add Aliases to brand_config.json

```json
{"brand": "HYUNDAI", "aliases": ["hyundai", "hy"], ...}
{"brand": "KIA", "aliases": ["kia", "ki"], ...}
```

### Keep Test/Prod Configs in Sync

When adding aliases to `brand_config.json`, also add to `brand_config_test.json`.
Only difference allowed: `driveFolderId` values.

### Supplier-Level Aliases for Conflicts

When global aliases conflict (e.g., "su" = SUZUKI vs SUBARU), use supplier config:

```json
{"supplier": "APF", "brand_aliases": {"su": "SUZUKI", "gm": "OPEL"}}
```

## Enforcement

If you see an abbreviation in output/logs/filenames, it's a bug - fix immediately.
