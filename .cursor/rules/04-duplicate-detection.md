# Duplicate Detection

## Core Principle

**Use `supplier_filename` + `valid_from_date` for duplicate detection.**

Why? Suppliers can have multiple files per brand (e.g., BMW_PART1, BMW_PART2). Using just `brand` + `date` causes collisions.

## Getting supplier_filename

- **Link downloaders**: `os.path.basename(urlparse(href).path)` 
- **API with headers**: Extract from Content-Disposition or response
- **API without headers**: Generate deterministically from API data

## Critical: Deterministic Generation

If you generate the filename, use API data (NOT `datetime.now()`):

```python
# Wrong - changes every run
supplier_filename = f"{brand}_{datetime.now().strftime('%Y%m%d')}.txt"

# Correct - uses API data
valid_from = parse_date(item['createdDateTime'])
supplier_filename = f"{item['brandCode']}_{valid_from.strftime('%Y%m%d')}.txt"
```

## Implementation

```python
# 1. Generate/extract supplier_filename
supplier_filename = self._generate_expected_filename(item)

# 2. Check duplicate
if self.state_manager.is_file_already_processed(
    supplier=self.supplier_name,
    supplier_filename=supplier_filename,
    valid_from_date=valid_from_date_str
):
    return True  # Skip

# 3. Download and store with SAME filename
actual_filename = self._extract_filename_from_response(response)
state_manager.add_downloaded_file(
    supplier=self.supplier_name,
    supplier_filename=actual_filename,  # MUST match check
    valid_from_date=valid_from_date_str,
    ...
)
```

**Critical**: `_generate_expected_filename()` and `_extract_filename_from_response()` MUST produce identical results.

## Examples by Type

```python
# Link downloader
supplier_filename = os.path.basename(urlparse(href).path)  # "FORD.csv"

# API with headers
supplier_filename = item['data_name']  # "autocar_FORD_22_10_2025.xlsx"

# API without headers
date_part = parse_date(item['createdDateTime']).strftime('%Y%m%d')
supplier_filename = f"{item['brandCode']}_{date_part}.txt"  # "BM_20260107.txt"
```

## Testing

Run scraper twice - second run MUST skip all files:

```bash
python scripts/run_scraper_local.py --supplier SUPPLIER
python scripts/run_scraper_local.py --supplier SUPPLIER  # Should skip all
```

## Debugging

If detection fails:
1. Compare CHECKED value vs STORED value in state
2. Find mismatch (filename? date format?)
3. Ensure check and store use identical values

Common issue: Checking with `version` but storing `date` (or vice versa).

See `06-debugging-workflow.md` for systematic debugging process.
