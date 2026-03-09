# BigQuery supersession Reconciliation Design

**Status**: Approved - SCD Type 2 Architecture  
**Author**: AI Assistant  
**Date**: 2026-01-22 (Updated)  
**Related**: Phase 2 BigQuery Integration

> **Architecture Update (2026-01-22)**: Redesigned to use SCD Type 2 pattern with deferred merge.
> - Staging tables preserved for scheduled merge (not dropped immediately)
> - New `canonical_prices` table with validity tracking
> - Scheduled nightly merge with status management (ACTIVE, UNAVAILABLE, HISTORY, DISCONTINUED)

---

## 1. Overview

### 1.1 Problem Statement

Supplier price lists contain **supersession** information indicating that one part number has been replaced by another. Currently, this data is captured but not reconciled:

- Part A may list Part B as its supersession
- Part B may have a different price than Part A
- Part B may not exist as a distinct row in the file
- supersession chains may exist: A → B → C

**Business requirement**: 
1. When querying prices, users need the **effective price** based on supersession relationships, not the raw uploaded price. 
2. Users need to be able to see both the original part and the supersession as distinct lines in the reconciled price file for a supplier

### 1.2 Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| **De-duplication (runs first)** |||
| D1 | De-duplicate part numbers BEFORE supersession reconciliation | Must |
| D2 | For duplicate parts, use the highest price | Must |
| D3 | For duplicate parts, take all metadata (description, former_part_number, etc.) from the row with the highest price | Must |
| D4 | For duplicate parts with mixed supersession (some rows have it, some don't), keep the supersession | Must |
| D5 | For duplicate parts with multiple different supersessions, pick one (alphabetically lowest for consistency) and log as error | Must |
| D6 | Log ONE error per duplicate part_number that had mismatched prices or supersessions, with all variant details | Must |
| **Supersession reconciliation (runs after de-duplication)** |||
| R1 | If a part has a supersession, display the part with the supersession's price | Must |
| R2 | If supersession is not listed as a line item in the file, use the original part's price | Must |
| R3 | If supersession is not in the file, add it as a new row with the price of the original part | Must |
| R4 | Follow supersession chains (A→B→C uses C's price) | Must |
| R5 | If A→B→C chain then use the price which is furthest down in the chain for all links  i.e. if C has a price use C's price for A & B, if C has no price then use B's price for A & C, if B has no price then use A's price for B & C | Must |
| R6 | Detect and flag circular references as errors | Must |
| R7 | If multiple parts supersede to the same non-existent part, create ONE synthetic row using the highest price among parents. Log as warning. | Must |
| R8 | Configurable logging of price inheritance for audit | Should |

### 1.3 Future Enhancements (Out of Scope for Now)

- **Canonical supersession reference**: A master table of known supersession relationships that can override/supplement supplier data
- **Per-supplier configuration**: Ability to disable reconciliation for specific suppliers with unreliable data
- **Cross-file supersession lookup**: Looking up supersessions across historical uploads

### 1.4 Scope

**In Scope**:
- BigQuery schema (`price_lists`, `canonical_prices` tables)
- SCD Type 2 data model with validity tracking (`valid_from`, `valid_until`, `status`)
- Supersession reconciliation as stored procedure (called inline by Python)
- Deferred merge via scheduled query into `canonical_prices`
- Status management: ACTIVE, UNAVAILABLE, HISTORY, DISCONTINUED
- Price deduplication across price lists (unchanged prices don't create new rows)
- Circular reference detection with fallback to original price
- Chain following (within single price list)
- Error logging to `processing_errors` table
- Reconciliation errors included in summary email
- Audit logging (optional, configurable globally)

**Out of Scope** (for now):
- Canonical supersession reference table
- Per-supplier reconciliation configuration
- Cross-supplier supersession lookup
- Cross-file supersession lookup (historical data)
- UI for viewing supersession relationships

### 1.5 Important Notes: Data Normalization

**Part number and supersession normalization is performed by Python BEFORE upload to BigQuery.**

The Python parsing pipeline (`price_list_parser.py`, `file_generator.py`) normalizes:
- `part_number`: Uppercase, alphanumeric only, padded to minimum length
- `supersession`: Same normalization as part_number

**BigQuery stored procedures assume all part numbers and supersessions are already normalized.** 
This means:
- De-duplication can use exact string matching on `part_number`
- Supersession lookups can use exact string matching
- No normalization logic is needed in SQL

This is an intentional design decision to keep BigQuery logic simple and leverage existing Python normalization.

---

## 2. Architecture

### 2.1 High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW (SCD Type 2)                          │
│           (Memory-efficient via GCS intermediary - handles 500k+ rows)       │
│           (Deferred merge for storage efficiency)                            │
└─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
REAL-TIME PROCESSING (during email processing)
═══════════════════════════════════════════════════════════════════════════════

┌─────────────┐     ┌─────────────────────┐     ┌──────────────────────────────┐
│ Source File │────▶│ Python Parser       │────▶│ 1. Stream parse to local CSV │
│ (XLSX/CSV)  │     │ (existing pipeline) │     │    (memory-efficient)        │
└─────────────┘     └─────────────────────┘     └──────────────┬───────────────┘
                                                              │
                                                              ▼
                                               ┌──────────────────────────────┐
                                               │ 2. Upload CSV to GCS         │
                                               │    gs://bucket/staging/      │
                                               │    {price_list_id}.csv       │
                                               └──────────────┬───────────────┘
                                                              │
                                                              ▼
                                               ┌──────────────────────────────┐
                                               │ 3. BigQuery Load Job         │
                                               │    - Create price_list record│
                                               │    - LOAD DATA from GCS to   │
                                               │      dedicated staging table │
                                               │      _staging_{price_list_id}│
                                               │    (free, fast, atomic)      │
                                               └──────────────┬───────────────┘
                                                              │
                                                              ▼
                                               ┌──────────────────────────────┐
                                               │ 4. Call Stored Procedure     │
                                               │    reconcile_supersessions(  │
                                               │      staging_table_id        │
                                               │    )                         │
                                               │    Operates ONLY on staging  │
                                               │    table (cost-efficient)    │
                                               └──────────────┬───────────────┘
                                                              │
                   ┌──────────────────────────────────────────┘
                   │  Stored Procedure (in BigQuery):
                   │  - Operates on staging table only (~50MB)
                   │  - Follow supersession chains
                   │  - Detect circular references
                   │  - Update effective_price column
                   │  - Add synthetic rows for missing supersessions
                   │  - Set reconciliation_status for errors
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 5. Export reconciled data from staging table to GCS                         │
│    EXPORT DATA to gs://bucket/reconciled/{price_list_id}/*.csv              │
└─────────────────────────────────────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 6. Register staging table for deferred merge                                │
│    - Store staging_table_id in price_lists                                  │
│    - Set merge_status = 'PENDING'                                           │
│    - STAGING TABLE IS PRESERVED (not dropped)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 7. Stream download from GCS → Upload to Google Drive                        │
│    (memory-efficient chunked transfer)                                      │
└─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
SCHEDULED PROCESSING (nightly at 2:00 AM)
═══════════════════════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────────────────────┐
│ 8. Scheduled Query: CALL merge_pending_to_canonical()                       │
│    - Merge staging tables directly → canonical_prices (via dynamic UNION)   │
│    - Drop processed staging tables                                          │
│    - Update merge_status = 'MERGED'                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                   │
                   │  SCD Type 2 Merge Logic:
                   │  - Price changed? → Close old row (HISTORY), insert new ACTIVE
                   │  - Price = 0? → Close old row, insert UNAVAILABLE
                   │  - Unchanged? → Update last_seen_date only (no new row)
                   │  - Supersession changed? → Close old row, insert new ACTIVE
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BigQuery Tables (Final State)                                               │
│ ─────────────────────────────                                               │
│ • price_lists         - Metadata (merge_status: MERGED)                     │
│ • canonical_prices    - SCD Type 2 with valid_from/valid_until/status       │
│ • processing_errors   - Errors logged during reconciliation                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Processing Model

**Two-phase processing** for efficiency and storage optimization:

#### Phase 1: Real-time Processing (during email processing)
- Python orchestrates the flow using GCS as intermediary for large data transfers
- **Streaming preserved**: Parse → local CSV uses existing streaming pipeline (~100MB memory)
- **GCS staging**: Upload/download via GCS avoids loading 500k rows into Python memory
- **BigQuery Load Jobs**: Free, fast, atomic (vs streaming inserts at 500 rows/batch)
- **Dedicated staging table**: Each price list loaded to its own table `_staging_{uuid}`
- **EXPORT DATA**: BigQuery writes results directly to GCS (no Python memory)
- **Staging table preserved**: NOT dropped immediately - kept for scheduled merge

#### Phase 2: Scheduled Merge (nightly)
- **SCD Type 2 merge**: Consolidates staging tables into `canonical_prices`
- **Storage efficient**: Unchanged prices don't create new rows
- **Status tracking**: ACTIVE, UNAVAILABLE, HISTORY, DISCONTINUED
- **Validity tracking**: `valid_from`, `valid_until` for historical queries
- **Cleanup**: Drops staging tables after successful merge

**Why SCD Type 2 with deferred merge?**
- **Storage efficiency**: Avoid duplicate rows for unchanged prices
- **Historical tracking**: Know when prices were valid with `valid_from`/`valid_until`
- **Status management**: Track part availability (ACTIVE vs UNAVAILABLE)
- **Query efficiency**: `WHERE status = 'ACTIVE'` for current prices
- **Trend analysis**: Easy to see price changes over time

**Why dedicated staging table per price list?**
- Stored procedure operates ONLY on current batch (~50MB), not entire historical table
- **Cost-efficient**: Avoids scanning large `canonical_prices` table (10GB+) 
- **Guaranteed isolation**: No risk of accidentally querying historical data
- **No partition pruning dependency**: Cost savings without relying on query optimizer

**Why GCS intermediary?**
- BigQuery streaming inserts recommend ~500 rows/batch → 1000 API calls for 500k rows
- Load jobs from GCS are free and handle any file size atomically
- EXPORT DATA writes directly to GCS without Python memory overhead
- Preserves the existing streaming architecture's memory efficiency

### 2.3 SCD Type 2 State Transitions

| Current Status | Incoming Signal | Action |
|----------------|-----------------|--------|
| None (new part) | Price > 0 | Insert new ACTIVE row |
| None (new part) | Price = 0 | Insert new UNAVAILABLE row |
| ACTIVE | Same price | Update `last_seen_date` only |
| ACTIVE | Different price | Close as HISTORY, insert new ACTIVE |
| ACTIVE | Price = 0 | Close as HISTORY, insert UNAVAILABLE |
| ACTIVE | Absent | No change (part may still be valid) |
| UNAVAILABLE | Price = 0 | Update `last_seen_date` only |
| UNAVAILABLE | Real price | Close as HISTORY, insert new ACTIVE |
| UNAVAILABLE | Absent | No change |

**Special handling:**
- **Description = NULL in new data**: Preserve existing description (don't overwrite)
- **Supersession = NULL in new data**: Preserve existing supersession
- **Supersession changed to new value**: Creates new row (could be a correction)

---

## 3. BigQuery Schema

### 3.1 Dataset Structure

```
PRICING/
├── price_lists             # Metadata for each uploaded file (with staging table tracking)
├── canonical_prices        # SCD Type 2 deduplicated price history (PRIMARY)
├── processing_errors       # Circular refs, validation errors
├── supersession_audit      # Price inheritance log (optional)
└── price_list_items        # DEPRECATED - kept for migration only
```

### 3.2 Table: `price_lists`

Metadata for each uploaded price list file. One row per file upload.

```sql
CREATE TABLE PRICING.price_lists (
  -- Primary key
  price_list_id STRING NOT NULL,            -- UUID, generated on insert
  
  -- Business keys
  supplier STRING NOT NULL,
  brand STRING NOT NULL,
  currency STRING NOT NULL,
  location STRING,
  
  -- Source info
  source_filename STRING NOT NULL,
  source_email_subject STRING,
  source_email_date TIMESTAMP,
  
  -- Validity
  valid_from_date DATE,
  valid_to_date DATE,
  
  -- Processing status
  upload_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  reconciliation_status STRING DEFAULT 'PENDING',  -- PENDING, COMPLETED, FAILED
  reconciliation_timestamp TIMESTAMP,
  
  -- Stats
  total_items INT64,
  reconciled_items INT64,
  items_with_errors INT64,
  synthetic_items_added INT64,              -- supersessions not in original file
  duplicates_found INT64,                   -- part numbers with duplicates
  duplicates_removed INT64,                 -- duplicate rows removed
  
  -- Drive upload
  drive_file_id STRING,
  drive_file_url STRING,
  
  -- Staging table tracking (for deferred merge)
  staging_table_id STRING,                  -- Full table ID of staging table
  merge_status STRING                       -- PENDING, MERGED, FAILED
)
PARTITION BY DATE(upload_timestamp)
CLUSTER BY supplier, brand;
```

### 3.3 Table: `canonical_prices` (SCD Type 2)

**Primary table for querying current and historical prices.** Deduplicated using SCD Type 2 pattern.

```sql
CREATE TABLE PRICING.canonical_prices (
  -- Primary key
  item_id STRING NOT NULL,                   -- UUID, generated on insert
  
  -- Business keys (uniqueness: supplier + brand + part_number + currency + valid_from)
  supplier STRING NOT NULL,
  brand STRING NOT NULL,
  part_number STRING NOT NULL,               -- Normalized (uppercase, alphanumeric, padded)
  currency STRING NOT NULL,                  -- Currency code (USD, EUR, etc.)
  
  -- Prices (NULL for UNAVAILABLE status)
  original_price NUMERIC(15, 4),             -- Price from supplier file
  effective_price NUMERIC(15, 4),            -- Price after supersession reconciliation
  
  -- SCD Type 2 validity
  valid_from DATE NOT NULL,                  -- Date this price became effective
  valid_until DATE,                          -- Date this price was superseded (NULL = current)
  
  -- Status: ACTIVE, UNAVAILABLE, HISTORY, DISCONTINUED
  status STRING NOT NULL,
  
  -- Tracking
  last_seen_date DATE NOT NULL,              -- Last date we received this price
  first_seen_price_list_id STRING NOT NULL,  -- Price list that first introduced this price
  last_seen_price_list_id STRING NOT NULL,   -- Most recent price list with this price
  
  -- Supersession data
  former_part_number STRING,
  supersession STRING,
  terminal_part_number STRING,
  supersession_chain_length INT64 DEFAULT 0,
  price_inherited_from STRING,
  
  -- Descriptive (preserved across updates if new value is NULL)
  description STRING,
  location STRING,
  
  -- Reconciliation metadata
  is_synthetic BOOL DEFAULT FALSE,
  reconciliation_status STRING,
  reconciliation_error_message STRING
)
PARTITION BY valid_from
CLUSTER BY supplier, brand, part_number, status;
```

**Status Values:**
| Status | Description | Price |
|--------|-------------|-------|
| `ACTIVE` | Current valid price | `effective_price` |
| `UNAVAILABLE` | Supplier sent price=0 (not available) | NULL |
| `HISTORY` | Superseded by newer price | `effective_price` (historical) |
| `DISCONTINUED` | Explicitly marked as discontinued | NULL |

### 3.4 Table: `price_list_items`

Individual parts with both original and reconciled prices.

```sql
CREATE TABLE PRICING.price_list_items (
  -- Primary key
  item_id STRING NOT NULL,                  -- UUID, generated on insert
  
  -- Foreign key to price_lists
  price_list_id STRING NOT NULL,
  
  -- Business keys
  part_number STRING NOT NULL,              -- Normalized (uppercase, alphanumeric, padded)
  currency STRING NOT NULL,                 -- Currency code (USD, EUR, etc.)
  
  -- Prices
  original_price NUMERIC(15, 4) NOT NULL,   -- Price from supplier file
  effective_price NUMERIC(15, 4),           -- Price after supersession reconciliation
                                            -- NULL until reconciliation runs
  
  -- supersession data
  former_part_number STRING,                -- Part this replaced (informational)
  supersession STRING,                      -- Part that replaces this one
  
  -- Reconciliation results
  terminal_part_number STRING,              -- End of chain (NULL if no supersession)
  supersession_chain_length INT64 DEFAULT 0,
  price_inherited_from STRING,              -- Part number price was inherited from
  
  -- Descriptive
  description STRING,
  
  -- Source tracking
  source_row_number INT64,
  is_synthetic BOOL DEFAULT FALSE,          -- TRUE if added during reconciliation
  
  -- Reconciliation status (for error tracking)
  reconciliation_status STRING DEFAULT 'PENDING',  
    -- Values: PENDING, OK, CIRCULAR_REF, CHAIN_TOO_LONG, ERROR
  reconciliation_error_message STRING,
  
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  reconciled_timestamp TIMESTAMP
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY price_list_id, part_number;
```

**Reconciliation Status Values**:
| Status | Description | Price Used |
|--------|-------------|------------|
| `PENDING` | Not yet reconciled | N/A |
| `OK` | Successfully reconciled | `effective_price` |
| `NO_supersession` | No supersession to process | `original_price` |
| `CIRCULAR_REF` | Circular chain detected | `original_price` (fallback) |
| `CHAIN_TOO_LONG` | Chain exceeds max depth | Last valid price in chain |
| `ERROR` | Other error | `original_price` (fallback) |

### 3.5 Table: `processing_errors`

Detailed error log for reconciliation issues.

```sql
CREATE TABLE PRICING.processing_errors (
  error_id STRING NOT NULL,
  
  -- Links
  price_list_id STRING NOT NULL,
  item_id STRING,                           -- May be NULL for price_list level errors
  
  -- Context
  supplier STRING,
  brand STRING,
  part_number STRING,
  
  -- Error details
  error_type STRING NOT NULL,               -- 'CIRCULAR_supersession', 'CHAIN_TOO_LONG', etc.
  error_message STRING,
  error_details JSON,                       -- Chain path, attempted lookups, etc.
  
  -- Metadata
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp);
```

**Error Types**:
| Error Type | Description |
|------------|-------------|
| `DUPLICATE_PART_NUMBER` | Same part number appears multiple times with different prices or supersessions |
| `CIRCULAR_supersession` | Chain forms a loop (A→B→C→A) |
| `CHAIN_TOO_LONG` | Chain exceeds max depth (default 10) |
| `SELF_REFERENCE` | Part supersedes to itself |
| `AMBIGUOUS_SYNTHETIC_PRICE` | Multiple parts supersede to same non-existent part with different prices |

### 3.6 Table: `supersession_audit` (Optional)

Audit log for price inheritance. Enabled via config flag.

```sql
CREATE TABLE PRICING.supersession_audit (
  audit_id STRING NOT NULL,
  
  -- Links
  price_list_id STRING NOT NULL,
  item_id STRING NOT NULL,
  
  -- The part whose price was changed
  part_number STRING NOT NULL,
  
  -- Price change details
  original_price NUMERIC(15, 4),
  effective_price NUMERIC(15, 4),
  price_inherited_from STRING,              -- Part number price came from
  
  -- Chain details
  supersession_chain ARRAY<STRING>,         -- e.g., ['A', 'B', 'C']
  chain_length INT64,
  
  -- Metadata
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_timestamp);
```

---

## 4. Reconciliation Logic

### 4.1 Algorithm Overview

The stored procedure performs two phases: **de-duplication** then **supersession reconciliation**.

```
INPUT: price_list_id (UUID of the price list to reconcile)

═══════════════════════════════════════════════════════════════════════════════
PHASE 1: DE-DUPLICATION (runs first)
═══════════════════════════════════════════════════════════════════════════════

1. Identify duplicate part_numbers (exact match - already normalized by Python)
2. FOR each group of duplicates:
   a. Select winner row: highest price, then alphabetically lowest supersession
   b. Resolve supersession:
      - If any row has supersession and others don't → keep the supersession
      - If multiple different supersessions → pick alphabetically lowest
   c. IF prices differ OR supersessions differ:
      - Log ONE error with all variant details (prices, supersessions)
   d. DELETE all duplicate rows except the winner
   e. UPDATE winner row with resolved supersession (if needed)
3. Record dedup stats (duplicates_found, duplicates_removed)

═══════════════════════════════════════════════════════════════════════════════
PHASE 2: SUPERSESSION RECONCILIATION (runs after de-duplication)
═══════════════════════════════════════════════════════════════════════════════

4. Load all part_number → (original_price, supersession) for this price_list_id
5. Build supersession graph within this price list
6. FOR each part with a supersession:
   a. Follow chain until terminal node (no further supersession)
   b. Detect cycles using visited set
   c. IF cycle detected:
      - Set reconciliation_status = 'CIRCULAR_REF'
      - Set effective_price = original_price (fallback)
      - Log error to processing_errors
   d. ELSE:
      - Get terminal node's price
      - IF terminal node not in file:
        - Use original price
        - Create synthetic row for supersession target
      - Set effective_price = terminal node's price
      - Set reconciliation_status = 'OK'
7. UPDATE all items in price_list_items with reconciled values
8. UPDATE price_lists with reconciliation stats
9. Log audit records (if enabled via config)

OUTPUT: Updated price_list_items with effective_price populated
```

### 4.2 SQL Implementation

#### 4.2.1 Stored Procedure: `reconcile_supersessions_staging`

```sql
CREATE OR REPLACE PROCEDURE PRICING.reconcile_supersessions_staging(
  IN p_staging_table_id STRING,
  IN p_price_list_id STRING,
  IN p_max_chain_depth INT64
)
BEGIN
  DECLARE v_supplier STRING;
  DECLARE v_brand STRING;
  DECLARE v_total_items INT64;
  DECLARE v_reconciled_items INT64 DEFAULT 0;
  DECLARE v_items_with_errors INT64 DEFAULT 0;
  DECLARE v_synthetic_items INT64 DEFAULT 0;
  DECLARE v_duplicates_found INT64 DEFAULT 0;
  DECLARE v_duplicates_removed INT64 DEFAULT 0;

  -- Get price list metadata
  SET (v_supplier, v_brand) = (
    SELECT AS STRUCT supplier, brand
    FROM PRICING.price_lists
    WHERE price_list_id = p_price_list_id
  );

  -- ═══════════════════════════════════════════════════════════════════════════
  -- PHASE 1: DE-DUPLICATION
  -- Part numbers are already normalized by Python before upload
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Step D1: Identify duplicates and select winner for each group
  -- Winner criteria: highest price, then alphabetically lowest supersession
  CREATE TEMP TABLE dedup_analysis AS
  WITH ranked AS (
    SELECT 
      item_id,
      part_number,
      original_price,
      supersession,
      description,
      former_part_number,
      -- Rank: highest price first, then alphabetically lowest supersession (NULLS last)
      ROW_NUMBER() OVER (
        PARTITION BY part_number 
        ORDER BY original_price DESC, COALESCE(supersession, 'ZZZZZZ') ASC
      ) AS rank_in_group,
      COUNT(*) OVER (PARTITION BY part_number) AS group_size,
      -- Aggregate all prices and supersessions for error logging
      ARRAY_AGG(STRUCT(original_price AS price, supersession)) 
        OVER (PARTITION BY part_number) AS all_variants,
      -- Check for conflicts
      COUNT(DISTINCT original_price) OVER (PARTITION BY part_number) AS distinct_prices,
      COUNT(DISTINCT COALESCE(supersession, '')) OVER (PARTITION BY part_number) AS distinct_supersessions,
      -- Resolve supersession: prefer non-null, then alphabetically lowest
      FIRST_VALUE(supersession) OVER (
        PARTITION BY part_number 
        ORDER BY CASE WHEN supersession IS NOT NULL AND supersession != '' THEN 0 ELSE 1 END,
                 COALESCE(supersession, 'ZZZZZZ') ASC
      ) AS resolved_supersession
    FROM PRICING.price_list_items
    WHERE price_list_id = p_price_list_id
  )
  SELECT * FROM ranked;

  -- Step D2: Log errors for duplicates with mismatched prices or supersessions
  INSERT INTO PRICING.processing_errors (
    error_id, price_list_id, supplier, brand, part_number,
    error_type, error_message, error_details
  )
  SELECT DISTINCT
    GENERATE_UUID(),
    p_price_list_id,
    v_supplier,
    v_brand,
    da.part_number,
    'DUPLICATE_PART_NUMBER',
    CONCAT('Part ', da.part_number, ' appears ', da.group_size, ' times with ',
           CASE WHEN da.distinct_prices > 1 THEN CONCAT(da.distinct_prices, ' different prices') ELSE 'same price' END,
           CASE WHEN da.distinct_supersessions > 1 THEN CONCAT(' and ', da.distinct_supersessions, ' different supersessions') ELSE '' END,
           '. Using price: ', (SELECT MAX(v.price) FROM UNNEST(da.all_variants) v),
           CASE WHEN da.resolved_supersession IS NOT NULL THEN CONCAT(', supersession: ', da.resolved_supersession) ELSE '' END),
    TO_JSON(STRUCT(
      da.all_variants AS variants,
      (SELECT MAX(v.price) FROM UNNEST(da.all_variants) v) AS resolved_price,
      da.resolved_supersession AS resolved_supersession
    ))
  FROM dedup_analysis da
  WHERE da.group_size > 1
    AND da.rank_in_group = 1  -- Only log once per duplicate group
    AND (da.distinct_prices > 1 OR da.distinct_supersessions > 1);  -- Only if there's a conflict

  -- Step D3: Delete duplicate rows (keep only winner)
  DELETE FROM PRICING.price_list_items
  WHERE price_list_id = p_price_list_id
    AND item_id IN (
      SELECT item_id FROM dedup_analysis WHERE rank_in_group > 1
    );

  -- Step D4: Update winner rows with resolved supersession (if different)
  UPDATE PRICING.price_list_items AS items
  SET supersession = da.resolved_supersession
  FROM dedup_analysis da
  WHERE items.item_id = da.item_id
    AND da.rank_in_group = 1
    AND da.group_size > 1
    AND (items.supersession IS DISTINCT FROM da.resolved_supersession);

  -- Step D5: Record dedup stats
  SET v_duplicates_found = (SELECT COUNT(DISTINCT part_number) FROM dedup_analysis WHERE group_size > 1);
  SET v_duplicates_removed = (SELECT COUNT(*) FROM dedup_analysis WHERE rank_in_group > 1);

  DROP TABLE IF EXISTS dedup_analysis;

  -- ═══════════════════════════════════════════════════════════════════════════
  -- PHASE 2: SUPERSESSION RECONCILIATION
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Step 1: Build price index for this price list (now de-duplicated)
  CREATE TEMP TABLE price_index AS
  SELECT 
    item_id,
    part_number,
    supersession,
    original_price
  FROM PRICING.price_list_items
  WHERE price_list_id = p_price_list_id;

  -- Step 2: Follow supersession chains using recursive CTE
  CREATE TEMP TABLE supersession_chains AS
  WITH RECURSIVE chain AS (
    -- Base case: all parts with supersessions
    SELECT 
      p.item_id,
      p.part_number AS origin_part,
      p.part_number AS current_part,
      p.supersession AS next_part,
      p.original_price AS origin_price,
      p.original_price AS current_price,
      1 AS depth,
      [p.part_number] AS chain_path,
      FALSE AS is_circular
    FROM price_index p
    WHERE p.supersession IS NOT NULL
      AND p.supersession != ''
      AND p.supersession != p.part_number
    
    UNION ALL
    
    -- Recursive case: follow the chain
    SELECT 
      c.item_id,
      c.origin_part,
      c.next_part AS current_part,
      p.supersession AS next_part,
      c.origin_price,
      COALESCE(p.original_price, c.current_price) AS current_price,
      c.depth + 1,
      ARRAY_CONCAT(c.chain_path, [c.next_part]),
      c.next_part IN UNNEST(c.chain_path) AS is_circular
    FROM chain c
    LEFT JOIN price_index p ON c.next_part = p.part_number
    WHERE c.next_part IS NOT NULL
      AND c.depth < p_max_chain_depth
      AND NOT c.is_circular
      AND (p.supersession IS NOT NULL OR p.part_number IS NULL)
  ),
  
  -- Get terminal state for each origin part
  terminal_states AS (
    SELECT 
      item_id,
      origin_part,
      ARRAY_AGG(
        STRUCT(current_part, next_part, current_price, depth, chain_path, is_circular)
        ORDER BY depth DESC
        LIMIT 1
      )[OFFSET(0)] AS terminal
    FROM chain
    GROUP BY item_id, origin_part
  )
  
  SELECT 
    t.item_id,
    t.origin_part,
    t.terminal.current_part AS terminal_part,
    t.terminal.current_price AS effective_price,
    t.terminal.depth AS chain_length,
    t.terminal.chain_path,
    t.terminal.is_circular,
    t.terminal.next_part AS unresolved_supersession
  FROM terminal_states t;

  -- Step 3: Log circular reference errors
  INSERT INTO PRICING.processing_errors 
    (error_id, price_list_id, item_id, supplier, brand, part_number, error_type, error_message, error_details)
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    sc.item_id,
    v_supplier,
    v_brand,
    sc.origin_part,
    'CIRCULAR_supersession',
    CONCAT('Circular chain: ', ARRAY_TO_STRING(sc.chain_path, ' → ')),
    TO_JSON(STRUCT(sc.chain_path AS path, sc.chain_length AS depth))
  FROM supersession_chains sc
  WHERE sc.is_circular = TRUE;

  -- Step 4: Update items with reconciled prices
  -- Parts WITH supersession (may have circular refs)
  UPDATE PRICING.price_list_items AS items
  SET 
    effective_price = CASE 
      WHEN sc.is_circular THEN items.original_price  -- Fallback for circular
      ELSE sc.effective_price 
    END,
    terminal_part_number = CASE 
      WHEN sc.is_circular THEN NULL 
      ELSE sc.terminal_part 
    END,
    supersession_chain_length = sc.chain_length,
    price_inherited_from = CASE 
      WHEN sc.is_circular THEN NULL
      WHEN sc.effective_price != items.original_price THEN sc.terminal_part
      ELSE NULL 
    END,
    reconciliation_status = CASE 
      WHEN sc.is_circular THEN 'CIRCULAR_REF'
      ELSE 'OK' 
    END,
    reconciliation_error_message = CASE 
      WHEN sc.is_circular THEN CONCAT('Circular chain: ', ARRAY_TO_STRING(sc.chain_path, ' → '))
      ELSE NULL 
    END,
    reconciled_timestamp = CURRENT_TIMESTAMP()
  FROM supersession_chains sc
  WHERE items.item_id = sc.item_id
    AND items.price_list_id = p_price_list_id;

  -- Parts WITHOUT supersession (no reconciliation needed)
  UPDATE PRICING.price_list_items
  SET 
    effective_price = original_price,
    reconciliation_status = 'NO_supersession',
    reconciled_timestamp = CURRENT_TIMESTAMP()
  WHERE price_list_id = p_price_list_id
    AND (supersession IS NULL OR supersession = '')
    AND reconciliation_status = 'PENDING';

  -- Step 5: Add synthetic rows for supersessions not in original file
  -- Handle case where multiple parts supersede to same non-existent part:
  -- - Create ONE synthetic row per terminal_part
  -- - Use the HIGHEST price among all parents
  -- - Log warning if multiple parents have different prices
  
  -- 5a: Identify synthetic rows needed with aggregated data
  CREATE TEMP TABLE synthetic_candidates AS
  SELECT 
    terminal_part,
    MAX(effective_price) AS max_price,
    MIN(effective_price) AS min_price,
    COUNT(*) AS parent_count,
    ARRAY_AGG(STRUCT(origin_part, effective_price) ORDER BY effective_price DESC LIMIT 10) AS parents,
    -- Pick the parent with highest price for attribution
    (ARRAY_AGG(origin_part ORDER BY effective_price DESC))[OFFSET(0)] AS highest_price_parent
  FROM supersession_chains sc
  WHERE sc.terminal_part NOT IN (SELECT part_number FROM price_index)
    AND NOT sc.is_circular
  GROUP BY terminal_part;
  
  -- 5b: Log warnings for ambiguous synthetic prices (multiple parents with different prices)
  INSERT INTO PRICING.processing_errors (
    error_id, price_list_id, supplier, brand, part_number,
    error_type, error_message, error_details
  )
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    v_supplier,
    v_brand,
    sc.terminal_part,
    'AMBIGUOUS_SYNTHETIC_PRICE',
    CONCAT('Synthetic part ', sc.terminal_part, ' has ', sc.parent_count, 
           ' parents with prices ranging from ', sc.min_price, ' to ', sc.max_price,
           '. Using highest price: ', sc.max_price),
    TO_JSON(STRUCT(
      sc.parents AS parent_parts,
      sc.max_price AS selected_price,
      sc.highest_price_parent AS price_source
    ))
  FROM synthetic_candidates sc
  WHERE sc.max_price != sc.min_price;  -- Only log if prices differ
  
  -- 5c: Insert ONE synthetic row per terminal_part with highest price
  INSERT INTO PRICING.price_list_items (
    item_id, price_list_id, part_number, currency, original_price, effective_price,
    former_part_number, supersession, terminal_part_number, supersession_chain_length,
    price_inherited_from, description, is_synthetic, reconciliation_status, reconciled_timestamp
  )
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    sc.terminal_part,                    -- The supersession becomes the part number
    (SELECT currency FROM PRICING.price_lists WHERE price_list_id = p_price_list_id),
    sc.max_price,                        -- Use highest price among parents
    sc.max_price,
    sc.highest_price_parent,             -- Attribute to the highest-price parent
    NULL,                                -- No further supersession
    sc.terminal_part,
    0,
    sc.highest_price_parent,             -- Price inherited from highest-price parent
    CASE 
      WHEN sc.parent_count > 1 THEN CONCAT('supersession of ', sc.parent_count, ' parts (using highest price from ', sc.highest_price_parent, ')')
      ELSE CONCAT('supersession of ', sc.highest_price_parent)
    END,
    TRUE,                                -- Mark as synthetic
    'OK',
    CURRENT_TIMESTAMP()
  FROM synthetic_candidates sc;
  
  -- 5d: Update all parts that supersede to synthetic parts to use the synthetic's price
  UPDATE PRICING.price_list_items AS items
  SET effective_price = sc.max_price
  FROM synthetic_candidates sc
  WHERE items.price_list_id = p_price_list_id
    AND items.terminal_part_number = sc.terminal_part
    AND items.is_synthetic = FALSE;
  
  DROP TABLE IF EXISTS synthetic_candidates;

  -- Step 6: Update price_list with stats
  SET (v_total_items, v_reconciled_items, v_items_with_errors, v_synthetic_items) = (
    SELECT AS STRUCT
      COUNT(*),
      COUNTIF(reconciliation_status IN ('OK', 'NO_supersession')),
      COUNTIF(reconciliation_status IN ('CIRCULAR_REF', 'CHAIN_TOO_LONG', 'ERROR')),
      COUNTIF(is_synthetic = TRUE)
    FROM PRICING.price_list_items
    WHERE price_list_id = p_price_list_id
  );

  UPDATE PRICING.price_lists
  SET 
    reconciliation_status = 'COMPLETED',
    reconciliation_timestamp = CURRENT_TIMESTAMP(),
    total_items = v_total_items,
    reconciled_items = v_reconciled_items,
    items_with_errors = v_items_with_errors,
    synthetic_items_added = v_synthetic_items,
    duplicates_found = v_duplicates_found,
    duplicates_removed = v_duplicates_removed
  WHERE price_list_id = p_price_list_id;

  -- Cleanup temp tables
  DROP TABLE IF EXISTS price_index;
  DROP TABLE IF EXISTS supersession_chains;
  DROP TABLE IF EXISTS synthetic_candidates;
  
END;
```

### 4.3 De-duplication Examples

#### De-dup Example 1: Different Prices, Same Supersession
```
Input (price_list_items - before de-duplication):
  Part A (row 1): original_price=$10, supersession=B, description="Widget v1"
  Part A (row 2): original_price=$12, supersession=B, description="Widget v2"

After de-duplication:
  Part A: original_price=$12, supersession=B, description="Widget v2"
          (kept row with highest price, all metadata from that row)
  
  processing_errors: 1 row logged (DUPLICATE_PART_NUMBER)
    - error_details: {variants: [{price: 10, supersession: "B"}, {price: 12, supersession: "B"}],
                      resolved_price: 12, resolved_supersession: "B"}

Stats: duplicates_found=1, duplicates_removed=1
```

#### De-dup Example 2: Same Price, Different Supersessions
```
Input (price_list_items - before de-duplication):
  Part A (row 1): original_price=$10, supersession=B
  Part A (row 2): original_price=$10, supersession=C

After de-duplication:
  Part A: original_price=$10, supersession=B
          (same price, so picked alphabetically lowest supersession: B < C)
  
  processing_errors: 1 row logged (DUPLICATE_PART_NUMBER)
    - error_details: {variants: [{price: 10, supersession: "B"}, {price: 10, supersession: "C"}],
                      resolved_price: 10, resolved_supersession: "B"}

Stats: duplicates_found=1, duplicates_removed=1
```

#### De-dup Example 3: Mixed Supersession (Some Have It, Some Don't)
```
Input (price_list_items - before de-duplication):
  Part A (row 1): original_price=$10, supersession=NULL
  Part A (row 2): original_price=$12, supersession=B
  Part A (row 3): original_price=$8, supersession=NULL

After de-duplication:
  Part A: original_price=$12, supersession=B
          (highest price wins, and it has the supersession)
  
  processing_errors: 1 row logged (DUPLICATE_PART_NUMBER)

Stats: duplicates_found=1, duplicates_removed=2
```

#### De-dup Example 4: Different Prices AND Different Supersessions
```
Input (price_list_items - before de-duplication):
  Part A (row 1): original_price=$10, supersession=C
  Part A (row 2): original_price=$12, supersession=NULL
  Part A (row 3): original_price=$12, supersession=B

After de-duplication:
  Part A: original_price=$12, supersession=B
          (highest price = $12, two rows tie → alphabetically lowest supersession: B < C, NULL last)
  
  processing_errors: 1 row logged (DUPLICATE_PART_NUMBER)
    - error_details: {variants: [{price: 10, supersession: "C"}, 
                                 {price: 12, supersession: null}, 
                                 {price: 12, supersession: "B"}],
                      resolved_price: 12, resolved_supersession: "B"}

Stats: duplicates_found=1, duplicates_removed=2
```

#### De-dup Example 5: No Conflict (Same Price, Same Supersession)
```
Input (price_list_items - before de-duplication):
  Part A (row 1): original_price=$10, supersession=B
  Part A (row 2): original_price=$10, supersession=B

After de-duplication:
  Part A: original_price=$10, supersession=B
  
  processing_errors: 0 rows logged (no conflict - identical data)

Stats: duplicates_found=1, duplicates_removed=1
```

### 4.4 Supersession Chain Examples

#### Example 1: Simple Chain
```
Input (price_list_items):
  Part A: original_price=$100, supersession=B
  Part B: original_price=$80, supersession=C
  Part C: original_price=$60, supersession=NULL

After reconciliation:
  Part A: effective_price=$60, terminal=C, inherited_from=C, status=OK
  Part B: effective_price=$60, terminal=C, inherited_from=C, status=OK
  Part C: effective_price=$60, terminal=C, inherited_from=NULL, status=NO_supersession

CSV Output (Price column):
  Part A: $60 (uses C's price)
  Part B: $60 (uses C's price)
  Part C: $60 (own price)
```

#### Example 2: supersession Not in File
```
Input (price_list_items):
  Part A: original_price=$100, supersession=B
  (Part B not in file)

After reconciliation:
  Part A: effective_price=$100, terminal=B, status=OK
  Part B: effective_price=$100, is_synthetic=TRUE, inherited_from=A, status=OK

CSV Output:
  Part A: $100 (supersession not found, uses own price)
  Part B: $100 (SYNTHETIC - added with inherited price)
```

#### Example 3: Multiple Parts Supersede to Same Non-Existent Part
```
Input (price_list_items):
  Part A: original_price=$10, supersession=C
  Part B: original_price=$12, supersession=C
  (Part C not in file)

After reconciliation:
  Part A: effective_price=$12, terminal=C, inherited_from=C, status=OK
  Part B: effective_price=$12, terminal=C, inherited_from=C, status=OK
  Part C: effective_price=$12, is_synthetic=TRUE, inherited_from=B, status=OK
          (ONE synthetic row created with highest parent price)
  
  processing_errors: 1 row logged (AMBIGUOUS_SYNTHETIC_PRICE)

CSV Output:
  Part A: $12 (gets synthetic C's price)
  Part B: $12 (gets synthetic C's price)
  Part C: $12 (SYNTHETIC - created with highest parent price from B)

Summary Email: "1 synthetic part had ambiguous price (multiple parents with different prices)"
```

**Note**: Part C is added only once. The price is taken from Part B ($12) because it has the highest 
original_price among parts that supersede to C. This is logged as a warning because the data is 
ambiguous - suppliers should ideally provide consistent pricing or include Part C explicitly.

#### Example 4: Circular Reference
```
Input (price_list_items):
  Part A: original_price=$100, supersession=B
  Part B: original_price=$80, supersession=C
  Part C: original_price=$60, supersession=A  ← Circular!

After reconciliation:
  Part A: effective_price=$100, status=CIRCULAR_REF, error_message="Circular chain: A → B → C → A"
  Part B: effective_price=$80, status=CIRCULAR_REF
  Part C: effective_price=$60, status=CIRCULAR_REF
  
  processing_errors: 3 rows logged

CSV Output (uses original prices as fallback):
  Part A: $100
  Part B: $80
  Part C: $60

Summary Email: "3 parts had circular supersession references"
```

---

## 5. Python Integration

### 5.1 Complete Processing Flow

```python
# src/storage/bigquery_processor.py

from google.cloud import bigquery, storage
from google.oauth2 import service_account
import uuid
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Generator
from datetime import date

class BigQueryPriceListProcessor:
    """
    Handles BigQuery operations for price list processing.
    
    Memory-efficient flow using GCS as intermediary:
    1. Parse source file → stream to local CSV (existing pipeline)
    2. Upload local CSV to GCS staging bucket
    3. Create price_list record in BigQuery
    4. Load items from GCS → BigQuery (load job, free & atomic)
    5. Call reconciliation stored procedure
    6. Export reconciled data to GCS
    7. Stream download from GCS → Drive upload
    8. Cleanup GCS staging files
    
    This preserves the existing streaming architecture's memory efficiency
    while enabling BigQuery-based supersession reconciliation.
    """
    
    # GCS paths for staging
    STAGING_PREFIX = 'bigquery_staging/input'
    RECONCILED_PREFIX = 'bigquery_staging/reconciled'
    
    def __init__(
        self, 
        project_id: str, 
        dataset_id: str = 'PRICING',
        staging_bucket: str = 'pricing-email-bot-bucket',
        service_account_info: Optional[Dict] = None,
        bigquery_config: Optional[Dict] = None
    ):
        """
        Initialize processor with proper credentials.
        
        Follows the same credential pattern as DriveUploader and GmailClient:
        - If service_account_info provided: use explicit service account credentials
        - Otherwise: fall back to Application Default Credentials (ADC) for local dev
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset name
            staging_bucket: GCS bucket for staging files
            service_account_info: Service account credentials dict (from core_config.json)
            bigquery_config: BigQuery config from core_config.json (includes cleanup_on_failure)
        """
        # Use service account credentials if provided (consistent with DriveUploader pattern)
        if service_account_info and len(service_account_info) > 0:
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            )
            self.bq_client = bigquery.Client(project=project_id, credentials=credentials)
            self.storage_client = storage.Client(project=project_id, credentials=credentials)
            logger.debug("BigQueryPriceListProcessor initialized with service account credentials")
        else:
            # Use Application Default Credentials for local development
            import google.auth
            credentials, detected_project = google.auth.default(
                scopes=[
                    'https://www.googleapis.com/auth/bigquery',
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            )
            self.bq_client = bigquery.Client(project=project_id, credentials=credentials)
            self.storage_client = storage.Client(project=project_id, credentials=credentials)
            logger.debug(
                "BigQueryPriceListProcessor initialized with Application Default Credentials",
                detected_project=detected_project
            )
        
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.staging_bucket = staging_bucket
        self.config = bigquery_config or {}
        self.max_chain_depth = self.config.get('reconciliation', {}).get('max_chain_depth', 10)
    
    def process_price_list(
        self,
        local_csv_path: str,
        supplier: str,
        brand: str,
        currency: str,
        location: str,
        source_filename: str,
        valid_from_date: Optional[date] = None
    ) -> Tuple[str, str]:
        """
        Full processing pipeline with GCS intermediary and dedicated staging table.
        
        Uses a dedicated staging table per price list for cost efficiency:
        - Stored procedure operates ONLY on staging table (~50MB scan)
        - Avoids scanning large price_list_items table (saves ~$0.05/query)
        - After reconciliation, results inserted into main table for analytics
        
        Args:
            local_csv_path: Path to locally generated CSV (from existing streaming parser)
            supplier: Supplier name
            brand: Brand name
            currency: Currency code
            location: Location string
            source_filename: Original source filename
            valid_from_date: Price list validity date
            
        Returns: 
            Tuple of (price_list_id, gcs_reconciled_path)
            
        The caller can then stream download from gcs_reconciled_path to Drive.
        """
        price_list_id = str(uuid.uuid4())
        gcs_input_path = None
        gcs_reconciled_path = None
        staging_table_id = None
        
        try:
            # Step 1: Upload local CSV to GCS
            gcs_input_path = f'{self.STAGING_PREFIX}/{price_list_id}.csv'
            self._upload_to_gcs(local_csv_path, gcs_input_path)
            
            # Step 2: Create price_list record
            self._create_price_list(
                price_list_id, supplier, brand, currency, 
                location, source_filename, valid_from_date
            )
            
            # Step 3: Load items to dedicated staging table (free load job)
            staging_table_id = self._load_items_to_staging(price_list_id, gcs_input_path, currency)
            
            # Step 4: Call reconciliation procedure on staging table only
            # This is cost-efficient: only scans ~50MB staging table, not 10GB+ main table
            self._reconcile(staging_table_id)
            
            # Step 5: Export reconciled data from staging table to GCS
            gcs_reconciled_path = f'{self.RECONCILED_PREFIX}/{price_list_id}'
            self._export_reconciled_to_gcs(staging_table_id, price_list_id, gcs_reconciled_path)
            
            # Step 6: Insert reconciled data into main table for analytics
            self._insert_to_main_table(staging_table_id, price_list_id)
            
            # Step 7: Drop staging table
            self._drop_staging_table(staging_table_id)
            
            return price_list_id, f'gs://{self.staging_bucket}/{gcs_reconciled_path}'
            
        except Exception as e:
            # Compensation: cleanup on failure
            self._cleanup_failed_processing(price_list_id, gcs_input_path, gcs_reconciled_path, staging_table_id)
            raise
    
    def _upload_to_gcs(self, local_path: str, gcs_path: str):
        """Upload local file to GCS staging bucket."""
        bucket = self.storage_client.bucket(self.staging_bucket)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
    
    def _create_price_list(
        self, 
        price_list_id: str,
        supplier: str, 
        brand: str, 
        currency: str, 
        location: str,
        source_filename: str, 
        valid_from_date: Optional[date]
    ):
        """Insert price_list record."""
        query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.price_lists` 
            (price_list_id, supplier, brand, currency, location, 
             source_filename, valid_from_date, upload_timestamp)
            VALUES (@price_list_id, @supplier, @brand, @currency, @location,
                    @source_filename, @valid_from_date, CURRENT_TIMESTAMP())
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("supplier", "STRING", supplier),
                bigquery.ScalarQueryParameter("brand", "STRING", brand),
                bigquery.ScalarQueryParameter("currency", "STRING", currency),
                bigquery.ScalarQueryParameter("location", "STRING", location),
                bigquery.ScalarQueryParameter("source_filename", "STRING", source_filename),
                bigquery.ScalarQueryParameter("valid_from_date", "DATE", valid_from_date),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
    
    def _load_items_to_staging(self, price_list_id: str, gcs_path: str, currency: str) -> str:
        """
        Load items from GCS CSV into a dedicated staging table.
        
        Uses a dedicated staging table per price list for cost efficiency:
        - Stored procedure operates ONLY on this small table (~50MB)
        - Avoids scanning large price_list_items table (saves ~$0.05/query)
        - After reconciliation, results inserted into main table
        
        Load jobs are:
        - Free (no query costs)
        - Fast (parallel loading)
        - Atomic (all or nothing)
        - Memory-efficient (no Python memory overhead)
        
        Returns:
            staging_table_id: Full table ID for the staging table
        """
        staging_table_id = f'{self.project_id}.{self.dataset_id}._staging_{price_list_id.replace("-", "_")}'
        
        # Schema includes reconciliation columns that stored procedure will populate
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=[
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("supplier_name", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("currency", "STRING"),
                bigquery.SchemaField("part_number", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("former_part_number", "STRING"),
                bigquery.SchemaField("supersession", "STRING"),
                bigquery.SchemaField("original_price", "NUMERIC"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        uri = f'gs://{self.staging_bucket}/{gcs_path}'
        load_job = self.bq_client.load_table_from_uri(
            uri, staging_table_id, job_config=job_config
        )
        load_job.result()  # Wait for completion
        
        # Add reconciliation columns to staging table
        alter_query = f"""
            ALTER TABLE `{staging_table_id}`
            ADD COLUMN IF NOT EXISTS item_id STRING,
            ADD COLUMN IF NOT EXISTS effective_price NUMERIC,
            ADD COLUMN IF NOT EXISTS terminal_part_number STRING,
            ADD COLUMN IF NOT EXISTS supersession_chain_length INT64,
            ADD COLUMN IF NOT EXISTS price_inherited_from STRING,
            ADD COLUMN IF NOT EXISTS reconciliation_status STRING,
            ADD COLUMN IF NOT EXISTS reconciliation_error_message STRING,
            ADD COLUMN IF NOT EXISTS is_synthetic BOOL,
            ADD COLUMN IF NOT EXISTS source_row_number INT64
        """
        self.bq_client.query(alter_query).result()
        
        # Initialize item_id and source_row_number
        init_query = f"""
            UPDATE `{staging_table_id}`
            SET item_id = GENERATE_UUID(),
                source_row_number = ROW_NUMBER() OVER ()
            WHERE TRUE
        """
        self.bq_client.query(init_query).result()
        
        return staging_table_id
    
    def _reconcile(self, staging_table_id: str, price_list_id: str):
        """
        Call the reconciliation stored procedure on the staging table.
        
        The stored procedure operates ONLY on the staging table,
        avoiding costly scans of the main price_list_items table.
        """
        query = f"""
            CALL `{self.project_id}.{self.dataset_id}.reconcile_supersessions_staging`(
                @staging_table_id,
                @price_list_id,
                @max_chain_depth
            )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("staging_table_id", "STRING", staging_table_id),
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                bigquery.ScalarQueryParameter("max_chain_depth", "INT64", self.max_chain_depth),
            ]
        )
        
        self.bq_client.query(query, job_config=job_config).result()
    
    def _export_reconciled_to_gcs(self, staging_table_id: str, price_list_id: str, gcs_path: str):
        """
        Export reconciled data from staging table directly to GCS.
        
        Exports from the staging table (not main table) for efficiency.
        
        Uses EXPORT DATA which:
        - Writes directly to GCS (no Python memory)
        - Handles large datasets efficiently
        - Produces properly formatted CSV
        """
        # Get metadata for export
        metadata_query = f"""
            SELECT supplier, brand, location, currency
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE price_list_id = @price_list_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        result = list(self.bq_client.query(metadata_query, job_config=job_config).result())[0]
        
        # Export from staging table (not main table - cost efficient)
        export_query = f"""
            EXPORT DATA OPTIONS(
                uri='gs://{self.staging_bucket}/{gcs_path}/*.csv',
                format='CSV',
                overwrite=true,
                header=true
            ) AS
            SELECT 
                '{result.brand}' as Brand,
                '{result.supplier}' as `Supplier Name`,
                '{result.location}' as Location,
                '{result.currency}' as Currency,
                part_number as `Part Number`,
                description as Description,
                COALESCE(former_part_number, '') as `FORMER PN`,
                COALESCE(supersession, '') as SUPERSESSION,
                effective_price as Price
            FROM `{staging_table_id}`
            ORDER BY source_row_number, part_number
        """
        
        self.bq_client.query(export_query).result()
    
    def _insert_to_main_table(self, staging_table_id: str, price_list_id: str):
        """
        Insert reconciled data from staging table into main price_list_items table.
        
        Called after export to GCS, preserves data in main table for analytics.
        """
        insert_query = f"""
            INSERT INTO `{self.project_id}.{self.dataset_id}.price_list_items`
            (item_id, price_list_id, part_number, currency, original_price,
             effective_price, former_part_number, supersession, description,
             terminal_part_number, supersession_chain_length, price_inherited_from,
             reconciliation_status, reconciliation_error_message, is_synthetic,
             source_row_number, created_timestamp, reconciled_timestamp)
            SELECT 
                item_id,
                @price_list_id,
                part_number,
                currency,
                original_price,
                effective_price,
                former_part_number,
                supersession,
                description,
                terminal_part_number,
                supersession_chain_length,
                price_inherited_from,
                reconciliation_status,
                reconciliation_error_message,
                COALESCE(is_synthetic, FALSE),
                source_row_number,
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            FROM `{staging_table_id}`
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        self.bq_client.query(insert_query, job_config=job_config).result()
    
    def _drop_staging_table(self, staging_table_id: str):
        """Drop the staging table after processing is complete."""
        self.bq_client.delete_table(staging_table_id, not_found_ok=True)
    
    def stream_reconciled_from_gcs(self, gcs_path: str) -> Generator[bytes, None, None]:
        """
        Stream download reconciled CSV from GCS in chunks.
        
        Memory-efficient: yields chunks instead of loading entire file.
        Can be piped directly to Drive upload.
        
        Args:
            gcs_path: Full GCS URI (gs://bucket/path)
            
        Yields:
            Bytes chunks for streaming upload
        """
        # Parse gs:// URI
        if gcs_path.startswith('gs://'):
            gcs_path = gcs_path[5:]
        bucket_name, blob_path = gcs_path.split('/', 1)
        
        bucket = self.storage_client.bucket(bucket_name)
        
        # EXPORT DATA creates files like 000000000000.csv, 000000000001.csv, etc.
        # For most price lists, there will be just one file
        blobs = list(bucket.list_blobs(prefix=blob_path))
        
        for blob in sorted(blobs, key=lambda b: b.name):
            if blob.name.endswith('.csv'):
                # Stream in 1MB chunks
                with blob.open('rb') as f:
                    while chunk := f.read(1024 * 1024):
                        yield chunk
    
    def get_reconciliation_errors(self, price_list_id: str) -> List[Dict]:
        """Get errors for summary email (small result set, OK to fetch to memory)."""
        query = f"""
            SELECT 
                part_number,
                reconciliation_status,
                reconciliation_error_message
            FROM `{self.project_id}.{self.dataset_id}.price_list_items`
            WHERE price_list_id = @price_list_id
              AND reconciliation_status NOT IN ('OK', 'NO_supersession', 'PENDING')
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        results = self.bq_client.query(query, job_config=job_config).result()
        return [dict(row) for row in results]
    
    def get_reconciliation_stats(self, price_list_id: str) -> Dict:
        """Get reconciliation statistics for reporting."""
        query = f"""
            SELECT 
                total_items,
                reconciled_items,
                items_with_errors,
                synthetic_items_added,
                duplicates_found,
                duplicates_removed,
                reconciliation_status,
                reconciliation_timestamp
            FROM `{self.project_id}.{self.dataset_id}.price_lists`
            WHERE price_list_id = @price_list_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        
        result = list(self.bq_client.query(query, job_config=job_config).result())
        return dict(result[0]) if result else {}
    
    def _cleanup_failed_processing(
        self, 
        price_list_id: str, 
        gcs_input_path: Optional[str],
        gcs_reconciled_path: Optional[str],
        staging_table_id: Optional[str] = None
    ):
        """
        Saga pattern compensation: cleanup partial data on failure.
        
        Behavior controlled by cleanup_on_failure.mode config:
        - TEST: Preserve data for inspection, mark price_list as FAILED
        - PRODUCTION: Full cleanup to avoid orphaned data
        
        See Section 7.2 for detailed cleanup logic and configuration.
        """
        cleanup_config = self.config.get('cleanup_on_failure', {})
        mode = cleanup_config.get('mode', 'test')  # Default to test mode
        mode_config = cleanup_config.get(mode, {})
        
        if mode_config.get('log_cleanup_skipped', True):
            logger.warning(
                f"Cleanup mode={mode}. Resources: staging_table={staging_table_id}, "
                f"gcs_input={gcs_input_path}, gcs_reconciled={gcs_reconciled_path}"
            )
        
        # Always mark price_list as FAILED for tracking
        try:
            update_query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.price_lists`
                SET reconciliation_status = 'FAILED',
                    reconciliation_timestamp = CURRENT_TIMESTAMP()
                WHERE price_list_id = @price_list_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                ]
            )
            self.bq_client.query(update_query, job_config=job_config).result()
        except Exception:
            pass  # Best effort
        
        # Conditional cleanup based on mode (see Section 7.2 for details)
        if mode_config.get('drop_staging_table', False) and staging_table_id:
            try:
                self.bq_client.delete_table(staging_table_id, not_found_ok=True)
            except Exception:
                pass
        
        if mode_config.get('delete_price_list_record', False):
            try:
                delete_items = f"""
                    DELETE FROM `{self.project_id}.{self.dataset_id}.price_list_items`
                    WHERE price_list_id = @price_list_id
                """
                delete_list = f"""
                    DELETE FROM `{self.project_id}.{self.dataset_id}.price_lists`
                    WHERE price_list_id = @price_list_id
                """
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                    ]
                )
                self.bq_client.query(delete_items, job_config=job_config).result()
                self.bq_client.query(delete_list, job_config=job_config).result()
            except Exception:
                pass
        
        if mode_config.get('delete_gcs_files', False):
            self._cleanup_gcs_files(price_list_id)
    
    def cleanup_gcs_files(self, price_list_id: str):
        """Public method to cleanup GCS staging files after successful Drive upload."""
        self._cleanup_gcs_files(price_list_id)
    
    def _cleanup_gcs_files(self, price_list_id: str):
        """Delete staging files from GCS."""
        bucket = self.storage_client.bucket(self.staging_bucket)
        
        # Delete input file
        input_blob = bucket.blob(f'{self.STAGING_PREFIX}/{price_list_id}.csv')
        input_blob.delete(if_exists=True)
        
        # Delete reconciled files (may be multiple from EXPORT DATA)
        reconciled_prefix = f'{self.RECONCILED_PREFIX}/{price_list_id}'
        blobs = bucket.list_blobs(prefix=reconciled_prefix)
        for blob in blobs:
            blob.delete()
```

### 5.2 Integration with Existing File Generator

The key insight is that we **preserve the existing streaming pipeline** for parsing, 
then use GCS as the intermediary for BigQuery operations.

```python
# Modified flow in file_generator.py or orchestrator

def generate_csv_with_reconciliation(
    self,
    input_file_path: str,
    parser: PriceListParser,
    brand_config: Dict,
    supplier_config: Dict,
    valid_from_date: datetime,
    output_path: str,
    bq_processor: BigQueryPriceListProcessor,
    drive_uploader: DriveUploader
) -> Tuple[str, int, int, List[str]]:
    """
    Generate CSV with BigQuery-based supersession reconciliation.
    
    Memory-efficient flow:
    1. Use existing streaming parser → local CSV (no change to current flow)
    2. Upload local CSV → GCS → BigQuery load job
    3. Run reconciliation stored procedure
    4. Export reconciled data → GCS
    5. Stream from GCS → Drive upload
    
    Peak memory stays at ~100MB regardless of file size.
    """
    warnings = []
    temp_csv_path = None
    
    try:
        # Step 1: Use EXISTING streaming pipeline to generate local CSV
        # This preserves current memory efficiency (~100MB for 500k rows)
        temp_csv_path = Path(output_path) / f'_temp_{uuid.uuid4()}.csv'
        
        total_rows, valid_rows, parse_errors = parser.stream_parse_to_csv(
            file_path=input_file_path,
            brand_config=brand_config,
            output_path=str(temp_csv_path),
            # ... other existing parameters
        )
        warnings.extend(parse_errors)
        
        # Step 2: Process through BigQuery (GCS intermediary)
        # This uploads to GCS, loads to BigQuery, reconciles, exports back to GCS
        price_list_id, gcs_reconciled_path = bq_processor.process_price_list(
            local_csv_path=str(temp_csv_path),
            supplier=supplier_config['supplier'],
            brand=brand_config['brand'],
            currency=brand_config['currency'],
            location=brand_config['location'],
            source_filename=Path(input_file_path).name,
            valid_from_date=valid_from_date.date()
        )
        
        # Step 3: Check for reconciliation errors
        errors = bq_processor.get_reconciliation_errors(price_list_id)
        if errors:
            for err in errors:
                warnings.append(
                    f"supersession error for {err['part_number']}: "
                    f"{err['reconciliation_status']} - {err['reconciliation_error_message']}"
                )
        
        # Step 4: Get reconciliation stats
        stats = bq_processor.get_reconciliation_stats(price_list_id)
        total_rows = stats.get('total_items', total_rows)
        
        # Step 5: Stream from GCS directly to Drive (memory-efficient)
        filename = self._generate_filename(
            brand=brand_config['brand'],
            supplier=supplier_config['supplier'],
            location=brand_config['location'],
            currency=brand_config['currency'],
            valid_from_date=valid_from_date
        )
        
        drive_file_id = drive_uploader.upload_from_gcs_stream(
            gcs_path=gcs_reconciled_path,
            filename=filename,
            folder_id=brand_config['driveFolderId'],
            chunk_generator=bq_processor.stream_reconciled_from_gcs(gcs_reconciled_path)
        )
        
        # Step 6: Cleanup GCS staging files
        bq_processor.cleanup_gcs_files(price_list_id)
        
        return filename, total_rows, stats.get('reconciled_items', valid_rows), warnings
        
    finally:
        # Cleanup temp local CSV
        if temp_csv_path and temp_csv_path.exists():
            temp_csv_path.unlink()
```

---

## 6. Configuration

### 6.1 Core Config Addition

```json
// config/core/core_config.json
{
  "bigquery": {
    "enabled": true,
    "project_id": "pricing-email-bot",
    "dataset_id": "PRICING",
    "location": "US",
    
    "reconciliation": {
      "enabled": true,
      "max_chain_depth": 10,
      "add_missing_supersessions": true,
      "audit_logging_enabled": false
    },
    
    "cleanup_on_failure": {
      "mode": "test",  // "test" or "production"
      "test": {
        "drop_staging_table": false,
        "delete_price_list_record": false,
        "delete_gcs_files": false,
        "log_cleanup_skipped": true
      },
      "production": {
        "drop_staging_table": true,
        "delete_price_list_record": true,
        "delete_gcs_files": true,
        "log_cleanup_skipped": false
      }
    }
  }
}
```

**Notes**:
- supersession reconciliation applies to **all suppliers** uniformly
- Uses supersession data directly from supplier files
- **cleanup_on_failure.mode**: Controls whether failed data is preserved (`test`) or cleaned up (`production`)
  - `test` mode: Preserves staging tables, GCS files, and BigQuery records for debugging
  - `production` mode: Full automatic cleanup on failure
- Future enhancement: canonical supersession reference table to override/supplement supplier data

---

## 7. Error Handling

### 7.1 Error Types and Actions

| Error Type | Detection | Action | Notification |
|------------|-----------|--------|--------------|
| Circular reference | SQL recursive CTE | Log to `processing_errors`, exclude from canonical | Include in daily summary email |
| Chain too long (>10) | Depth counter | Truncate chain, log warning | Include in daily summary |
| Missing price | NULL price in chain | Use last known price | Log only |
| Self-reference | A→A | Filter out in SQL | Log only |
| GCS upload failure | Storage API error | Retry 3x, then fail with cleanup | Alert email |
| BigQuery load failure | Load job error | Cleanup (mode-dependent) | Alert email |
| Stored procedure failure | Query error | Cleanup (mode-dependent) | Alert email |

### 7.2 Saga Pattern: Compensation on Failure

When processing fails at any step, the system can perform cleanup (compensation) to avoid orphaned data. 
However, **cleanup behavior is controlled by a TEST/PRODUCTION flag** to allow inspection of failed data before it's production-ready.

#### Configuration

```json
// config/core/core_config.json
{
  "bigquery": {
    "cleanup_on_failure": {
      "mode": "test",  // "test" or "production"
      "test": {
        "drop_staging_table": false,
        "delete_price_list_record": false,
        "delete_gcs_files": false,
        "log_cleanup_skipped": true
      },
      "production": {
        "drop_staging_table": true,
        "delete_price_list_record": true,
        "delete_gcs_files": true,
        "log_cleanup_skipped": false
      }
    }
  }
}
```

#### Cleanup Behavior by Mode

| Resource | TEST Mode | PRODUCTION Mode |
|----------|-----------|-----------------|
| Staging table `_staging_{uuid}` | **Preserved** for inspection | Dropped |
| `price_lists` record | **Preserved** (status=FAILED) | Deleted |
| `price_list_items` (if any) | **Preserved** | Deleted |
| GCS input file | **Preserved** | Deleted |
| GCS reconciled files | **Preserved** | Deleted |

#### Compensation Logic (Python)

```python
def _cleanup_failed_processing(
    self, 
    price_list_id: str, 
    gcs_input_path: Optional[str],
    gcs_reconciled_path: Optional[str],
    staging_table_id: Optional[str] = None
):
    """
    Saga pattern compensation: cleanup partial data on failure.
    
    Behavior controlled by cleanup_on_failure.mode config:
    - TEST: Preserve data for inspection, mark price_list as FAILED
    - PRODUCTION: Full cleanup to avoid orphaned data
    """
    cleanup_config = self.config.get('cleanup_on_failure', {})
    mode = cleanup_config.get('mode', 'test')
    mode_config = cleanup_config.get(mode, {})
    
    if mode_config.get('log_cleanup_skipped', False):
        logger.warning(
            f"Cleanup skipped (mode={mode}). Resources preserved for inspection:",
            price_list_id=price_list_id,
            staging_table=staging_table_id,
            gcs_input=gcs_input_path,
            gcs_reconciled=gcs_reconciled_path
        )
    
    # Mark price_list as FAILED (always, for tracking)
    try:
        update_query = f"""
            UPDATE `{self.project_id}.{self.dataset_id}.price_lists`
            SET reconciliation_status = 'FAILED',
                reconciliation_timestamp = CURRENT_TIMESTAMP()
            WHERE price_list_id = @price_list_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
            ]
        )
        self.bq_client.query(update_query, job_config=job_config).result()
    except Exception:
        pass  # Best effort
    
    # Conditional cleanup based on mode
    if mode_config.get('drop_staging_table', False) and staging_table_id:
        try:
            self.bq_client.delete_table(staging_table_id, not_found_ok=True)
        except Exception:
            pass
    
    if mode_config.get('delete_price_list_record', False):
        try:
            delete_items = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.price_list_items`
                WHERE price_list_id = @price_list_id
            """
            delete_list = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.price_lists`
                WHERE price_list_id = @price_list_id
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("price_list_id", "STRING", price_list_id),
                ]
            )
            self.bq_client.query(delete_items, job_config=job_config).result()
            self.bq_client.query(delete_list, job_config=job_config).result()
        except Exception:
            pass
    
    if mode_config.get('delete_gcs_files', False):
        self._cleanup_gcs_files(price_list_id)
```

#### Benefits of TEST Mode

- **Inspect failed data**: View staging table contents to debug reconciliation issues
- **Verify GCS files**: Check if input CSV was correctly formatted
- **Track failures**: `price_lists` table shows FAILED status with timestamp
- **Manual cleanup**: Use BigQuery console or scripts to cleanup after investigation

#### Switching to PRODUCTION Mode

Once the system is stable and tested:
1. Change `cleanup_on_failure.mode` from `"test"` to `"production"`
2. Full automatic cleanup will occur on failures
3. No manual intervention needed

### 7.3 Error Query for Reporting

```sql
-- Query for daily summary email
SELECT 
  error_type,
  COUNT(*) as error_count,
  ARRAY_AGG(STRUCT(supplier, brand, part_number, error_message) LIMIT 10) as examples
FROM PRICING.processing_errors
WHERE created_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND is_resolved = FALSE
GROUP BY error_type;
```

### 7.4 Query to Find Failed Price Lists (TEST Mode)

```sql
-- Find failed price lists that need investigation
SELECT 
  price_list_id,
  supplier,
  brand,
  source_filename,
  upload_timestamp,
  reconciliation_timestamp
FROM PRICING.price_lists
WHERE reconciliation_status = 'FAILED'
ORDER BY reconciliation_timestamp DESC;

-- Find orphaned staging tables (for manual cleanup)
-- Run in BigQuery console:
-- SELECT table_name FROM `PRICING.INFORMATION_SCHEMA.TABLES` WHERE table_name LIKE '_staging_%';
```

---

## 8. Querying Data in BigQuery

### 8.1 Get Items for a Specific Price List

```sql
-- Get all items for a price list (what Python reads back)
SELECT 
  part_number,
  description,
  original_price,
  effective_price,
  supersession,
  reconciliation_status,
  is_synthetic
FROM PRICING.price_list_items
WHERE price_list_id = 'abc-123-uuid'
ORDER BY source_row_number;
```

### 8.2 Find Reconciliation Errors

```sql
-- Get all errors for a price list (for summary email)
SELECT 
  pli.part_number,
  pli.reconciliation_status,
  pli.reconciliation_error_message,
  pe.error_details
FROM PRICING.price_list_items pli
LEFT JOIN PRICING.processing_errors pe 
  ON pli.item_id = pe.item_id
WHERE pli.price_list_id = 'abc-123-uuid'
  AND pli.reconciliation_status NOT IN ('OK', 'NO_supersession');
```

### 8.3 Price List Summary

```sql
-- Get processing summary for a price list
SELECT 
  price_list_id,
  supplier,
  brand,
  source_filename,
  reconciliation_status,
  total_items,
  reconciled_items,
  items_with_errors,
  synthetic_items_added,
  duplicates_found,
  duplicates_removed,
  reconciliation_timestamp
FROM PRICING.price_lists
WHERE price_list_id = 'abc-123-uuid';
```

### 8.4 Analytics Queries (Future Use)

```sql
-- Find all parts with supersession issues across all price lists
SELECT 
  pl.supplier,
  pl.brand,
  pli.part_number,
  pli.reconciliation_status,
  pl.source_filename,
  pl.upload_timestamp
FROM PRICING.price_list_items pli
JOIN PRICING.price_lists pl ON pli.price_list_id = pl.price_list_id
WHERE pli.reconciliation_status = 'CIRCULAR_REF'
ORDER BY pl.upload_timestamp DESC;

-- Price history for a specific part
SELECT 
  pl.upload_timestamp,
  pl.supplier,
  pli.original_price,
  pli.effective_price,
  pli.supersession
FROM PRICING.price_list_items pli
JOIN PRICING.price_lists pl ON pli.price_list_id = pl.price_list_id
WHERE pli.part_number = '12345678901'
ORDER BY pl.upload_timestamp DESC;
```

---

## 9. Migration Plan

### 9.1 Phase 1: BigQuery Setup (Day 1-2)

1. Create BigQuery dataset `PRICING`
2. Create tables: `price_lists`, `price_list_items`, `processing_errors`
3. Create stored procedure `reconcile_supersessions`
4. Test procedure with sample data manually

### 9.2 Phase 2: Python Integration (Days 3-5)

1. Create `BigQueryPriceListProcessor` class
2. Add BigQuery credentials to Secret Manager
3. Modify file processing flow to:
   - Parse file (existing)
   - Upload to BigQuery
   - Call reconciliation procedure
   - Read back reconciled data
   - Generate CSV with effective_price
   - Upload to Drive (existing)
4. Add reconciliation errors to summary email

### 9.3 Phase 3: Testing (Days 6-8)

1. Run with test suppliers in parallel (BigQuery + existing flow)
2. Compare CSV outputs for consistency
3. Verify error handling for circular references
4. Performance testing with large files (500K+ rows)
5. Monitor BigQuery costs

### 9.4 Phase 4: Production Rollout (Days 9-14)

1. Enable for all suppliers
2. Remove parallel path (BigQuery only)
3. Documentation update
4. Monitor and tune

---

## 10. Cost Estimate

| Resource | Usage | Monthly Cost |
|----------|-------|--------------|
| BigQuery Storage | ~10GB (5M rows × 2KB) | ~$0.20 |
| BigQuery Queries | ~100GB scanned/month | ~$0.50 |
| Scheduled Queries | 720 runs/month | Included |
| **Total** | | **~$0.70/month** |

---

## 11. Open Questions

1. **Validity dates**: How should supersession interact with `valid_from_date` and `valid_to_date`? (Current design: independent, supersession resolved within each batch)

2. **Multiple currencies**: If the same part exists in USD and EUR, should supersession be currency-specific? (Current design: yes, reconciliation is scoped to supplier + brand + currency)

3. **Retention policy**: How long to keep data in `staging_prices`? (Suggestion: 90 days, then archive to cold storage)

## 12. Future Enhancements

These are explicitly deferred for later implementation:

1. **Canonical supersession reference table**: A master table of known supersession relationships that can override or supplement supplier-provided data. This would allow corrections when supplier data is wrong.

2. **Per-supplier configuration**: Ability to disable or customize reconciliation for specific suppliers with unreliable supersession data.

3. **Cross-file supersession lookup**: Looking up supersession prices from historical uploads (e.g., if Part B was in last month's file but not this month's).

4. **Streaming GCS→Drive upload**: Direct streaming from GCS to Google Drive without downloading to a temp file first. Current implementation downloads reconciled CSV from GCS to a local temp file, then uploads to Drive using existing `DriveUploader.upload_file()`. A more memory-efficient approach would be to stream directly from GCS to Drive using chunked transfers and the resumable upload API. This would eliminate the temp file and reduce memory usage for very large files (500k+ rows).

---

## 13. Approval Checklist

- [ ] **Schema design approved**: `price_lists` + `price_list_items` tables
- [ ] **Processing flow approved**: Python → BigQuery → Stored Procedure → Read back → CSV → Drive
- [ ] **Reconciliation logic approved**: Stored procedure with chain following
- [ ] **Error handling approved**: Circular refs use original price, logged to summary email
- [ ] **CSV format**: Same as today, Price column = effective_price
- [ ] **Migration plan approved**: 2-week rollout
- [ ] **Cost estimate acceptable**: ~$0.70/month additional
- [ ] **Future enhancements deferred**: Canonical reference table, per-supplier config, cross-file lookup

---

**Next Steps After Approval**:
1. Set up BigQuery dataset and tables
2. Create and test stored procedure
3. Implement `BigQueryPriceListProcessor` Python class
4. Integrate with existing file processing flow
5. Add reconciliation errors to summary email
6. Test with sample suppliers
7. Production rollout
