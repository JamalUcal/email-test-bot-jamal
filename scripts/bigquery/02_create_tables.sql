-- BigQuery Table Creation Script
-- Run this script after creating the PRICING dataset
-- 
-- Tables:
--   1. price_lists - Metadata for each uploaded file
--   2. canonical_prices - SCD Type 2 table for deduplicated price history
--   3. prices_current - Current prices only (ACTIVE/UNAVAILABLE), optimized for queries
--   4. processing_errors - Error log for reconciliation issues
--   5. supersession_audit - Audit log for price inheritance (optional)
--   6. price_list_items - DEPRECATED: kept for migration, use canonical_prices

DROP TABLE IF EXISTS `PRICING.price_lists`;
DROP TABLE IF EXISTS `PRICING.canonical_prices`;
DROP TABLE IF EXISTS `PRICING.prices_current`;
DROP TABLE IF EXISTS `PRICING.processing_errors`;
DROP TABLE IF EXISTS `PRICING.supersession_audit`;
DROP TABLE IF EXISTS `PRICING.price_list_items`;



-- ============================================================================
-- Table: price_lists
-- Metadata for each uploaded price list file. One row per file upload.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.price_lists` (
  -- Primary key (INT64 for efficient JOINs)
  price_list_seq INT64 NOT NULL,            -- Auto-incrementing sequence, used as FK in canonical_prices
  
  -- Logical identifier (UUID for external references and logging)
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
  upload_timestamp TIMESTAMP NOT NULL,             -- Set to CURRENT_TIMESTAMP() on insert
  reconciliation_status STRING,                    -- PENDING, COMPLETED, FAILED (default: PENDING)
  reconciliation_timestamp TIMESTAMP,
  
  -- Stats
  total_items INT64,
  reconciled_items INT64,
  items_with_errors INT64,
  synthetic_items_added INT64,              -- Supersessions not in original file
  duplicates_found INT64,                   -- Part numbers with duplicates
  duplicates_removed INT64,                 -- Duplicate rows removed
  
  -- Drive upload
  drive_file_id STRING,
  drive_file_url STRING,
  
  -- Staging table tracking (for deferred merge)
  staging_table_id STRING,                   -- Full table ID of staging table
  merge_status STRING                        -- PENDING, MERGED, FAILED
)
PARTITION BY DATE(upload_timestamp)
CLUSTER BY supplier, brand;


-- ============================================================================
-- Table: canonical_prices (SCD Type 2)
-- Deduplicated price history with validity periods.
-- This is the primary table for querying current and historical prices.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.canonical_prices` (
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
  -- ACTIVE: Current valid price
  -- UNAVAILABLE: Supplier sent price=0 (not available)
  -- HISTORY: Superseded by newer price
  -- DISCONTINUED: Explicitly marked as discontinued
  status STRING NOT NULL,
  
  -- Tracking
  last_seen_date DATE NOT NULL,              -- Last date we received this price
  first_seen_price_list_seq INT64 NOT NULL,  -- Price list that first introduced this price (FK to price_lists.price_list_seq)
  last_seen_price_list_seq INT64 NOT NULL,   -- Most recent price list with this price (FK to price_lists.price_list_seq)
  
  -- Supersession data
  former_part_number STRING,                 -- Part this replaced (informational)
  supersession STRING,                       -- Part that replaces this one
  terminal_part_number STRING,               -- End of chain (NULL if no supersession)
  supersession_chain_length INT64 DEFAULT 0,
  price_inherited_from STRING,               -- Part number price was inherited from
  
  -- Descriptive (preserved across updates if new value is NULL)
  description STRING,
  location STRING,
  
  -- Reconciliation metadata
  is_synthetic BOOL DEFAULT FALSE,           -- TRUE if added during reconciliation
  reconciliation_status STRING,              -- OK, CIRCULAR_REF, CHAIN_TOO_LONG, etc.
  reconciliation_error_message STRING
)
PARTITION BY valid_from
CLUSTER BY supplier, brand, part_number, status;


-- ============================================================================
-- Table: prices_current
-- Current prices only (ACTIVE and UNAVAILABLE status).
-- Maintained by merge_pending_to_canonical procedure.
-- Optimized for fast queries from NestJS application.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.prices_current` (
  -- Business keys
  supplier STRING NOT NULL,
  brand STRING NOT NULL,
  part_number STRING NOT NULL,               -- Normalized (uppercase, alphanumeric, padded)
  currency STRING NOT NULL,                  -- Currency code (USD, EUR, etc.)
  location STRING,
  
  -- Prices (NULL for UNAVAILABLE status)
  original_price NUMERIC(15, 4),             -- Price from supplier file
  effective_price NUMERIC(15, 4),            -- Price after supersession reconciliation
  
  -- Status: ACTIVE or UNAVAILABLE only
  -- ACTIVE: Current valid price
  -- UNAVAILABLE: Supplier sent price=0 (not available)
  status STRING NOT NULL,
  
  -- Tracking
  valid_from DATE NOT NULL,                  -- Date this price became effective
  last_seen_date DATE NOT NULL,              -- Last date we received this price
  last_seen_price_list_seq INT64 NOT NULL,   -- Most recent price list with this price (FK to price_lists.price_list_seq)
  
  -- Supersession data (for display)
  supersession STRING,                       -- Part that replaces this one
  terminal_part_number STRING,               -- End of chain (NULL if no supersession)
  
  -- Reference to canonical record
  item_id STRING NOT NULL,                   -- FK to canonical_prices.item_id
  
  -- Descriptive
  description STRING
)
CLUSTER BY part_number, supplier, brand, currency;


-- ============================================================================
-- Table: price_list_items (DEPRECATED)
-- Kept for backwards compatibility during migration.
-- Use canonical_prices for new queries.
-- Individual parts with both original and reconciled prices.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.price_list_items` (
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
  
  -- Supersession data
  former_part_number STRING,                -- Part this replaced (informational)
  supersession STRING,                      -- Part that replaces this one
  
  -- Reconciliation results
  terminal_part_number STRING,              -- End of chain (NULL if no supersession)
  supersession_chain_length INT64,                 -- Default: 0
  price_inherited_from STRING,              -- Part number price was inherited from
  
  -- Descriptive
  description STRING,
  
  -- Source tracking
  source_row_number INT64,
  is_synthetic BOOL,                         -- TRUE if added during reconciliation (default: FALSE)
  
  -- Reconciliation status (for error tracking)
  reconciliation_status STRING,                    -- Default: PENDING
    -- Values: PENDING, OK, NO_SUPERSESSION, CIRCULAR_REF, CHAIN_TOO_LONG, ERROR
  reconciliation_error_message STRING,
  
  -- Timestamps
  created_timestamp TIMESTAMP,               -- Set to CURRENT_TIMESTAMP() on insert
  reconciled_timestamp TIMESTAMP
)
PARTITION BY DATE(created_timestamp)
CLUSTER BY price_list_id, part_number;


-- ============================================================================
-- Table: processing_errors
-- Detailed error log for reconciliation issues.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.processing_errors` (
  error_id STRING NOT NULL,
  
  -- Links
  price_list_id STRING NOT NULL,
  item_id STRING,                           -- May be NULL for price_list level errors
  
  -- Context
  supplier STRING,
  brand STRING,
  part_number STRING,
  
  -- Error details
  error_type STRING NOT NULL,               -- 'CIRCULAR_SUPERSESSION', 'CHAIN_TOO_LONG', etc.
  error_message STRING,
  error_details JSON,                       -- Chain path, attempted lookups, etc.
  
  -- Resolution tracking
  is_resolved BOOL,                          -- Default: FALSE
  resolved_timestamp TIMESTAMP,
  resolved_by STRING,
  
  -- Metadata
  created_timestamp TIMESTAMP                -- Set to CURRENT_TIMESTAMP() on insert
)
PARTITION BY DATE(created_timestamp);


-- ============================================================================
-- Table: supersession_audit (Optional)
-- Audit log for price inheritance. Enabled via config flag.
-- ============================================================================
CREATE TABLE IF NOT EXISTS `PRICING.supersession_audit` (
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
  created_timestamp TIMESTAMP                -- Set to CURRENT_TIMESTAMP() on insert
)
PARTITION BY DATE(created_timestamp);
