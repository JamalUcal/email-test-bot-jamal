-- BigQuery Views for price_lists and canonical_prices joins
-- 
-- Views:
--   1. current_prices_with_metadata - Current active prices with price list metadata
--   2. price_history_with_metadata - All prices with price list metadata
--   3. supplier_price_summary - Summary stats by supplier/brand
--   4. processing_errors_summary - Errors with price list context
--
-- Note: JOINs use price_list_seq (INT64) for efficiency instead of price_list_id (UUID)


-- ============================================================================
-- View: current_prices_with_metadata
-- Current active prices joined with their source price list metadata.
-- Useful for getting the latest prices with upload/validity information.
-- Note: For high-frequency queries, use prices_current table directly.
-- ============================================================================
CREATE OR REPLACE VIEW `PRICING.current_prices_with_metadata` AS
SELECT
  -- Price data
  cp.item_id,
  cp.supplier,
  cp.brand,
  cp.part_number,
  cp.currency,
  cp.original_price,
  cp.effective_price,
  cp.status,
  cp.description,
  cp.location,
  
  -- SCD validity
  cp.valid_from,
  cp.valid_until,
  cp.last_seen_date,
  
  -- Supersession data
  cp.former_part_number,
  cp.supersession,
  cp.terminal_part_number,
  cp.supersession_chain_length,
  cp.price_inherited_from,
  cp.is_synthetic,
  cp.reconciliation_status AS item_reconciliation_status,
  
  -- Price list metadata (from first seen)
  cp.first_seen_price_list_seq,
  pl_first.price_list_id AS first_seen_price_list_id,
  pl_first.source_filename AS first_seen_filename,
  pl_first.source_email_subject AS first_seen_email_subject,
  pl_first.source_email_date AS first_seen_email_date,
  pl_first.upload_timestamp AS first_upload_timestamp,
  
  -- Price list metadata (from last seen)
  cp.last_seen_price_list_seq,
  pl_last.price_list_id AS last_seen_price_list_id,
  pl_last.source_filename AS last_seen_filename,
  pl_last.source_email_subject AS last_seen_email_subject,
  pl_last.source_email_date AS last_seen_email_date,
  pl_last.upload_timestamp AS last_upload_timestamp,
  pl_last.valid_from_date AS price_list_valid_from,
  pl_last.valid_to_date AS price_list_valid_to,
  pl_last.drive_file_url

FROM `PRICING.canonical_prices` cp
LEFT JOIN `PRICING.price_lists` pl_first
  ON cp.first_seen_price_list_seq = pl_first.price_list_seq
LEFT JOIN `PRICING.price_lists` pl_last
  ON cp.last_seen_price_list_seq = pl_last.price_list_seq
WHERE cp.status = 'ACTIVE';


-- ============================================================================
-- View: price_history_with_metadata
-- All prices (including historical) joined with price list metadata.
-- Useful for auditing and tracing price changes over time.
-- ============================================================================
CREATE OR REPLACE VIEW `PRICING.price_history_with_metadata` AS
SELECT
  -- Price data
  cp.item_id,
  cp.supplier,
  cp.brand,
  cp.part_number,
  cp.currency,
  cp.original_price,
  cp.effective_price,
  cp.status,
  cp.description,
  cp.location,
  
  -- SCD validity
  cp.valid_from,
  cp.valid_until,
  cp.last_seen_date,
  
  -- Supersession data
  cp.former_part_number,
  cp.supersession,
  cp.terminal_part_number,
  cp.supersession_chain_length,
  cp.price_inherited_from,
  cp.is_synthetic,
  cp.reconciliation_status AS item_reconciliation_status,
  cp.reconciliation_error_message,
  
  -- Price list metadata (from last seen)
  cp.last_seen_price_list_seq,
  pl.price_list_id,
  pl.source_filename,
  pl.source_email_subject,
  pl.source_email_date,
  pl.upload_timestamp,
  pl.valid_from_date AS price_list_valid_from,
  pl.valid_to_date AS price_list_valid_to,
  pl.reconciliation_status AS price_list_reconciliation_status,
  pl.drive_file_url

FROM `PRICING.canonical_prices` cp
LEFT JOIN `PRICING.price_lists` pl
  ON cp.last_seen_price_list_seq = pl.price_list_seq;


-- ============================================================================
-- View: supplier_price_summary
-- Summary statistics by supplier and brand from joined tables.
-- Useful for dashboards and monitoring.
-- ============================================================================
CREATE OR REPLACE VIEW `PRICING.supplier_price_summary` AS
SELECT
  cp.supplier,
  cp.brand,
  cp.currency,
  
  -- Counts
  COUNT(DISTINCT cp.item_id) AS total_items,
  COUNT(DISTINCT CASE WHEN cp.status = 'ACTIVE' THEN cp.item_id END) AS active_items,
  COUNT(DISTINCT CASE WHEN cp.status = 'UNAVAILABLE' THEN cp.item_id END) AS unavailable_items,
  COUNT(DISTINCT CASE WHEN cp.status = 'HISTORY' THEN cp.item_id END) AS historical_items,
  COUNT(DISTINCT CASE WHEN cp.is_synthetic THEN cp.item_id END) AS synthetic_items,
  
  -- Price stats (active only)
  MIN(CASE WHEN cp.status = 'ACTIVE' THEN cp.effective_price END) AS min_price,
  MAX(CASE WHEN cp.status = 'ACTIVE' THEN cp.effective_price END) AS max_price,
  AVG(CASE WHEN cp.status = 'ACTIVE' THEN cp.effective_price END) AS avg_price,
  
  -- Latest price list info
  MAX(pl.upload_timestamp) AS latest_upload,
  MAX(pl.source_email_date) AS latest_email_date,
  COUNT(DISTINCT pl.price_list_seq) AS total_price_lists

FROM `PRICING.canonical_prices` cp
LEFT JOIN `PRICING.price_lists` pl
  ON cp.last_seen_price_list_seq = pl.price_list_seq
GROUP BY cp.supplier, cp.brand, cp.currency;



-- ============================================================================
-- View: processing_errors_summary
-- Processing errors with price list context.
-- ============================================================================
CREATE OR REPLACE VIEW `PRICING.processing_errors_summary` AS
SELECT 
  price_lists.supplier,
  price_lists.brand, 
  price_lists.currency,
  price_lists.location, 
  price_lists.source_filename,
  price_lists.source_email_subject,
  price_lists.source_email_date,
  price_lists.upload_timestamp,
  processing_errors.part_number,
  processing_errors.error_type,
  processing_errors.error_message,
  processing_errors.error_details,
  processing_errors.is_resolved,
  processing_errors.resolved_timestamp,
  processing_errors.resolved_by
FROM PRICING.processing_errors 
JOIN PRICING.price_lists 
  ON price_lists.price_list_id = processing_errors.price_list_id;


-- ============================================================================
-- View: prices_current_with_metadata
-- Current prices (from optimized prices_current table) with price list metadata.
-- Use this for queries that need metadata; use prices_current directly for
-- high-frequency lookups where metadata isn't needed.
-- ============================================================================
CREATE OR REPLACE VIEW `PRICING.prices_current_with_metadata` AS
SELECT
  -- Price data
  pc.supplier,
  pc.brand,
  pc.part_number,
  pc.currency,
  pc.location,
  pc.original_price,
  pc.effective_price,
  pc.status,
  pc.description,
  
  -- Tracking
  pc.valid_from,
  pc.last_seen_date,
  
  -- Supersession data
  pc.supersession,
  pc.terminal_part_number,
  
  -- Reference
  pc.item_id,
  
  -- Price list metadata (from last seen)
  pc.last_seen_price_list_seq,
  pl.price_list_id AS last_seen_price_list_id,
  pl.source_filename,
  pl.source_email_subject,
  pl.source_email_date,
  pl.upload_timestamp,
  pl.valid_from_date AS price_list_valid_from,
  pl.valid_to_date AS price_list_valid_to,
  pl.drive_file_url

FROM `PRICING.prices_current` pc
LEFT JOIN `PRICING.price_lists` pl
  ON pc.last_seen_price_list_seq = pl.price_list_seq;
