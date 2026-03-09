-- BigQuery Stored Procedure for Supersession Reconciliation
-- 
-- This procedure performs two phases:
--   Phase 1: De-duplication (handle duplicate part numbers)
--   Phase 2: Supersession reconciliation (follow chains, create synthetic rows)
--
-- IMPORTANT: This procedure operates on a STAGING TABLE, not the main price_list_items table.
-- The staging table is created by the Python code before calling this procedure.

CREATE OR REPLACE PROCEDURE `pricing-email-bot.PRICING.reconcile_supersessions_staging`(
  IN p_staging_table_id STRING,
  IN p_price_list_id STRING,
  IN p_max_chain_depth INT64
)
BEGIN
  DECLARE v_supplier STRING;
  DECLARE v_brand STRING;
  DECLARE v_currency STRING;
  DECLARE v_total_items INT64;
  DECLARE v_reconciled_items INT64 DEFAULT 0;
  DECLARE v_items_with_errors INT64 DEFAULT 0;
  DECLARE v_synthetic_items INT64 DEFAULT 0;
  DECLARE v_duplicates_found INT64 DEFAULT 0;
  DECLARE v_duplicates_removed INT64 DEFAULT 0;

  -- Get price list metadata
  SET (v_supplier, v_brand, v_currency) = (
    SELECT AS STRUCT supplier, brand, currency
    FROM `pricing-email-bot.PRICING.price_lists`
    WHERE price_list_id = p_price_list_id
  );

  -- ═══════════════════════════════════════════════════════════════════════════
  -- PHASE 1: DE-DUPLICATION
  -- Part numbers are already normalized by Python before upload
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Step D1: Identify duplicates and select winner for each group
  -- Winner criteria: highest price, then alphabetically lowest supersession
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE dedup_analysis AS
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
      FROM `%s`
    )
    SELECT * FROM ranked
  """, p_staging_table_id);

  -- Step D2: Log errors for duplicates with mismatched prices or supersessions
  INSERT INTO `pricing-email-bot.PRICING.processing_errors` (
    error_id, price_list_id, supplier, brand, part_number,
    error_type, error_message, error_details, created_timestamp
  )
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    v_supplier,
    v_brand,
    da.part_number,
    'DUPLICATE_PART_NUMBER',
    CONCAT('Part ', da.part_number, ' appears ', da.group_size, ' times with ',
           CASE WHEN da.distinct_prices > 1 THEN CONCAT(CAST(da.distinct_prices AS STRING), ' different prices') ELSE 'same price' END,
           CASE WHEN da.distinct_supersessions > 1 THEN CONCAT(' and ', CAST(da.distinct_supersessions AS STRING), ' different supersessions') ELSE '' END,
           '. Using price: ', CAST((SELECT MAX(v.price) FROM UNNEST(da.all_variants) v) AS STRING),
           CASE WHEN da.resolved_supersession IS NOT NULL THEN CONCAT(', supersession: ', da.resolved_supersession) ELSE '' END),
    TO_JSON(STRUCT(
      da.all_variants AS variants,
      (SELECT MAX(v.price) FROM UNNEST(da.all_variants) v) AS resolved_price,
      da.resolved_supersession AS resolved_supersession
    )),
    CURRENT_TIMESTAMP()
  FROM dedup_analysis da
  WHERE da.group_size > 1
    AND da.rank_in_group = 1  -- Only log once per duplicate group
    AND (da.distinct_prices > 1 OR da.distinct_supersessions > 1);  -- Only if there's a conflict

  -- Step D3: Delete duplicate rows (keep only winner)
  EXECUTE IMMEDIATE FORMAT("""
    DELETE FROM `%s`
    WHERE item_id IN (
      SELECT item_id FROM dedup_analysis WHERE rank_in_group > 1
    )
  """, p_staging_table_id);

  -- Step D4: Update winner rows with resolved supersession (if different)
  EXECUTE IMMEDIATE FORMAT("""
    UPDATE `%s` AS items
    SET supersession = da.resolved_supersession
    FROM dedup_analysis da
    WHERE items.item_id = da.item_id
      AND da.rank_in_group = 1
      AND da.group_size > 1
      AND (items.supersession IS DISTINCT FROM da.resolved_supersession)
  """, p_staging_table_id);

  -- Step D5: Record dedup stats
  SET v_duplicates_found = (SELECT COUNT(DISTINCT part_number) FROM dedup_analysis WHERE group_size > 1);
  SET v_duplicates_removed = (SELECT COUNT(*) FROM dedup_analysis WHERE rank_in_group > 1);

  DROP TABLE IF EXISTS dedup_analysis;

  -- ═══════════════════════════════════════════════════════════════════════════
  -- PHASE 2: SUPERSESSION RECONCILIATION
  -- ═══════════════════════════════════════════════════════════════════════════

  -- Step 1: Build price index for this price list (now de-duplicated)
  EXECUTE IMMEDIATE FORMAT("""
    CREATE OR REPLACE TEMP TABLE price_index AS
    SELECT 
      item_id,
      part_number,
      supersession,
      original_price
    FROM `%s`
  """, p_staging_table_id);

  -- Step 2: Follow supersession chains using recursive CTE
  -- NOTE: Recursion continues until we reach a part with NO supersession (the terminal).
  -- The chain naturally stops when next_part becomes NULL (no further supersession).
  CREATE OR REPLACE TEMP TABLE supersession_chains AS
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
  INSERT INTO `pricing-email-bot.PRICING.processing_errors` 
    (error_id, price_list_id, item_id, supplier, brand, part_number, error_type, error_message, error_details, created_timestamp)
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    sc.item_id,
    v_supplier,
    v_brand,
    sc.origin_part,
    'CIRCULAR_SUPERSESSION',
    CONCAT('Circular chain: ', ARRAY_TO_STRING(sc.chain_path, ' → ')),
    TO_JSON(STRUCT(sc.chain_path AS path, sc.chain_length AS depth)),
    CURRENT_TIMESTAMP()
  FROM supersession_chains sc
  WHERE sc.is_circular = TRUE;

  -- Step 4: Update items with reconciled prices
  -- Parts WITH supersession (may have circular refs)
  EXECUTE IMMEDIATE FORMAT("""
    UPDATE `%s` AS items
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
  """, p_staging_table_id);

  -- Parts WITHOUT supersession (no reconciliation needed)
  EXECUTE IMMEDIATE FORMAT("""
    UPDATE `%s`
    SET 
      effective_price = original_price,
      reconciliation_status = 'NO_SUPERSESSION',
      reconciled_timestamp = CURRENT_TIMESTAMP()
    WHERE (supersession IS NULL OR supersession = '')
      AND reconciliation_status = 'PENDING'
  """, p_staging_table_id);

  -- Step 5: Add synthetic rows for supersessions not in original file
  -- Handle case where multiple parts supersede to same non-existent part:
  -- - Create ONE synthetic row per terminal_part
  -- - Use the HIGHEST price among all parents
  -- - Log warning if multiple parents have different prices
  
  -- 5a: Identify synthetic rows needed with aggregated data
  CREATE OR REPLACE TEMP TABLE synthetic_candidates AS
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
  INSERT INTO `pricing-email-bot.PRICING.processing_errors` (
    error_id, price_list_id, supplier, brand, part_number,
    error_type, error_message, error_details, created_timestamp
  )
  SELECT 
    GENERATE_UUID(),
    p_price_list_id,
    v_supplier,
    v_brand,
    sc.terminal_part,
    'AMBIGUOUS_SYNTHETIC_PRICE',
    CONCAT('Synthetic part ', sc.terminal_part, ' has ', CAST(sc.parent_count AS STRING), 
           ' parents with prices ranging from ', CAST(sc.min_price AS STRING), ' to ', CAST(sc.max_price AS STRING),
           '. Using highest price: ', CAST(sc.max_price AS STRING)),
    TO_JSON(STRUCT(
      sc.parents AS parent_parts,
      sc.max_price AS selected_price,
      sc.highest_price_parent AS price_source
    )),
    CURRENT_TIMESTAMP()
  FROM synthetic_candidates sc
  WHERE sc.max_price != sc.min_price;  -- Only log if prices differ
  
  -- 5c: Insert ONE synthetic row per terminal_part with highest price
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO `%s` (
      item_id, part_number, currency, original_price, effective_price,
      former_part_number, supersession, terminal_part_number, supersession_chain_length,
      price_inherited_from, description, is_synthetic, reconciliation_status, reconciled_timestamp
    )
    SELECT 
      GENERATE_UUID(),
      sc.terminal_part,                    -- The supersession becomes the part number
      @currency,
      sc.max_price,                        -- Use highest price among parents
      sc.max_price,
      sc.highest_price_parent,             -- Attribute to the highest-price parent
      NULL,                                -- No further supersession
      sc.terminal_part,
      0,
      sc.highest_price_parent,             -- Price inherited from highest-price parent
      CASE 
        WHEN sc.parent_count > 1 THEN CONCAT('Supersession of ', CAST(sc.parent_count AS STRING), ' parts (using highest price from ', sc.highest_price_parent, ')')
        ELSE CONCAT('Supersession of ', sc.highest_price_parent)
      END,
      TRUE,                                -- Mark as synthetic
      'OK',
      CURRENT_TIMESTAMP()
    FROM synthetic_candidates sc
  """, p_staging_table_id)
  USING v_currency AS currency;
  
  -- 5d: Update all parts that supersede to synthetic parts to use the synthetic's price
  EXECUTE IMMEDIATE FORMAT("""
    UPDATE `%s` AS items
    SET effective_price = sc.max_price
    FROM synthetic_candidates sc
    WHERE items.terminal_part_number = sc.terminal_part
      AND items.is_synthetic = FALSE
  """, p_staging_table_id);
  
  DROP TABLE IF EXISTS synthetic_candidates;

  -- Step 6: Update price_list with stats
  EXECUTE IMMEDIATE FORMAT("""
    SELECT 
      COUNT(*) AS total,
      COUNTIF(reconciliation_status IN ('OK', 'NO_SUPERSESSION')) AS reconciled,
      COUNTIF(reconciliation_status IN ('CIRCULAR_REF', 'CHAIN_TOO_LONG', 'ERROR')) AS errors,
      COUNTIF(is_synthetic = TRUE) AS synthetic
    FROM `%s`
  """, p_staging_table_id)
  INTO v_total_items, v_reconciled_items, v_items_with_errors, v_synthetic_items;

  UPDATE `pricing-email-bot.PRICING.price_lists`
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
  
END;


-- ============================================================================
-- Procedure: merge_pending_to_canonical
-- SCD Type 2 merge from staging tables into canonical_prices.
-- Also maintains prices_current table with only ACTIVE/UNAVAILABLE records.
-- Run by scheduled job (e.g., nightly).
--
-- SUPPORTS OUT-OF-SEQUENCE HISTORICAL DATA:
-- - Properly handles loading older price lists after newer ones
-- - Inserts historical records between existing SCD Type 2 rows
-- - Adjusts valid_until dates to maintain temporal integrity
--
-- PRICES_CURRENT MAINTENANCE:
-- - Deletes from prices_current when closing rows (ACTIVE → HISTORY)
-- - Inserts to prices_current for new ACTIVE/UNAVAILABLE rows
-- - Updates prices_current for last_seen updates
-- ============================================================================
CREATE OR REPLACE PROCEDURE `pricing-email-bot.PRICING.merge_pending_to_canonical`()
BEGIN
  DECLARE v_staging_tables_processed INT64 DEFAULT 0;
  DECLARE v_items_inserted INT64 DEFAULT 0;
  DECLARE v_items_updated INT64 DEFAULT 0;
  DECLARE v_items_closed INT64 DEFAULT 0;
  DECLARE v_items_historical INT64 DEFAULT 0;
  DECLARE v_items_skipped_duplicate INT64 DEFAULT 0;
  DECLARE v_union_query STRING DEFAULT '';

  -- ═══════════════════════════════════════════════════════════════════════════
  -- STEP 1: Build consolidated view of all pending staging tables
  -- Uses dynamic UNION ALL to avoid intermediate table storage
  -- ═══════════════════════════════════════════════════════════════════════════
  
  -- Get list of pending price lists with staging tables
  CREATE OR REPLACE TEMP TABLE pending_price_lists AS
  SELECT 
    price_list_seq,
    price_list_id,
    staging_table_id,
    supplier,
    brand,
    currency,
    location,
    valid_from_date
  FROM `pricing-email-bot.PRICING.price_lists`
  WHERE merge_status = 'PENDING'
    AND staging_table_id IS NOT NULL
    AND reconciliation_status = 'COMPLETED';

  -- Build dynamic UNION ALL query from all staging tables
  FOR staging_record IN (SELECT * FROM pending_price_lists)
  DO
    SET v_union_query = CONCAT(
      v_union_query,
      IF(v_union_query = '', '', ' UNION ALL '),
      FORMAT('''
        SELECT 
          %d AS price_list_seq,
          '%s' AS price_list_id,
          '%s' AS staging_table_id,
          item_id,
          part_number,
          currency,
          original_price,
          effective_price,
          former_part_number,
          supersession,
          terminal_part_number,
          supersession_chain_length,
          price_inherited_from,
          description,
          COALESCE(is_synthetic, FALSE) AS is_synthetic,
          reconciliation_status,
          reconciliation_error_message,
          '%s' AS supplier,
          '%s' AS brand,
          %s AS location,
          DATE '%s' AS valid_from_date,
          CURRENT_TIMESTAMP() AS import_timestamp
        FROM `%s`
      ''',
      staging_record.price_list_seq,
      staging_record.price_list_id,
      staging_record.staging_table_id,
      staging_record.supplier,
      staging_record.brand,
      IF(staging_record.location IS NULL, 'NULL', CONCAT("'", staging_record.location, "'")),
      CAST(staging_record.valid_from_date AS STRING),
      staging_record.staging_table_id)
    );
    SET v_staging_tables_processed = v_staging_tables_processed + 1;
  END FOR;

  -- If no staging tables to process, exit early
  IF v_union_query = '' THEN
    SELECT 
      0 AS staging_tables_processed,
      0 AS items_inserted,
      0 AS items_updated,
      0 AS items_closed,
      0 AS items_historical,
      0 AS items_skipped_duplicate;
    RETURN;
  END IF;

  -- Create consolidated temp table from all staging tables
  EXECUTE IMMEDIATE FORMAT('CREATE OR REPLACE TEMP TABLE staging_consolidated AS %s', v_union_query);

  -- ═══════════════════════════════════════════════════════════════════════════
  -- STEP 2: SCD Type 2 Merge into canonical_prices
  -- Now with OUT-OF-SEQUENCE support for historical data loading
  -- ═══════════════════════════════════════════════════════════════════════════

  -- 2a: Identify what action to take for each incoming item
  -- Enhanced to support out-of-sequence loading by comparing valid_from dates
  CREATE OR REPLACE TEMP TABLE merge_actions AS
  WITH incoming AS (
    SELECT 
      sc.*,
      ROW_NUMBER() OVER (
        PARTITION BY sc.supplier, sc.brand, sc.part_number, sc.currency
        ORDER BY sc.valid_from_date DESC, sc.import_timestamp DESC
      ) AS rn
    FROM staging_consolidated sc
  ),
  latest_incoming AS (
    SELECT * FROM incoming WHERE rn = 1
  ),
  -- Get CURRENT (ACTIVE/UNAVAILABLE) canonical records
  current_canonical AS (
    SELECT *
    FROM `pricing-email-bot.PRICING.canonical_prices`
    WHERE status IN ('ACTIVE', 'UNAVAILABLE')
  ),
  -- Get ALL canonical records for the same part (to find gaps for historical inserts)
  all_canonical AS (
    SELECT 
      supplier, brand, part_number, currency,
      valid_from,
      valid_until,
      status,
      item_id
    FROM `pricing-email-bot.PRICING.canonical_prices`
  ),
  -- Check if a record already exists for the exact same valid_from date
  existing_same_date AS (
    SELECT DISTINCT
      supplier, brand, part_number, currency, valid_from
    FROM `pricing-email-bot.PRICING.canonical_prices`
  ),
  -- Find the next record after incoming date (for historical inserts)
  next_record_after_incoming AS (
    SELECT 
      li.supplier,
      li.brand,
      li.part_number,
      li.currency,
      li.valid_from_date AS incoming_valid_from,
      MIN(ac.valid_from) AS next_valid_from
    FROM latest_incoming li
    JOIN all_canonical ac 
      ON li.supplier = ac.supplier
      AND li.brand = ac.brand
      AND li.part_number = ac.part_number
      AND li.currency = ac.currency
      AND ac.valid_from > li.valid_from_date
    GROUP BY li.supplier, li.brand, li.part_number, li.currency, li.valid_from_date
  )
  SELECT 
    li.price_list_seq,
    li.price_list_id,
    li.item_id AS incoming_item_id,
    li.supplier,
    li.brand,
    li.part_number,
    li.currency,
    li.original_price AS incoming_original_price,
    li.effective_price AS incoming_effective_price,
    li.former_part_number AS incoming_former_pn,
    li.supersession AS incoming_supersession,
    li.terminal_part_number AS incoming_terminal_pn,
    li.supersession_chain_length AS incoming_chain_length,
    li.price_inherited_from AS incoming_price_inherited_from,
    li.description AS incoming_description,
    li.location AS incoming_location,
    li.is_synthetic AS incoming_is_synthetic,
    li.reconciliation_status AS incoming_recon_status,
    li.reconciliation_error_message AS incoming_recon_error,
    li.valid_from_date AS incoming_valid_from,
    cc.item_id AS existing_item_id,
    cc.original_price AS existing_original_price,
    cc.effective_price AS existing_effective_price,
    cc.supersession AS existing_supersession,
    cc.description AS existing_description,
    cc.status AS existing_status,
    cc.valid_from AS existing_valid_from,
    -- Next record date (for calculating valid_until on historical inserts)
    nrai.next_valid_from,
    -- Pre-generate UUID for new inserts (used by both canonical_prices and prices_current)
    GENERATE_UUID() AS new_item_id,
    -- Determine action with temporal awareness
    CASE
      -- DUPLICATE: Exact same date already exists for this part
      WHEN esd.valid_from IS NOT NULL THEN 'SKIP_DUPLICATE'
      
      -- NEW PART: No existing record at all
      WHEN cc.item_id IS NULL THEN 
        CASE 
          WHEN li.effective_price IS NULL OR li.effective_price = 0 THEN 'INSERT_UNAVAILABLE'
          ELSE 'INSERT_ACTIVE'
        END
      
      -- HISTORICAL: Incoming date is OLDER than current record
      -- Insert as HISTORY with valid_until = day before next record's valid_from
      WHEN li.valid_from_date < cc.valid_from THEN
        CASE
          WHEN li.effective_price IS NULL OR li.effective_price = 0 THEN 'INSERT_HISTORICAL_UNAVAILABLE'
          ELSE 'INSERT_HISTORICAL'
        END
      
      -- NEWER DATA: Incoming date is newer - use existing logic
      -- Price = 0 means unavailable
      WHEN li.effective_price IS NULL OR li.effective_price = 0 THEN
        CASE
          WHEN cc.status = 'UNAVAILABLE' THEN 'UPDATE_LAST_SEEN'
          ELSE 'CLOSE_AND_INSERT_UNAVAILABLE'
        END
      -- Price changed (original or effective)
      WHEN li.original_price IS DISTINCT FROM cc.original_price 
           OR li.effective_price IS DISTINCT FROM cc.effective_price THEN
        'CLOSE_AND_INSERT_ACTIVE'
      -- Supersession changed to a NEW non-null value
      WHEN li.supersession IS NOT NULL 
           AND li.supersession != ''
           AND (cc.supersession IS NULL OR li.supersession != cc.supersession) THEN
        'CLOSE_AND_INSERT_ACTIVE'
      -- No change - just update tracking
      ELSE 'UPDATE_LAST_SEEN'
    END AS action,
    -- Preserve description if incoming is null
    COALESCE(li.description, cc.description) AS resolved_description,
    -- Preserve supersession if incoming is null/empty
    CASE 
      WHEN li.supersession IS NOT NULL AND li.supersession != '' THEN li.supersession
      ELSE cc.supersession
    END AS resolved_supersession
  FROM latest_incoming li
  LEFT JOIN current_canonical cc 
    ON li.supplier = cc.supplier
    AND li.brand = cc.brand
    AND li.part_number = cc.part_number
    AND li.currency = cc.currency
  LEFT JOIN existing_same_date esd
    ON li.supplier = esd.supplier
    AND li.brand = esd.brand
    AND li.part_number = esd.part_number
    AND li.currency = esd.currency
    AND li.valid_from_date = esd.valid_from
  LEFT JOIN next_record_after_incoming nrai
    ON li.supplier = nrai.supplier
    AND li.brand = nrai.brand
    AND li.part_number = nrai.part_number
    AND li.currency = nrai.currency
    AND li.valid_from_date = nrai.incoming_valid_from;

  -- 2b: Close rows that are being superseded (HISTORY) - only for NEWER data
  UPDATE `pricing-email-bot.PRICING.canonical_prices` cp
  SET 
    status = 'HISTORY',
    valid_until = DATE_SUB(ma.incoming_valid_from, INTERVAL 1 DAY)
  FROM merge_actions ma
  WHERE cp.item_id = ma.existing_item_id
    AND ma.action IN ('CLOSE_AND_INSERT_ACTIVE', 'CLOSE_AND_INSERT_UNAVAILABLE');

  SET v_items_closed = @@row_count;

  -- 2c: Update last_seen for unchanged items (only when incoming is newer or same date)
  UPDATE `pricing-email-bot.PRICING.canonical_prices` cp
  SET 
    last_seen_date = GREATEST(cp.last_seen_date, ma.incoming_valid_from),
    last_seen_price_list_seq = CASE 
      WHEN ma.incoming_valid_from >= cp.last_seen_date THEN ma.price_list_seq 
      ELSE cp.last_seen_price_list_seq 
    END,
    -- Update description only if we have a new non-null value
    description = COALESCE(ma.incoming_description, cp.description),
    -- Update location only if we have a new non-null value
    location = COALESCE(ma.incoming_location, cp.location)
  FROM merge_actions ma
  WHERE cp.item_id = ma.existing_item_id
    AND ma.action = 'UPDATE_LAST_SEEN';

  SET v_items_updated = @@row_count;

  -- 2c-2: Update prices_current for unchanged items
  UPDATE `pricing-email-bot.PRICING.prices_current` pc
  SET 
    last_seen_date = GREATEST(pc.last_seen_date, ma.incoming_valid_from),
    last_seen_price_list_seq = CASE 
      WHEN ma.incoming_valid_from >= pc.last_seen_date THEN ma.price_list_seq 
      ELSE pc.last_seen_price_list_seq 
    END,
    description = COALESCE(ma.incoming_description, pc.description)
  FROM merge_actions ma
  WHERE pc.item_id = ma.existing_item_id
    AND ma.action = 'UPDATE_LAST_SEEN';

  -- 2d: Delete from prices_current for rows being closed
  DELETE FROM `pricing-email-bot.PRICING.prices_current`
  WHERE item_id IN (
    SELECT existing_item_id FROM merge_actions 
    WHERE action IN ('CLOSE_AND_INSERT_ACTIVE', 'CLOSE_AND_INSERT_UNAVAILABLE')
  );

  -- 2e: Insert new ACTIVE rows (for NEW parts or when closing current)
  INSERT INTO `pricing-email-bot.PRICING.canonical_prices`
  (item_id, supplier, brand, part_number, currency,
   original_price, effective_price, valid_from, valid_until, status,
   last_seen_date, first_seen_price_list_seq, last_seen_price_list_seq,
   former_part_number, supersession, terminal_part_number,
   supersession_chain_length, price_inherited_from,
   description, location, is_synthetic,
   reconciliation_status, reconciliation_error_message)
  SELECT
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    ma.incoming_original_price,
    ma.incoming_effective_price,
    ma.incoming_valid_from,
    NULL,  -- valid_until = NULL means current
    'ACTIVE',
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.price_list_seq,
    ma.incoming_former_pn,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.incoming_chain_length,
    ma.incoming_price_inherited_from,
    ma.resolved_description,
    ma.incoming_location,
    ma.incoming_is_synthetic,
    ma.incoming_recon_status,
    ma.incoming_recon_error
  FROM merge_actions ma
  WHERE ma.action IN ('INSERT_ACTIVE', 'CLOSE_AND_INSERT_ACTIVE');

  SET v_items_inserted = v_items_inserted + @@row_count;

  -- 2e-2: Insert new ACTIVE rows into prices_current
  INSERT INTO `pricing-email-bot.PRICING.prices_current`
  (supplier, brand, part_number, currency, location,
   original_price, effective_price, status,
   valid_from, last_seen_date, last_seen_price_list_seq,
   supersession, terminal_part_number, item_id, description)
  SELECT
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    ma.incoming_location,
    ma.incoming_original_price,
    ma.incoming_effective_price,
    'ACTIVE',
    ma.incoming_valid_from,
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.resolved_description
  FROM merge_actions ma
  WHERE ma.action IN ('INSERT_ACTIVE', 'CLOSE_AND_INSERT_ACTIVE');

  -- 2f: Insert new UNAVAILABLE rows
  INSERT INTO `pricing-email-bot.PRICING.canonical_prices`
  (item_id, supplier, brand, part_number, currency,
   original_price, effective_price, valid_from, valid_until, status,
   last_seen_date, first_seen_price_list_seq, last_seen_price_list_seq,
   former_part_number, supersession, terminal_part_number,
   supersession_chain_length, price_inherited_from,
   description, location, is_synthetic,
   reconciliation_status, reconciliation_error_message)
  SELECT
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    NULL,  -- original_price = NULL for unavailable
    NULL,  -- effective_price = NULL for unavailable
    ma.incoming_valid_from,
    NULL,  -- valid_until = NULL means current
    'UNAVAILABLE',
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.price_list_seq,
    ma.incoming_former_pn,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.incoming_chain_length,
    ma.incoming_price_inherited_from,
    ma.resolved_description,
    ma.incoming_location,
    ma.incoming_is_synthetic,
    ma.incoming_recon_status,
    ma.incoming_recon_error
  FROM merge_actions ma
  WHERE ma.action IN ('INSERT_UNAVAILABLE', 'CLOSE_AND_INSERT_UNAVAILABLE');

  SET v_items_inserted = v_items_inserted + @@row_count;

  -- 2f-2: Insert new UNAVAILABLE rows into prices_current
  INSERT INTO `pricing-email-bot.PRICING.prices_current`
  (supplier, brand, part_number, currency, location,
   original_price, effective_price, status,
   valid_from, last_seen_date, last_seen_price_list_seq,
   supersession, terminal_part_number, item_id, description)
  SELECT
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    ma.incoming_location,
    NULL,  -- original_price = NULL for unavailable
    NULL,  -- effective_price = NULL for unavailable
    'UNAVAILABLE',
    ma.incoming_valid_from,
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.resolved_description
  FROM merge_actions ma
  WHERE ma.action IN ('INSERT_UNAVAILABLE', 'CLOSE_AND_INSERT_UNAVAILABLE');

  -- ═══════════════════════════════════════════════════════════════════════════
  -- STEP 2g: Insert HISTORICAL rows (out-of-sequence data)
  -- These are older records being loaded after newer ones already exist
  -- Note: Historical rows do NOT go into prices_current (only current prices there)
  -- ═══════════════════════════════════════════════════════════════════════════
  
  INSERT INTO `pricing-email-bot.PRICING.canonical_prices`
  (item_id, supplier, brand, part_number, currency,
   original_price, effective_price, valid_from, valid_until, status,
   last_seen_date, first_seen_price_list_seq, last_seen_price_list_seq,
   former_part_number, supersession, terminal_part_number,
   supersession_chain_length, price_inherited_from,
   description, location, is_synthetic,
   reconciliation_status, reconciliation_error_message)
  SELECT
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    ma.incoming_original_price,
    ma.incoming_effective_price,
    ma.incoming_valid_from,
    -- Set valid_until to day before the next record's valid_from
    -- If next_valid_from is NULL, use existing_valid_from (the current record's valid_from)
    DATE_SUB(COALESCE(ma.next_valid_from, ma.existing_valid_from), INTERVAL 1 DAY),
    'HISTORY',  -- Historical records are immediately HISTORY status
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.price_list_seq,
    ma.incoming_former_pn,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.incoming_chain_length,
    ma.incoming_price_inherited_from,
    ma.resolved_description,
    ma.incoming_location,
    ma.incoming_is_synthetic,
    ma.incoming_recon_status,
    ma.incoming_recon_error
  FROM merge_actions ma
  WHERE ma.action = 'INSERT_HISTORICAL';

  SET v_items_historical = @@row_count;

  -- 2h: Insert HISTORICAL UNAVAILABLE rows
  INSERT INTO `pricing-email-bot.PRICING.canonical_prices`
  (item_id, supplier, brand, part_number, currency,
   original_price, effective_price, valid_from, valid_until, status,
   last_seen_date, first_seen_price_list_seq, last_seen_price_list_seq,
   former_part_number, supersession, terminal_part_number,
   supersession_chain_length, price_inherited_from,
   description, location, is_synthetic,
   reconciliation_status, reconciliation_error_message)
  SELECT
    ma.new_item_id,  -- Use pre-generated UUID from merge_actions
    ma.supplier,
    ma.brand,
    ma.part_number,
    ma.currency,
    NULL,
    NULL,
    ma.incoming_valid_from,
    DATE_SUB(COALESCE(ma.next_valid_from, ma.existing_valid_from), INTERVAL 1 DAY),
    'HISTORY',
    ma.incoming_valid_from,
    ma.price_list_seq,
    ma.price_list_seq,
    ma.incoming_former_pn,
    ma.resolved_supersession,
    ma.incoming_terminal_pn,
    ma.incoming_chain_length,
    ma.incoming_price_inherited_from,
    ma.resolved_description,
    ma.incoming_location,
    ma.incoming_is_synthetic,
    ma.incoming_recon_status,
    ma.incoming_recon_error
  FROM merge_actions ma
  WHERE ma.action = 'INSERT_HISTORICAL_UNAVAILABLE';

  SET v_items_historical = v_items_historical + @@row_count;

  -- Count skipped duplicates for logging
  SET v_items_skipped_duplicate = (
    SELECT COUNT(*) FROM merge_actions WHERE action = 'SKIP_DUPLICATE'
  );

  -- ═══════════════════════════════════════════════════════════════════════════
  -- STEP 3: Update price_lists status and drop staging tables
  -- ═══════════════════════════════════════════════════════════════════════════

  FOR staging_record IN (SELECT * FROM pending_price_lists)
  DO
    BEGIN
      -- Drop the staging table
      EXECUTE IMMEDIATE FORMAT("DROP TABLE IF EXISTS `%s`", staging_record.staging_table_id);
      
      -- Mark price_list as merged
      UPDATE `pricing-email-bot.PRICING.price_lists`
      SET merge_status = 'MERGED',
          staging_table_id = NULL
      WHERE price_list_id = staging_record.price_list_id;
    EXCEPTION WHEN ERROR THEN
      -- Mark as failed if we couldn't drop the table
      UPDATE `pricing-email-bot.PRICING.price_lists`
      SET merge_status = 'FAILED'
      WHERE price_list_id = staging_record.price_list_id;
    END;
  END FOR;

  -- Cleanup temp tables
  DROP TABLE IF EXISTS pending_price_lists;
  DROP TABLE IF EXISTS merge_actions;
  DROP TABLE IF EXISTS staging_consolidated;

  -- Log summary (enhanced with historical stats)
  SELECT 
    v_staging_tables_processed AS staging_tables_processed,
    v_items_inserted AS items_inserted,
    v_items_updated AS items_updated,
    v_items_closed AS items_closed,
    v_items_historical AS items_historical,
    v_items_skipped_duplicate AS items_skipped_duplicate;

END;
