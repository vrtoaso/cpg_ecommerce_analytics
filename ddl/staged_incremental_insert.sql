/*
================================================================================
STAGED INCREMENTAL ETL PATTERN (SIL Pattern) - BigQuery Implementation
================================================================================

Hi Javi,

This is the ETL design pattern I mentioned that may help with your Shopify loyalty program workloads, or any other ETL workloads. I've used this pattern successfully across many DW deployments in BG and Azure. There are also benefits for detecting/handling bad data from source systems. Much more to discuss, but I wanted you to test it first to see if there's any value for your current POC work.

WHAT IT SOLVES:
- Incremental data loading (only processes changed/new records)
- Full lineage tracking and audit trails
- Handles source system updates intelligently
- Avoids unnecessary data processing
- Works at any scale (testing to production)

HOW IT WORKS:
1. Extract: Pulls only new/changed records since last run using cutoff timestamps
2. Stage: Transforms data in a staging table
3. Merge: Upserts to final table (insert new, update changed)
4. Track: Maintains lineage and updates cutoff times automatically

EXECUTION:
Run 3 stored procedures sequentially:
1. get_lineage_key() - Start lineage tracking
2. get_orders_detail_updates() - Extract & stage data - note it can be get_anything(transactional__updates, not just sales. 
3. migrate_staged_orders_detail_updates() - Merge & complete

TESTING:
- Sample data spans 7 days for incremental testing
- Modify raw data, re-run procedures, observe changes
- Reset: truncate tables + set cutoff_time to NULL
- Watch how it skips unchanged data on subsequent runs

DEPLOYMENT:
- Manual execution, schedulers, Airflow Python scripts, or adapt for dbt
- Portable across cloud platforms (BigQuery, Azure, etc.)
- The second procedure is where your complex source queries go (joins, transformations, business logic)

Questions? Let's talk after you test it.

================================================================================
*/

-- ============================================================================
-- STEP 1: CREATE TABLES
-- ============================================================================

-- Raw table (simulates data from Hevo/Fivetran), This table stores a result set from a source query referencing raw table joins and transformations (date formatting and other CASTing)
CREATE OR REPLACE TABLE `Demo.raw_shopify_order_items` (
  id STRING,
  order_id STRING,
  order_number STRING,
  order_date DATETIME,
  update_date DATETIME,
  customer_id STRING,
  financial_status STRING,
  fulfillment_status STRING,
  discount_codes STRING,
  channel STRING,
  partner_name STRING,
  product_key STRING,
  product_name STRING,
  cost NUMERIC,
  price NUMERIC,
  total_discount NUMERIC
)
OPTIONS(
  description = "Raw Shopify Order Items data"
);

-- Staging table (temporary processing area)
CREATE OR REPLACE TABLE `Demo.stg_orders_detail` (
  id STRING,
  order_id STRING,
  order_number STRING,
  order_date DATETIME,
  update_date DATETIME,
  customer_id STRING,
  financial_status STRING,
  fulfillment_status STRING,
  discount_codes STRING,
  channel STRING,
  partner_name STRING,
  product_key STRING,
  product_name STRING,
  cost NUMERIC,
  price NUMERIC,
  total_discount NUMERIC
)
OPTIONS(
  description = "Staging table for order detail records"
);

-- Fact (or OBT) table (final destination with ETL metadata)
CREATE OR REPLACE TABLE `Demo.fact_order_detail` (
  detail_id STRING,
  order_id STRING,
  order_number STRING,
  order_date DATETIME,
  update_date DATETIME,
  customer_id STRING,
  financial_status STRING,
  fulfillment_status STRING,
  discount_code STRING,
  marketing_channel STRING,
  partner STRING,
  product_key STRING,
  product_name STRING,
  cost NUMERIC,
  price NUMERIC,
  total_discount NUMERIC,
  dt_insert DATETIME,  -- When record was first inserted
  dt_update DATETIME   -- When record was last updated
)
OPTIONS(
  description = "Fact table for transformed order detail records"
);

-- Lineage tracking table; you can alwasys add more to this, for example # of records loaded, and other metadata
CREATE OR REPLACE TABLE `Demo.int_lineage` (
  lineage_key STRING NOT NULL 
    OPTIONS(description = "Unique identifier for each ETL run"),
  data_load_started DATETIME 
    OPTIONS(description = "When ETL process started"),
  src_table_name STRING 
    OPTIONS(description = "Source table name"),
  tgt_table_name STRING 
    OPTIONS(description = "Target table name"),
  data_load_completed DATETIME 
    OPTIONS(description = "When ETL process completed"),
  was_successful BOOLEAN 
    OPTIONS(description = "Success/failure flag"),
  source_sys_cutoff_time DATETIME 
    OPTIONS(description = "Data cutoff time for this run")
)
OPTIONS(
  description = "ETL lineage and audit trail"
);

-- Cutoff time tracking table, one record for each source/target pair. cutoff_time field will be the only fields that gets updated  
CREATE OR REPLACE TABLE `Demo.int_etl_cutoff` (
  src_table_name STRING 
    OPTIONS(description = "Source table name"),
  tgt_table_name STRING 
    OPTIONS(description = "Target table name"),
  cutoff_time DATETIME 
    OPTIONS(description = "Last successfully processed timestamp")
)
OPTIONS(
  description = "Incremental ETL cutoff times"
);

-- ============================================================================
-- STEP 2: POPULATE TEST DATA (spread across 7 days)
-- ============================================================================

INSERT INTO `Demo.raw_shopify_order_items`(
  id, order_id, order_number, order_date, update_date, customer_id, 
  financial_status, fulfillment_status, discount_codes, channel, partner_name,
  product_key, product_name, cost, price, total_discount
)
VALUES
  ('10209319026874', '3972847337658', '822716', 
   DATETIME '2025-09-19T13:55:00', DATETIME '2025-09-19T13:55:00',
   '197905842193', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759930', '3972796219578', '822703', 
   DATETIME '2025-09-16T11:36:00', DATETIME '2025-09-16T19:19:00',
   '639616680011', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943674', '3972771938490', '822701', 
   DATETIME '2025-09-15T13:47:00', DATETIME '2025-09-15T13:47:00',
   '5408128041146', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052858', '3972787372218', '822706', 
   DATETIME '2025-09-16T19:19:00', DATETIME '2025-09-16T11:36:00',
   '5408128336058', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536890', '3972937121978', '822735', 
   DATETIME '2025-09-15T09:56:00', DATETIME '2025-09-15T09:56:00',
   '5408235094202', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0),
   
  ('10209319026875', '3972847337659', '822717', 
   DATETIME '2025-09-19T14:00:00', DATETIME '2025-09-19T14:00:00',
   '197905842194', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759931', '3972796219579', '822704', 
   DATETIME '2025-09-16T11:40:00', DATETIME '2025-09-16T19:20:00',
   '639616680012', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943675', '3972771938491', '822702', 
   DATETIME '2025-09-15T13:50:00', DATETIME '2025-09-15T13:50:00',
   '5408128041147', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052859', '3972787372219', '822707', 
   DATETIME '2025-09-16T19:20:00', DATETIME '2025-09-16T11:40:00',
   '5408128336059', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536891', '3972937121979', '822736', 
   DATETIME '2025-09-15T10:00:00', DATETIME '2025-09-15T10:00:00',
   '5408235094203', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0),
   
  ('10209319026876', '3972847337660', '822718', 
   DATETIME '2025-09-19T14:05:00', DATETIME '2025-09-19T14:05:00',
   '197905842195', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759932', '3972796219580', '822705', 
   DATETIME '2025-09-16T11:45:00', DATETIME '2025-09-16T19:25:00',
   '639616680013', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943676', '3972771938492', '822703', 
   DATETIME '2025-09-15T13:55:00', DATETIME '2025-09-15T13:55:00',
   '5408128041148', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052860', '3972787372220', '822708', 
   DATETIME '2025-09-16T19:25:00', DATETIME '2025-09-16T11:45:00',
   '5408128336060', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536892', '3972937121980', '822737', 
   DATETIME '2025-09-15T10:05:00', DATETIME '2025-09-15T10:05:00',
   '5408235094204', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0),
   
  ('10209319026877', '3972847337661', '822719', 
   DATETIME '2025-09-19T14:10:00', DATETIME '2025-09-19T14:10:00',
   '197905842196', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759933', '3972796219581', '822706', 
   DATETIME '2025-09-16T11:50:00', DATETIME '2025-09-16T19:30:00',
   '639616680014', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943677', '3972771938493', '822704', 
   DATETIME '2025-09-15T14:00:00', DATETIME '2025-09-15T14:00:00',
   '5408128041149', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052861', '3972787372221', '822709', 
   DATETIME '2025-09-16T19:30:00', DATETIME '2025-09-16T11:50:00',
   '5408128336061', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536893', '3972937121981', '822738', 
   DATETIME '2025-09-15T10:10:00', DATETIME '2025-09-15T10:10:00',
   '5408235094205', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0),
   
  ('10209319026878', '3972847337662', '822720', 
   DATETIME '2025-09-19T14:15:00', DATETIME '2025-09-19T14:15:00',
   '197905842197', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759934', '3972796219582', '822707', 
   DATETIME '2025-09-16T11:55:00', DATETIME '2025-09-16T19:35:00',
   '639616680015', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943678', '3972771938494', '822705', 
   DATETIME '2025-09-15T14:05:00', DATETIME '2025-09-15T14:05:00',
   '5408128041150', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052862', '3972787372222', '822710', 
   DATETIME '2025-09-16T19:35:00', DATETIME '2025-09-16T11:55:00',
   '5408128336062', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536894', '3972937121982', '822739', 
   DATETIME '2025-09-15T10:15:00', DATETIME '2025-09-15T10:15:00',
   '5408235094206', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0),
   
  ('10209319026879', '3972847337663', '822721', 
   DATETIME '2025-09-19T14:20:00', DATETIME '2025-09-19T14:20:00',
   '197905842198', 'paid', 'fulfilled', 'shrugged', 'Podcast', 'Mike Bledsoe', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 62.95),
   
  ('10209241759935', '3972796219583', '822708', 
   DATETIME '2025-09-16T12:00:00', DATETIME '2025-09-16T19:40:00',
   '639616680016', 'paid', NULL, 'mimifit', 'Collective', 'Doug Larson', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209178943679', '3972771938495', '822706', 
   DATETIME '2025-09-15T14:10:00', DATETIME '2025-09-15T14:10:00',
   '5408128041151', 'paid', 'fulfilled', 'pelz', 'Podcast', 'Anders Varner / Barbell Shrugged', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 31.48),
   
  ('10209217052863', '3972787372223', '822711', 
   DATETIME '2025-09-16T19:40:00', DATETIME '2025-09-16T12:00:00',
   '5408128336063', 'paid', NULL, 'success', 'Collective', 'Meagan Lindquist', 
   '01000130', 'Red Juice Canister', 19.26, 99, 0),
   
  ('10209486536895', '3972937121983', '822740', 
   DATETIME '2025-09-15T10:20:00', DATETIME '2025-09-15T10:20:00',
   '5408235094207', 'paid', 'fulfilled', 'ultimatehealth', 'Podcast', 'The Resetter Podcast', 
   '02000130', 'Green Juice Canister', 21.05, 69.95, 0);

-- ============================================================================
-- STEP 3: CREATE STORED PROCEDURES
-- ============================================================================

-- PROCEDURE 1: Initialize lineage tracking for each ETL run
CREATE OR REPLACE PROCEDURE `Demo.get_lineage_key`(
  src_table_name STRING, 
  tgt_table_name STRING, 
  new_cutoff_time DATETIME
)
BEGIN
  DECLARE DataLoadStartedWhen DATETIME;
  DECLARE LineageKey STRING;
  
  SET DataLoadStartedWhen = CURRENT_DATETIME('America/Los_Angeles');
  SET LineageKey = GENERATE_UUID();
  
  INSERT INTO `Demo.int_lineage`(
    lineage_key, data_load_started, src_table_name, tgt_table_name, 
    data_load_completed, was_successful, source_sys_cutoff_time
  )
  VALUES(
    LineageKey, DataLoadStartedWhen, src_table_name, tgt_table_name, 
    NULL, false, new_cutoff_time
  );
END;

-- PROCEDURE 2: Extract and stage incremental data
-- THIS IS THE CORE ETL LOGIC - Replace SELECT with your complex source query. 
-- I've successfully nested and ran queries with many joins and complex transformations up to 1000+ lines here.
-- This is a simple query for demo purposes - transformations already applied in the sample data.

CREATE OR REPLACE PROCEDURE `Demo.get_orders_detail_updates`(
  src_table STRING, 
  tgt_table STRING, 
  target_cutoff_time DATETIME
)
BEGIN
  DECLARE initial_load_date DATETIME;
  DECLARE target_etl_cutoff_time DATETIME;
  DECLARE last_etl_cutoff_time DATETIME;
  
  SET initial_load_date = '2025-09-01T00:00:00';
  SET target_etl_cutoff_time = target_cutoff_time;
  SET last_etl_cutoff_time = (
    SELECT cutoff_time
    FROM `Demo.int_etl_cutoff`
    WHERE src_table_name = src_table
      AND tgt_table_name = tgt_table
  );
  
  INSERT INTO `Demo.stg_orders_detail`(
    SELECT 
      id,
      order_id,
      order_number,
      order_date,
      update_date,
      customer_id,
      financial_status,
      fulfillment_status,
      discount_codes,
      channel,
      partner_name,
      product_key,
      product_name,
      cost,
      price,
      total_discount
    FROM `Demo.raw_shopify_order_items`
    WHERE update_date >= COALESCE(last_etl_cutoff_time, initial_load_date)
      AND update_date < target_etl_cutoff_time
  );
END;

-- PROCEDURE 3: Merge staged data into fact table and update tracking
CREATE OR REPLACE PROCEDURE `Demo.migrate_staged_orders_detail_updates`()
BEGIN 
  DECLARE LineageKey STRING;

  -- Get the current lineage key for this run
  SET LineageKey = (
    SELECT lineage_key
    FROM Demo.int_lineage
    WHERE src_table_name = 'Demo.raw_shopify_order_items'
      AND data_load_completed IS NULL
    ORDER BY lineage_key DESC
    LIMIT 1
  );

  -- Merge staged data into fact table
  MERGE INTO `Demo.fact_order_detail` t 
  USING (
    SELECT DISTINCT 
      id AS detail_id,
      order_id,
      order_number,
      order_date,
      update_date,
      customer_id,
      financial_status,
      fulfillment_status,
      discount_codes,
      channel,
      partner_name,
      product_key,
      product_name,
      cost,
      price,
      total_discount,
      CURRENT_DATETIME('America/Los_Angeles') AS dt_insert,
      CAST(NULL AS DATETIME) AS dt_update
    FROM `Demo.stg_orders_detail`
  ) s ON t.detail_id = s.detail_id 
  
  -- Update existing records if key fields changed
  WHEN MATCHED AND (
    t.financial_status <> s.financial_status 
    OR t.fulfillment_status <> s.fulfillment_status 
    OR COALESCE(t.discount_code, '') <> COALESCE(s.discount_codes, '')
    OR COALESCE(t.marketing_channel, '') <> COALESCE(s.channel, '')
    OR COALESCE(t.partner, '') <> COALESCE(s.partner_name, '')
    OR t.update_date <> s.update_date
  )
  THEN UPDATE SET
    t.financial_status = s.financial_status,
    t.fulfillment_status = s.fulfillment_status,
    t.discount_code = s.discount_codes,
    t.marketing_channel = s.channel,
    t.partner = s.partner_name,
    t.update_date = s.update_date,
    t.dt_update = CURRENT_DATETIME('America/Los_Angeles')
    
  -- Insert new records
  WHEN NOT MATCHED THEN INSERT(
    detail_id, order_id, order_number, order_date, update_date, customer_id,
    financial_status, fulfillment_status, discount_code, marketing_channel, partner,
    product_key, product_name, cost, price, total_discount, dt_insert, dt_update
  )
  VALUES(
    detail_id, order_id, order_number, order_date, update_date, customer_id,
    financial_status, fulfillment_status, discount_codes, channel, partner_name,
    product_key, product_name, cost, price, total_discount, dt_insert, dt_update
  );

  -- Mark lineage as completed successfully
  UPDATE `Demo.int_lineage`
  SET 
    data_load_completed = CAST(CURRENT_DATETIME('America/Los_Angeles') AS DATETIME),
    was_successful = true
  WHERE lineage_key = LineageKey;

  -- Update cutoff time for next incremental run
  UPDATE `Demo.int_etl_cutoff`
  SET cutoff_time = (
    SELECT COALESCE(
      (SELECT MAX(update_date) FROM `Demo.stg_orders_detail`),
      (SELECT MAX(update_date) FROM Demo.fact_order_detail) 
    )
  )
  WHERE src_table_name = 'Demo.raw_shopify_order_items'; 
END;

-- ============================================================================
-- STEP 4: INITIALIZE CUTOFF TRACKING (run once per source/target pair)
-- ============================================================================

INSERT INTO `Demo.int_etl_cutoff`(
  src_table_name,
  tgt_table_name,
  cutoff_time
)
VALUES(
  'Demo.raw_shopify_order_items',
  'Demo.fact_order_detail',
  CAST(NULL AS DATETIME)   
);

-- ============================================================================
-- STEP 5: EXECUTE THE ETL PATTERN
-- ============================================================================

-- Step 5a: Start lineage tracking
CALL `Demo.get_lineage_key`(
  'Demo.raw_shopify_order_items',
  'Demo.fact_order_detail',
  CURRENT_DATETIME('America/Los_Angeles')
);

-- Step 5b: Extract and stage incremental data
TRUNCATE TABLE `Demo.stg_orders_detail`;
CALL `Demo.get_orders_detail_updates`(
  'Demo.raw_shopify_order_items',
  'Demo.fact_order_detail', 
  '2025-09-17T00:00:00'  -- Test with specific date(s)
  -- CURRENT_DATETIME('America/Los_Angeles') -- Use this in production
);

-- Step 5c: Merge to fact table and complete tracking
CALL Demo.migrate_staged_orders_detail_updates();

-- ============================================================================
-- STEP 6: VERIFY RESULTS AND TESTING QUERIES
-- ============================================================================

-- Check final results
SELECT * FROM `Demo.fact_order_detail` ORDER BY detail_id;

-- Check lineage audit trail
SELECT * FROM Demo.int_lineage ORDER BY data_load_started DESC;

-- Check cutoff tracking
SELECT * FROM Demo.int_etl_cutoff;

-- Count records in each table
SELECT 'raw_data' AS table_name, COUNT(*) AS record_count 
FROM `Demo.raw_shopify_order_items`
UNION ALL
SELECT 'staged_data', COUNT(*) 
FROM `Demo.stg_orders_detail`
UNION ALL
SELECT 'fact_data', COUNT(*) 
FROM `Demo.fact_order_detail`;

-- ============================================================================
-- RESET COMMANDS (for testing)
-- ============================================================================

/*
-- Reset cutoff time to reprocess all data
UPDATE `Demo.int_etl_cutoff`
SET cutoff_time = CAST(NULL AS DATETIME)
WHERE src_table_name = 'Demo.raw_shopify_order_items';

-- Clear all processed data (keeps raw data)
TRUNCATE TABLE `Demo.stg_orders_detail`;
TRUNCATE TABLE `Demo.fact_order_detail`;
TRUNCATE TABLE `Demo.int_lineage`;

-- Test different date ranges by changing target_cutoff_time:
-- '2025-09-16T00:00:00' -- Process through 2025-09-15
-- '2025-09-17T00:00:00' -- Process through 2025-09-16  
-- '2025-09-20T00:00:00' -- Process through 2025-09-19
*/