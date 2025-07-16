/*
=========================================================================================================
Purpose:
--------
This script creates the **AUDITING layer** tables used for data reconciliation between different layers
of the data pipeline — specifically from RAW to STG. The reconciliation helps ensure that records
have been correctly transferred, transformed, and loaded.

The script creates the following tables:

1. **RECONCILIATION_REFERENCE**:
   - A metadata-driven configuration table that defines how to map and compare columns between
     the source (RAW_LAYER.ENTITY_EXTRACT) and target (STG_LAYER.CUSTOMERS / PRODUCTS) objects.

2. **RECONCILIATION_OVERVIEW**:
   - Stores high-level audit results of batch-level and record-level counts between source and target.

3. **RECONCILIATION_DETAIL**:
   - Stores detailed column-level comparison results between source and target rows.

How to Use:
-----------
- Run this after creating the RAW_LAYER and STG_LAYER schemas/tables.
- Populate RECONCILIATION_REFERENCE as configuration.
- Use stored procedures or scripts to populate the OVERVIEW and DETAIL tables based on the config.

Preconditions:
--------------
- Assumes the existence of RAW_LAYER.ENTITY_EXTRACT, STG_LAYER.CUSTOMERS, and STG_LAYER.PRODUCTS.
- Should be executed in SQL Server 2016+ for best compatibility.
=========================================================================================================
*/

-- Switch to the target working database
USE TheDataGuyzPractice;
GO

-- Drop the RECONCILIATION_REFERENCE table if it exists
IF OBJECT_ID('AUDITING.RECONCILIATION_REFERENCE', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_REFERENCE;
GO

-- Create metadata-driven config table for recon rules between RAW and STG layers
CREATE TABLE AUDITING.RECONCILIATION_REFERENCE (
    reference_id INT,                      -- Unique ID for the reconciliation rule
    load_phase NVARCHAR(50),              -- e.g. 'RAW to STG'
    source_schema NVARCHAR(50),           -- Source schema (e.g., RAW_LAYER)
    source_object NVARCHAR(50),           -- Source table name
    source_column NVARCHAR(50),           -- Column to compare from source
    join_query NVARCHAR(MAX),             -- Optional join logic (can be used for custom joins)
    filter_query NVARCHAR(MAX),           -- Filter logic (e.g., entity_type='product')
    target_schema NVARCHAR(50),           -- Target schema (e.g., STG_LAYER)
    target_object NVARCHAR(50),           -- Target table name
    target_column NVARCHAR(50)            -- Column to compare from target
);
GO

-- Insert reconciliation rules to compare RAW_LAYER.ENTITY_EXTRACT to STG_LAYER tables
INSERT INTO AUDITING.RECONCILIATION_REFERENCE (
    reference_id, load_phase, source_schema, source_object, source_column, 
    join_query, filter_query, target_schema, target_object, target_column
)
VALUES
-- Reconciliation for PRODUCTS
(1, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'batch_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'batch_id'),
(2, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_event_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'entity_event_id'),
(3, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'product_id'),
(4, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'product_name'),
(5, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'unit_price'),

-- Reconciliation for CUSTOMERS
(6, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'batch_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'batch_id'),
(7, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_event_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'entity_event_id'),
(8, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'customer_id'),
(9, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'fname'),
(10, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'lname');
GO

-- View reference metadata (optional debug step)
SELECT * FROM AUDITING.RECONCILIATION_REFERENCE;


-- Drop the RECONCILIATION_OVERVIEW table if it exists
IF OBJECT_ID('AUDITING.RECONCILIATION_OVERVIEW', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_OVERVIEW;
GO

-- Create a summary audit table that holds counts and results per batch/load
CREATE TABLE AUDITING.RECONCILIATION_OVERVIEW (
    audit_id INT,                          -- Unique ID for the audit run
    audit_job_id INT,                      -- Job run identifier
    load_phase NVARCHAR(50),              -- Phase name (e.g., 'RAW to STG')
    source_object NVARCHAR(50),           -- Source table being audited
    source_column NVARCHAR(50),           -- Source column in the audit
    target_object NVARCHAR(50),           -- Target table being audited
    target_column NVARCHAR(50),           -- Target column
    raw_batch_id_count INT,               -- Distinct batch_id count in source
    stg_batch_id_count INT,               -- Distinct batch_id count in target
    batch_id_audit_result NVARCHAR(20),   -- Match/Mismatch
    batch_id_audit_difference INT,        -- Numeric difference (if any)
    raw_records_count INT,                -- Total records in RAW
    stg_records_count INT,                -- Total records in STG
    records_audit_result NVARCHAR(20),    -- Match/Mismatch
    records_audit_difference INT,         -- Difference in record count
    created_date DATETIME DEFAULT GETDATE() -- Timestamp of the audit run
);
GO

-- Drop the RECONCILIATION_DETAIL table if it exists
IF OBJECT_ID('AUDITING.RECONCILIATION_DETAIL', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_DETAIL;
GO

-- Create a detailed-level audit result table (column-by-column comparison results)
CREATE TABLE AUDITING.RECONCILIATION_DETAIL (
    audit_id INT,                          -- ID that ties to OVERVIEW table
    audit_job_id INT,                      -- Job run identifier
    load_phase NVARCHAR(50),              -- Phase being audited
    batch_id INT,                          -- The batch being validated
    source_object NVARCHAR(50),           -- Source table
    source_column NVARCHAR(50),           -- Source column name
    source_value NVARCHAR(MAX),           -- Actual value from source
    target_object NVARCHAR(50),           -- Target table
    target_column NVARCHAR(50),           -- Target column name
    target_value NVARCHAR(MAX),           -- Actual value from target
    audit_result NVARCHAR(50),            -- Comparison result (e.g., 'MATCH', 'MISMATCH')
    created_date DATETIME DEFAULT GETDATE() -- Timestamp
);
GO
