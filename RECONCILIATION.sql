USE TheDataGuyzPractice;
GO

IF OBJECT_ID('AUDITING.RECONCILIATION_REFERENCE', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_REFERENCE;
GO

CREATE TABLE AUDITING.RECONCILIATION_REFERENCE (
    reference_id INT,
    load_phase NVARCHAR(50),
    source_schema NVARCHAR(50),
    source_object NVARCHAR(50),
    source_column NVARCHAR(50),
    join_query NVARCHAR(MAX),
    filter_query NVARCHAR(MAX),
    target_schema NVARCHAR(50),
    target_object NVARCHAR(50),
    target_column NVARCHAR(50),
);

INSERT INTO AUDITING.RECONCILIATION_REFERENCE (reference_id, load_phase, source_schema, source_object, source_column, join_query, filter_query, target_schema, target_object, target_column)
VALUES
(1, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'batch_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'batch_id'),
(2, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_event_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'entity_event_id'),
(3, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_id', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'product_id'),
(4, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'product_name'),
(5, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''product''', 'STG_LAYER', 'PRODUCTS', 'unit_price'),
(6, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'batch_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'batch_id'),
(7, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_event_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'entity_event_id'),
(8, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_id', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'customer_id'),
(9, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'fname'),
(10, 'RAW to STG', 'RAW_LAYER', 'ENTITY_EXTRACT', 'entity_json_data', NULL, 'entity_type = ''customer''', 'STG_LAYER', 'CUSTOMERS', 'lname')

select * from AUDITING.RECONCILIATION_REFERENCE;

IF OBJECT_ID('AUDITING.RECONCILIATION_OVERVIEW', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_OVERVIEW;
GO


CREATE TABLE AUDITING.RECONCILIATION_OVERVIEW (
    audit_id INT,
    audit_job_id INT,
    load_phase NVARCHAR(50),
    source_object NVARCHAR(50),
	source_column NVARCHAR(50),
    target_object NVARCHAR(50),
	target_column NVARCHAR(50),
    raw_batch_id_count INT,
    stg_batch_id_count INT,
    batch_id_audit_result NVARCHAR(20),
    batch_id_audit_difference INT,
    raw_records_count INT,
    stg_records_count INT,
    records_audit_result NVARCHAR(20),
    records_audit_difference INT,
    created_date DATETIME DEFAULT GETDATE()
);

IF OBJECT_ID('AUDITING.RECONCILIATION_DETAIL', 'U') IS NOT NULL
    DROP TABLE AUDITING.RECONCILIATION_DETAIL;
GO

CREATE TABLE AUDITING.RECONCILIATION_DETAIL (
	audit_id INT,
	audit_job_id INT,
	load_phase NVARCHAR(50),
    batch_id  INT,
    source_object NVARCHAR(50),
	source_column NVARCHAR(50),
	source_value NVARCHAR(MAX),
    target_object NVARCHAR(50),
	target_column NVARCHAR(50),
	target_value NVARCHAR(MAX),
	audit_result NVARCHAR(50),
    created_date DATETIME DEFAULT GETDATE()
);