USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE AUDITING.LOAD_RECONCILIATION_DETAIL AS
BEGIN
    DECLARE
	    @proc_start_time DATETIME = GETDATE(), 
        @proc_end_time DATETIME,
        @max_audit_id INT = ISNULL(
            (SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_DETAIL),
            0
        ),
        @max_audit_job_id INT = ISNULL(
            (SELECT MAX(audit_job_id) FROM AUDITING.RECONCILIATION_DETAIL),
            0
        ),
        @customer_records_count INT = (SELECT COUNT(STG_LAYER.CUSTOMERS.entity_event_id)
                                       FROM STG_LAYER.CUSTOMERS),
        @product_records_count INT = (SELECT COUNT(STG_LAYER.PRODUCTS.entity_event_id)
                                      FROM STG_LAYER.PRODUCTS),
        @raw_customer_records_count INT = (SELECT COUNT(RAW_LAYER.ENTITY_EXTRACT.entity_event_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'customer'),
        @raw_product_records_count INT = (SELECT COUNT(DISTINCT RAW_LAYER.ENTITY_EXTRACT.entity_event_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'product')
    IF @customer_records_count <> @raw_customer_records_count 
        INSERT INTO AUDITING.RECONCILIATION_DETAIL (
	        audit_id,
	        audit_job_id,
	        load_phase,
            batch_id ,
            source_object,
	        source_column,
	        source_value,
            target_object,
	        target_column,
	        target_value,
	        audit_result
        )
        SELECT 
            @max_audit_id + ROW_NUMBER() OVER (ORDER BY (SELECT 1)),
            @max_audit_job_id + 1,
            'RAW to STG',
            RAW_LAYER.ENTITY_EXTRACT.batch_id,
            'RAW_LAYER.ENTITY_EXTRACT',
            'entity_event_id',
            RAW_LAYER.ENTITY_EXTRACT.entity_event_id,
            'STG_LAYER.CUSTOMERS',
            'entity_event_id',
            STG_LAYER.CUSTOMERS.entity_event_id,
            'Missing'
        FROM
            RAW_LAYER.ENTITY_EXTRACT 
            LEFT JOIN STG_LAYER.CUSTOMERS
            ON RAW_LAYER.ENTITY_EXTRACT.entity_event_id = STG_LAYER.CUSTOMERS.entity_event_id
        WHERE 
            RAW_LAYER.ENTITY_EXTRACT.entity_type = 'customer'
            AND STG_LAYER.CUSTOMERS.customer_id IS NULL;
END;

EXEC AUDITING.LOAD_RECONCILIATION_DETAIL;
SELECT * FROM AUDITING.RECONCILIATION_DETAIL;