USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE AUDITING.LOAD_CONCILIATION_OVERVIEW AS
BEGIN
	DECLARE
		@proc_start_time DATETIME, 
        @proc_end_time DATETIME,
        @start_time DATETIME, 
        @end_time DATETIME, 
        @max_audit_id INT = ISNULL(
            (SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_OVERVIEW),
            0
        ),
        @max_audit_job_id INT = ISNULL(
            (SELECT MAX(audit_job_id) FROM AUDITING.RECONCILIATION_OVERVIEW),
            0
        ),
        @customer_batchs_count INT = (SELECT COUNT(DISTINCT STG_LAYER.CUSTOMERS.batch_id) 
                                      FROM STG_LAYER.CUSTOMERS),
        @customer_records_count INT = (SELECT COUNT(STG_LAYER.CUSTOMERS.entity_event_id)
                                       FROM STG_LAYER.CUSTOMERS),
        @product_batchs_count INT = (SELECT COUNT(DISTINCT STG_LAYER.PRODUCTS.batch_id)
                                     FROM STG_LAYER.PRODUCTS),
        @product_records_count INT = (SELECT COUNT(STG_LAYER.PRODUCTS.entity_event_id)
                                      FROM STG_LAYER.PRODUCTS),
        @raw_customer_batchs_count INT = (SELECT COUNT(DISTINCT RAW_LAYER.ENTITY_EXTRACT.batch_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'customer'),
        @raw_customer_records_count INT = (SELECT COUNT(RAW_LAYER.ENTITY_EXTRACT.entity_event_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'customer'),
        @raw_product_batchs_count INT = (SELECT COUNT(DISTINCT RAW_LAYER.ENTITY_EXTRACT.batch_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'product'),
        @raw_product_records_count INT = (SELECT COUNT(DISTINCT RAW_LAYER.ENTITY_EXTRACT.entity_event_id)
                                          FROM RAW_LAYER.ENTITY_EXTRACT
                                          WHERE entity_type = 'product')
        INSERT INTO AUDITING.RECONCILIATION_OVERVIEW (
            audit_id,
            audit_job_id,
            load_phase,
            source_object,
	        source_column,
            target_object,
	        target_column,
            raw_batch_id_count,
            stg_batch_id_count,
            batch_id_audit_result,
            batch_id_audit_difference,
            raw_records_count,
            stg_records_count,
            records_audit_result,
            records_audit_difference
        )
        VALUES
        (   @max_audit_id+1, 
            @max_audit_job_id+1, 
            'RAW to STG', 
            'RAW_LAYER.ENTITY_EXTRACT',
            'entity_event_id',
            'STG_LAYER.CUSTOMERS',
            'entity_event_id',
            @raw_customer_batchs_count,
            @customer_batchs_count,
            CASE 
                WHEN @raw_customer_batchs_count = @customer_batchs_count
                THEN 'Match'
                ELSE 'Mismatch'
            END,
            @raw_customer_batchs_count - @customer_batchs_count,
            @raw_customer_records_count,
            @customer_records_count,
            CASE 
                WHEN @raw_customer_records_count = @customer_records_count
                THEN 'Match'
                ELSE 'Mismatch'
            END,
            @raw_customer_records_count - @customer_records_count
        ),
        (   @max_audit_id+2, 
            @max_audit_job_id+1, 
            'RAW to STG', 
            'RAW_LAYER.ENTITY_EXTRACT',
            'entity_event_id',
            'STG_LAYER.PRODUCTS',
            'entity_event_id',
            @raw_product_batchs_count,
            @product_batchs_count,
            CASE 
                WHEN @raw_product_batchs_count = @product_batchs_count
                THEN 'Match'
                ELSE 'Mismatch'
            END,
            @raw_product_batchs_count - @product_batchs_count,
            @raw_product_records_count,
            @product_records_count,
            CASE 
                WHEN @raw_product_records_count = @product_records_count
                THEN 'Match'
                ELSE 'Mismatch'
            END,
            @raw_product_records_count - @product_records_count
        );
           
END;

EXEC AUDITING.LOAD_CONCILIATION_OVERVIEW;
SELECT * FROM AUDITING.RECONCILIATION_OVERVIEW;