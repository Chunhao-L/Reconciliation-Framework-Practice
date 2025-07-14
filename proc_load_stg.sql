USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS AS
BEGIN
    DECLARE 
        @proc_start_time DATETIME, 
        @proc_end_time DATETIME,
        @start_time DATETIME, 
        @end_time DATETIME, 
        @max_customer_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM STG_LAYER.CUSTOMERS),
            0
        ),
        @max_products_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM STG_LAYER.PRODUCTS),
            0
        ); 
    SET @proc_start_time = GETDATE();
    SET @start_time = GETDATE();
    PRINT '>> Inserting Data Into: STG_LAYER.CUSTOMERS';
    PRINT '>> Inserting Data with batch_id greater than ' + CAST(@max_customer_batch_id as VARCHAR);
    INSERT INTO STG_LAYER.CUSTOMERS (
        batch_id,
        entity_event_id,
        customer_id,
        fname,
        lname,
        gender
    )
    SELECT
        batch_id,
        entity_event_id,
        entity_id,
        JSON_VALUE(entity_json_data, '$.first_name') AS first_name,
        JSON_VALUE(entity_json_data, '$.last_name') AS last_name,
        JSON_VALUE(entity_json_data, '$.gender') AS gender
    FROM RAW_LAYER.ENTITY_EXTRACT
    WHERE 1 = 1 
        AND entity_type = 'customer' 
        AND batch_id > @max_customer_batch_id;
    SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    SET @start_time = GETDATE();
    PRINT '>> Inserting Data Into: STG_LAYER.PRODUCTS';
    PRINT '>> Inserting Data with batch_id greater than ' + CAST(@max_products_batch_id as VARCHAR);
    INSERT INTO STG_LAYER.PRODUCTS (
        batch_id,
        entity_event_id,
        product_id,
        product_name,
        unit_price
    )
    SELECT
        batch_id,
        entity_event_id,
        entity_id,
        JSON_VALUE(entity_json_data, '$.product_name') AS product_name,
        JSON_VALUE(entity_json_data, '$.unit_price') AS unit_price
    FROM RAW_LAYER.ENTITY_EXTRACT
    WHERE 1 = 1 
        AND entity_type = 'product'
        AND batch_id > @max_products_batch_id;
    SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    SET @proc_end_time = GETDATE();
    PRINT '>> Proc total Duration: ' + CAST(DATEDIFF(second, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
END;

TRUNCATE TABLE STG_LAYER.CUSTOMERS;
TRUNCATE TABLE STG_LAYER.PRODUCTS;
EXEC STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS;
SELECT * FROM STG_LAYER.CUSTOMERS;
SELECT * FROM STG_LAYER.PRODUCTS;