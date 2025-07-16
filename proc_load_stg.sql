/*
====================================================================================================
Procedure: STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS
----------------------------------------------------------------------------------------------------
Purpose:
--------
This stored procedure extracts parsed entity data from `RAW_LAYER.ENTITY_EXTRACT` and loads it
into structured staging tables: `STG_LAYER.CUSTOMERS` and `STG_LAYER.PRODUCTS`.

Features:
---------
- Performs **incremental loading** by checking the max `batch_id` already loaded in each staging table.
- Uses `JSON_VALUE()` to extract individual fields from the raw JSON entity data.
- Separates logic and timing for each entity type (customer and product).
- Includes performance logging via PRINT statements.

How to Use:
-----------
1. Run after executing `RAW_LAYER.LOAD_ENTITY_EXTRACT`.
2. Execute: `EXEC STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS`.
3. Data will be loaded into `STG_LAYER.CUSTOMERS` and `STG_LAYER.PRODUCTS`.

Note:
-----
This procedure assumes consistent JSON structure and valid data in `RAW_LAYER.ENTITY_EXTRACT`.

====================================================================================================
*/

USE TheDataGuyzPractice;
GO

-- Create or replace the procedure for loading staging layer tables
CREATE OR ALTER PROCEDURE STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS AS
BEGIN
    -- Declare timing variables for logging
    DECLARE 
        @proc_start_time DATETIME, 
        @proc_end_time DATETIME,
        @start_time DATETIME, 
        @end_time DATETIME,

        -- Track latest batch IDs already loaded to enable incremental load
        @max_customer_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM STG_LAYER.CUSTOMERS),
            0
        ),
        @max_products_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM STG_LAYER.PRODUCTS),
            0
        ); 

    -- Start the overall procedure timer
    SET @proc_start_time = GETDATE();

    -----------------------------------------------------------------------------------------------
    -- Load CUSTOMERS from RAW_LAYER.ENTITY_EXTRACT
    -----------------------------------------------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT '>> Inserting Data Into: STG_LAYER.CUSTOMERS';
    PRINT '>> Inserting Data with batch_id greater than ' + CAST(@max_customer_batch_id AS VARCHAR);

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
        JSON_VALUE(entity_json_data, '$.first_name') AS first_name, -- Extract first_name from JSON
        JSON_VALUE(entity_json_data, '$.last_name') AS last_name,   -- Extract last_name from JSON
        JSON_VALUE(entity_json_data, '$.gender') AS gender          -- Extract gender from JSON
    FROM RAW_LAYER.ENTITY_EXTRACT
    WHERE 
        entity_type = 'customer' 
        AND batch_id > @max_customer_batch_id; -- Incremental load

    SET @end_time = GETDATE();
    PRINT '>> CUSTOMERS Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -----------------------------------------------------------------------------------------------
    -- Load PRODUCTS from RAW_LAYER.ENTITY_EXTRACT
    -----------------------------------------------------------------------------------------------
    SET @start_time = GETDATE();

    PRINT '>> Inserting Data Into: STG_LAYER.PRODUCTS';
    PRINT '>> Inserting Data with batch_id greater than ' + CAST(@max_products_batch_id AS VARCHAR);

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
        JSON_VALUE(entity_json_data, '$.product_name') AS product_name, -- Extract product_name
        JSON_VALUE(entity_json_data, '$.unit_price') AS unit_price      -- Extract unit_price
    FROM RAW_LAYER.ENTITY_EXTRACT
    WHERE 
        entity_type = 'product'
        AND batch_id > @max_products_batch_id; -- Incremental load

    SET @end_time = GETDATE();
    PRINT '>> PRODUCTS Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

    -----------------------------------------------------------------------------------------------
    -- End of procedure
    -----------------------------------------------------------------------------------------------
    SET @proc_end_time = GETDATE();
    PRINT '>> Total Procedure Duration: ' + CAST(DATEDIFF(SECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
END;
GO

-- Clean staging tables before test (only for dev/test purposes)
TRUNCATE TABLE STG_LAYER.CUSTOMERS;
TRUNCATE TABLE STG_LAYER.PRODUCTS;

-- Run the load procedure
EXEC STG_LAYER.LOAD_STG_CUSTOMERS_AND_PRODUCTS;

-- View the loaded data
SELECT * FROM STG_LAYER.CUSTOMERS;
SELECT * FROM STG_LAYER.PRODUCTS;

DELETE FROM STG_LAYER.CUSTOMERS WHERE entity_event_id = 12;
DELETE FROM STG_LAYER.PRODUCTS WHERE entity_event_id = 17;