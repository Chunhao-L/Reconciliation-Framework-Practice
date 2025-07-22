/*
====================================================================================================
Procedure: RAW_LAYER.LOAD_ENTITY_EXTRACT
----------------------------------------------------------------------------------------------------
Purpose:
--------
This stored procedure parses JSON batch data from `RAW_LAYER.BATCH_EXTRACT` and inserts each
entity as a flattened row into `RAW_LAYER.ENTITY_EXTRACT`.

Key Features:
-------------
- Uses `OPENJSON` to parse JSON arrays stored in `json_data`.
- Ensures **incremental loading** by inserting only new batches (based on `batch_id`).
- Maintains a unique `entity_event_id` using the current max value + row number offset.
- Outputs duration of load process using `PRINT`.

Usage:
------
1. Ensure `RAW_LAYER.BATCH_EXTRACT` is populated with valid JSON batch payloads.
2. Run: `EXEC RAW_LAYER.LOAD_ENTITY_EXTRACT;`
3. Output will be inserted into `RAW_LAYER.ENTITY_EXTRACT`.

Note:
-----
The procedure assumes `json_data` is well-formed and consistent across batches.

====================================================================================================
*/

USE TheDataGuyzPractice;
GO

-- Create or replace the load procedure
CREATE OR ALTER PROCEDURE RAW_LAYER.LOAD_ENTITY_EXTRACT AS
BEGIN
    -- Declare audit and tracking variables
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 

        -- Track the current max entity_event_id (for uniqueness)
        @max_entity_event_id INT = ISNULL(
            (SELECT MAX(entity_event_id) FROM RAW_LAYER.ENTITY_EXTRACT),
            0
        ),

        -- Track the current max batch_id (for incremental logic)
        @max_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM RAW_LAYER.ENTITY_EXTRACT),
            0
        );

    -- Capture the start time for performance logging
    SET @start_time = GETDATE();

    PRINT '>> Inserting Data Into: RAW_LAYER.LOAD_ENTITY_EXTRACT';
    PRINT '>> Inserting Batches with batch_id greater than ' + CAST(@max_batch_id as VARCHAR);

    -- CTE: Parse new JSON data from BATCH_EXTRACT using OPENJSON
    WITH ParsedData AS (
        SELECT 
            be.batch_id,                             -- Batch metadata
            bd.entity_id,                            -- Entity ID from JSON
            be.extraction_date,                      -- Extraction timestamp from batch record
            bd.entity_type,                          -- 'customer' or 'product'
            bd.entity,                               -- Raw JSON object for the entity
            ROW_NUMBER() OVER (                      -- Generate a row number to derive entity_event_id
                ORDER BY be.batch_id, be.extraction_date, bd.entity_id
            ) AS rn
        FROM RAW_LAYER.BATCH_EXTRACT be
        CROSS APPLY OPENJSON(be.json_data, '$.batch_data')  -- Parse the JSON array inside json_data
        WITH (
            entity_id INT,
            entity_type NVARCHAR(50),
            entity NVARCHAR(MAX) AS JSON
        ) AS bd
        WHERE batch_id > @max_batch_id              -- Only parse new/unprocessed batches
    )

    -- Insert parsed rows into ENTITY_EXTRACT
    INSERT INTO RAW_LAYER.ENTITY_EXTRACT (
        batch_id,
        entity_event_id,
        entity_id,
        extraction_date,
        entity_type,
        entity_json_data
    )
    SELECT 
        batch_id,
        rn + @max_entity_event_id AS entity_event_id,  -- Ensure globally unique entity_event_id
        entity_id,
        extraction_date,
        entity_type,
        entity
    FROM ParsedData;

    -- Capture end time and log duration
    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
END;
GO

-- Clear out previous ENTITY_EXTRACT data (for demo/testing purposes)
TRUNCATE TABLE RAW_LAYER.ENTITY_EXTRACT;

-- Execute the load procedure to extract and insert new entities
EXEC RAW_LAYER.LOAD_ENTITY_EXTRACT;

-- View results
SELECT * FROM RAW_LAYER.ENTITY_EXTRACT;
