USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE RAW_LAYER.LOAD_ENTITY_EXTRACT AS
BEGIN
    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME, 
        @max_entity_event_id INT = ISNULL(
            (SELECT MAX(entity_event_id) FROM RAW_LAYER.ENTITY_EXTRACT),
            0
        ),
        @max_batch_id INT = ISNULL(
            (SELECT MAX(batch_id) FROM RAW_LAYER.ENTITY_EXTRACT),
            0
        );
    SET @start_time = GETDATE();
    PRINT '>> Inserting Data Into: RAW_LAYER.LOAD_ENTITY_EXTRACT';
    PRINT '>> Inserting Batches with batch_id greater than ' + CAST(@max_batch_id as VARCHAR);
    WITH ParsedData AS (
        SELECT 
            be.batch_id,
            bd.entity_id,
            be.extraction_date,
            bd.entity_type,
            bd.entity,
            ROW_NUMBER() OVER (ORDER BY be.batch_id, be.extraction_date, bd.entity_id) AS rn
        FROM RAW_LAYER.BATCH_EXTRACT be
        CROSS APPLY OPENJSON(be.json_data, '$.batch_data')
        WITH (
            entity_id INT,
            entity_type NVARCHAR(50),
            entity NVARCHAR(MAX) AS JSON
        ) AS bd
        WHERE 1 = 1 
            AND batch_id > @max_batch_id
    )
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
        rn + @max_entity_event_id AS entity_event_id,
        entity_id,
        extraction_date,
        entity_type,
        entity
    FROM ParsedData;
    SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
END;

TRUNCATE TABLE RAW_LAYER.ENTITY_EXTRACT;
EXEC RAW_LAYER.LOAD_ENTITY_EXTRACT;
SELECT * FROM RAW_LAYER.ENTITY_EXTRACT;