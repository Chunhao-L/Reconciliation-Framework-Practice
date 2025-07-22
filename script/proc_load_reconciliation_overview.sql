/*
====================================================================================================
Procedure: AUDITING.LOAD_RECONCILIATION_OVERVIEW
----------------------------------------------------------------------------------------------------
Purpose:
--------
This stored procedure performs a reconciliation audit between source and target tables by comparing
batch counts and record counts. It dynamically queries tables listed in the
`AUDITING.RECONCILIATION_REFERENCE` table, applies filters, and inserts reconciliation results
into `AUDITING.RECONCILIATION_OVERVIEW`.

Features:
---------
- Uses dynamic SQL to count distinct batch IDs and record counts in source and target tables.
- Compares counts and flags results as "Match" or "Mismatch".
- Supports filtering source data via dynamic WHERE clauses.
- Uses a cursor to iterate over multiple reconciliation references.
- Inserts audit details including differences and summary status.
- Logs procedure start and total execution time.

How to Use:
-----------
1. Populate `AUDITING.RECONCILIATION_REFERENCE` with the tables, columns, and filters to audit.
2. Run the procedure using: `EXEC AUDITING.LOAD_RECONCILIATION_OVERVIEW`.
3. Query `AUDITING.RECONCILIATION_OVERVIEW` to review reconciliation results.
4. Optionally, truncate `RECONCILIATION_OVERVIEW` before rerunning to clear old data.

Note:
-----
- The procedure assumes `source_column` = 'entity_event_id' as a filter in the reference table.
- Relies on consistent naming of `batch_id` columns in source and target tables.
- Designed for auditing batch and record-level data integrity across layers.

====================================================================================================
*/

USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE AUDITING.LOAD_RECONCILIATION_OVERVIEW
AS
BEGIN
    SET NOCOUNT ON;

    -- Declare variables to track execution time and audit IDs
    DECLARE 
        @proc_start_time DATETIME = GETDATE(),  -- Procedure start time
        @proc_end_time DATETIME,                -- Procedure end time
        @max_audit_id INT = ISNULL((SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_OVERVIEW), 0), -- Max audit_id for incrementing
        @max_audit_job_id INT = ISNULL((SELECT MAX(audit_job_id) FROM AUDITING.RECONCILIATION_OVERVIEW), 0), -- Max job ID to increment

        -- Variables for current reconciliation reference metadata
        @load_phase NVARCHAR(50),
        @source_schema NVARCHAR(50),
        @source_object NVARCHAR(50),
        @source_column NVARCHAR(50),
        @filter_query NVARCHAR(MAX),
        @target_schema NVARCHAR(50),
        @target_object NVARCHAR(50),
        @target_column NVARCHAR(50),

        -- Variables for counts fetched dynamically
        @raw_batch_count INT,
        @stg_batch_count INT,
        @raw_record_count INT,
        @stg_record_count INT,

        -- Dynamic SQL string variable
        @sql NVARCHAR(MAX),

        -- Counter to create unique audit_id values
        @row_offset INT = 1;

    -- Cursor to iterate reconciliation reference rows where source_column = 'entity_event_id'
    DECLARE ref_cursor CURSOR FOR
        SELECT DISTINCT 
            load_phase, source_schema, source_object, source_column,
            filter_query, target_schema, target_object, target_column
        FROM AUDITING.RECONCILIATION_REFERENCE
        WHERE source_column = 'entity_event_id';

    PRINT '>> Starting the reconciliation overview procedure.';
    OPEN ref_cursor;

    FETCH NEXT FROM ref_cursor INTO 
        @load_phase, @source_schema, @source_object, @source_column,
        @filter_query, @target_schema, @target_object, @target_column;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Build dynamic SQL to count distinct batch IDs and records in source table applying the filter
        SET @sql = '
            SELECT 
                @raw_batch_count_out = COUNT(DISTINCT batch_id),
                @raw_record_count_out = COUNT(' + QUOTENAME(@source_column) + ')
            FROM ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_object) + '
            WHERE ' + @filter_query + ';';

        EXEC sp_executesql @sql,
            N'@raw_batch_count_out INT OUTPUT, @raw_record_count_out INT OUTPUT',
            @raw_batch_count OUTPUT, @raw_record_count OUTPUT;

        -- Build dynamic SQL to count distinct batch IDs and records in target table (no filter)
        SET @sql = '
            SELECT 
                @stg_batch_count_out = COUNT(DISTINCT batch_id),
                @stg_record_count_out = COUNT(' + QUOTENAME(@target_column) + ')
            FROM ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_object) + ';';

        EXEC sp_executesql @sql,
            N'@stg_batch_count_out INT OUTPUT, @stg_record_count_out INT OUTPUT',
            @stg_batch_count OUTPUT, @stg_record_count OUTPUT;

        -- Insert reconciliation summary into the overview table with match/mismatch flags and differences
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
        VALUES (
            @max_audit_id + @row_offset,
            @max_audit_job_id + 1,
            @load_phase,
            @source_schema + '.' + @source_object,
            @source_column,
            @target_schema + '.' + @target_object,
            @target_column,
            @raw_batch_count,
            @stg_batch_count,
            CASE WHEN @raw_batch_count = @stg_batch_count THEN 'Match' ELSE 'Mismatch' END,
            @raw_batch_count - @stg_batch_count,
            @raw_record_count,
            @stg_record_count,
            CASE WHEN @raw_record_count = @stg_record_count THEN 'Match' ELSE 'Mismatch' END,
            @raw_record_count - @stg_record_count
        );

        SET @row_offset += 1;

        FETCH NEXT FROM ref_cursor INTO 
            @load_phase, @source_schema, @source_object, @source_column,
            @filter_query, @target_schema, @target_object, @target_column;
    END;

    CLOSE ref_cursor;
    DEALLOCATE ref_cursor;

    SET @proc_end_time = GETDATE();
    PRINT '>> Proc total Duration: ' + CAST(DATEDIFF(SECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds';
END;

-- Clear previous reconciliation overview records before reloading
TRUNCATE TABLE AUDITING.RECONCILIATION_OVERVIEW;

-- Execute the reconciliation audit procedure
EXEC AUDITING.LOAD_RECONCILIATION_OVERVIEW;

-- Retrieve reconciliation results
SELECT * FROM AUDITING.RECONCILIATION_OVERVIEW;
