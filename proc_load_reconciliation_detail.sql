/*
====================================================================================================
Procedure: AUDITING.LOAD_RECONCILIATION_DETAIL
----------------------------------------------------------------------------------------------------
Purpose:
--------
This stored procedure performs a detailed reconciliation audit at the record level by identifying
source records that are missing in the corresponding target tables. It inserts detailed mismatch
records into `AUDITING.RECONCILIATION_DETAIL`.

Features:
---------
- Iterates over reconciliation references from `AUDITING.RECONCILIATION_REFERENCE` where
  `source_column` = 'entity_event_id'.
- Dynamically constructs and executes SQL to find source records with no matching target records.
- Uses a cursor to process multiple reconciliation scenarios.
- Inserts results into a detailed audit table with unique audit_id and audit_job_id.

How to Use:
-----------
1. Ensure `AUDITING.RECONCILIATION_REFERENCE` is populated with appropriate source/target table metadata
   and filters.
2. Run the procedure using: `EXEC AUDITING.LOAD_RECONCILIATION_DETAIL`.
3. Query `AUDITING.RECONCILIATION_DETAIL` to review detailed mismatches.
4. Optionally truncate `RECONCILIATION_DETAIL` before rerunning to clear previous audit details.

Note:
-----
- Assumes presence of `entity_event_id` and `batch_id` columns in source and target tables.
- Designed to capture only missing records in target tables; matches or mismatches in values are not handled here.
- Uses dynamic SQL and cursors, so performance depends on the size of source and target tables.

====================================================================================================
*/

USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE AUDITING.LOAD_RECONCILIATION_DETAIL AS
BEGIN
    SET NOCOUNT ON;  -- Prevent extra result sets from interfering with output

    -- Declare variables to track procedure start/end time and current max audit IDs
    DECLARE
        @proc_start_time DATETIME = GETDATE(), -- Record start time of procedure
        @proc_end_time DATETIME,                -- Will hold end time
        @max_audit_id INT = ISNULL(
            (SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_DETAIL),
            0
        ), -- Get current max audit_id, default to 0 if table empty
        @max_audit_job_id INT = ISNULL(
            (SELECT MAX(audit_job_id) FROM AUDITING.RECONCILIATION_DETAIL),
            0
        ), -- Get current max audit_job_id, default to 0 if empty
        @filter_query NVARCHAR(50),             -- Filter condition from metadata table
        @load_phase NVARCHAR(50),               -- Load phase identifier
        @source_schema NVARCHAR(50),            -- Source schema name
        @source_table NVARCHAR(50),             -- Source table name
        @source_id_column VARCHAR(50),          -- Source column used for matching (entity_event_id)
        @target_schema NVARCHAR(50),            -- Target schema name
        @target_table VARCHAR(50),              -- Target table name
        @target_id_column VARCHAR(50),          -- Target column used for matching
        @sql NVARCHAR(MAX);                     -- Dynamic SQL command text

    -- Cursor to iterate over reconciliation reference metadata for entity_event_id
    DECLARE metadata_cursor CURSOR FOR
        SELECT 
            filter_query,
            load_phase,
            source_schema,
            source_object,
            source_column,
            target_schema,
            target_object,
            target_column
        FROM
            AUDITING.RECONCILIATION_REFERENCE
        WHERE 
            source_column = 'entity_event_id';

    OPEN metadata_cursor; -- Open the cursor for fetching

    -- Fetch the first row from cursor
    FETCH NEXT FROM metadata_cursor
    INTO @filter_query, @load_phase, @source_schema, @source_table, @source_id_column, @target_schema, @target_table, @target_id_column;

    WHILE @@FETCH_STATUS = 0 -- Loop while there are rows to fetch
    BEGIN
        -- Build dynamic SQL string for inserting missing records from source not found in target
        SET @sql = '
            INSERT INTO AUDITING.RECONCILIATION_DETAIL (
                audit_id,
                audit_job_id,
                load_phase,
                batch_id,
                source_object,
                source_column,
                source_value,
                target_object,
                target_column,
                target_value,
                audit_result
            )
            SELECT 
                ' + CAST(@max_audit_id AS NVARCHAR) + ' + ROW_NUMBER() OVER (ORDER BY (SELECT 1)),
                ' + CAST(@max_audit_job_id AS NVARCHAR) + ' + 1, 
                ''' + @load_phase + ''', 
                ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '.batch_id, 
                ''' + @source_table + ''',  
                ''' + @source_id_column + ''',  
                ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '.entity_event_id, 
                ''' + @target_table + ''', 
                ''' + @target_id_column + ''',  
                ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + '.entity_event_id, 
                ''Missing in target table''  
            FROM
                ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '
                LEFT JOIN ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + '
                ON ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '.' + QUOTENAME(@source_id_column) + ' = ' 
                    + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + '.' + QUOTENAME(@target_id_column) + '
            WHERE 
                ' + @filter_query + '  
                AND ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + '.' + QUOTENAME(@target_id_column) + ' IS NULL;  
        ';

        EXEC sp_executesql @sql; -- Execute the constructed dynamic SQL

        --Update the maximum audit id in each loop
        SET @max_audit_id = ISNULL(
                (SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_DETAIL),
                0
        );

        -- Fetch next row from cursor
        FETCH NEXT FROM metadata_cursor
        INTO @filter_query, @load_phase, @source_schema, @source_table, @source_id_column, @target_schema, @target_table, @target_id_column;
    END;
        
    CLOSE metadata_cursor;       -- Close the cursor after processing
    DEALLOCATE metadata_cursor;  -- Release resources held by cursor

    SET @proc_end_time = GETDATE(); -- Capture procedure end time
    PRINT '>> Proc total Duration: ' + CAST(DATEDIFF(SECOND, @proc_start_time, @proc_end_time) AS NVARCHAR) + ' seconds'; -- Print duration
END;

-- Clear any previous reconciliation detail data before loading fresh audit info
TRUNCATE TABLE AUDITING.RECONCILIATION_DETAIL;

-- Execute the reconciliation detail loading procedure
EXEC AUDITING.LOAD_RECONCILIATION_DETAIL;

-- Select all rows from the reconciliation detail table for review
SELECT * FROM AUDITING.RECONCILIATION_DETAIL;
