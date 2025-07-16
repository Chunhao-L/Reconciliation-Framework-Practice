USE TheDataGuyzPractice;
GO

CREATE OR ALTER PROCEDURE AUDITING.LOAD_RECONCILIATION_OVERVIEW
AS
BEGIN

    DECLARE 
        @proc_start_time DATETIME = GETDATE(),
        @proc_end_time DATETIME,
        @max_audit_id INT = ISNULL((SELECT MAX(audit_id) FROM AUDITING.RECONCILIATION_OVERVIEW), 0),
        @max_audit_job_id INT = ISNULL((SELECT MAX(audit_job_id) FROM AUDITING.RECONCILIATION_OVERVIEW), 0),
        @load_phase NVARCHAR(50),
        @source_schema NVARCHAR(50),
        @source_object NVARCHAR(50),
        @source_column NVARCHAR(50),
        @filter_query NVARCHAR(MAX),
        @target_schema NVARCHAR(50),
        @target_object NVARCHAR(50),
        @target_column NVARCHAR(50),
        @raw_batch_count INT,
        @stg_batch_count INT,
        @raw_record_count INT,
        @stg_record_count INT,
        @sql NVARCHAR(MAX),
        @row_offset INT = 1;

    DECLARE ref_cursor CURSOR FOR
        SELECT DISTINCT 
            load_phase, source_schema, source_object, source_column,
            filter_query, target_schema, target_object, target_column
        FROM AUDITING.RECONCILIATION_REFERENCE
        WHERE source_column = 'entity_event_id';

    SET @proc_start_time = GETDATE();
    PRINT '>> Starting the reconciliation overview procedure.';
    OPEN ref_cursor;
    FETCH NEXT FROM ref_cursor INTO 
        @load_phase, @source_schema, @source_object, @source_column,
        @filter_query, @target_schema, @target_object, @target_column;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @sql = '
            SELECT 
                @raw_batch_count_out = COUNT(DISTINCT batch_id),
                @raw_record_count_out = COUNT(' + QUOTENAME(@source_column) + ')
            FROM ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_object) + '
            WHERE ' + @filter_query + ';';

        EXEC sp_executesql @sql,
            N'@raw_batch_count_out INT OUTPUT, @raw_record_count_out INT OUTPUT',
            @raw_batch_count OUTPUT, @raw_record_count OUTPUT;

        SET @sql = '
            SELECT 
                @stg_batch_count_out = COUNT(DISTINCT batch_id),
                @stg_record_count_out = COUNT(' + QUOTENAME(@target_column) + ')
            FROM ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_object) + ';';

        EXEC sp_executesql @sql,
            N'@stg_batch_count_out INT OUTPUT, @stg_record_count_out INT OUTPUT',
            @stg_batch_count OUTPUT, @stg_record_count OUTPUT;

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


TRUNCATE TABLE AUDITING.RECONCILIATION_OVERVIEW;
EXEC AUDITING.LOAD_RECONCILIATION_OVERVIEW;
SELECT * FROM AUDITING.RECONCILIATION_OVERVIEW;