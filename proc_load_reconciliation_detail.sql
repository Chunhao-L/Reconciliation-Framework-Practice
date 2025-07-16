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
        @filter_query NVARCHAR(50),
        @load_phase nvarchar(50),
        @source_schema NVARCHAR(50),
        @source_table NVARCHAR(50),
        @source_id_column   VARCHAR(50),
        @target_schema NVARCHAR(50),
        @target_table       VARCHAR(50),
        @target_id_column   VARCHAR(50),
        @sql             NVARCHAR(MAX);
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

        OPEN metadata_cursor;

        FETCH NEXT FROM metadata_cursor
        INTO @filter_query, @load_phase, @source_schema, @source_table, @source_id_column, @target_schema, @target_table, @target_id_column;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @sql = '
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
                    ' + CAST(@max_audit_id AS NVARCHAR) + '+ ROW_NUMBER() OVER (ORDER BY (SELECT 1)),
                    ' + CAST(@max_audit_job_id AS NVARCHAR) +' + 1,
                    '''+ @load_phase + ''',
                    ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '.batch_id,
                    ''' + @source_table + ''',
                    ''' + @source_id_column +''',
                    ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + '.entity_event_id,
                    ''' + @target_table +''',
                    ''' + @target_id_column + ''',
                    ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + '.entity_event_id,
                    ''Missing in target table''
                FROM
                    ' + QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table) + ' 
                    LEFT JOIN ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table) + ' 
                    ON '+ QUOTENAME(@source_schema) + '.' + QUOTENAME(@source_table)  + '.' + QUOTENAME(@source_id_column) 
                    + ' = ' + QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table)  + '.' + QUOTENAME(@target_id_column) + ' 
                WHERE 
                    ' + @filter_query + '
                    AND '+ QUOTENAME(@target_schema) + '.' + QUOTENAME(@target_table)  + '.' + QUOTENAME(@target_id_column) + ' IS NULL;
            ';

            EXEC sp_executesql @sql;

            FETCH NEXT FROM metadata_cursor
            INTO @filter_query, @load_phase, @source_schema, @source_table, @source_id_column, @target_schema, @target_table, @target_id_column;

        END;
            
        CLOSE metadata_cursor;
        DEALLOCATE metadata_cursor;

END;

TRUNCATE TABLE AUDITING.RECONCILIATION_DETAIL;
EXEC AUDITING.LOAD_RECONCILIATION_DETAIL;
SELECT * FROM AUDITING.RECONCILIATION_DETAIL;