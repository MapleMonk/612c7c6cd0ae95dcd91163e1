{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE gs_bad_sheet1_final AS SELECT g.*, CASE WHEN g.\"Ticket No\" IS NULL OR TRIM(TO_VARCHAR(g.\"Ticket No\")) = \'\' THEN \'not raised\' WHEN TRY_TO_NUMBER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) IS NOT NULL THEN \'raised\' WHEN UPPER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) LIKE \'%RAISED%\' THEN \'raised\' ELSE \'not eligible\' END AS main_base, CASE WHEN UPPER(TRIM(g.\"Value diffrence Action\")) = \'CLAIM NOT REQUIRED\' AND ( CASE WHEN g.\"Ticket No\" IS NULL OR TRIM(TO_VARCHAR(g.\"Ticket No\")) = \'\' THEN \'not raised\' WHEN TRY_TO_NUMBER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) IS NOT NULL THEN \'raised\' WHEN UPPER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) LIKE \'%RAISED%\' THEN \'raised\' ELSE \'not eligible\' END ) <> \'raised\' THEN \'not eligible\' ELSE CASE WHEN g.\"Ticket No\" IS NULL OR TRIM(TO_VARCHAR(g.\"Ticket No\")) = \'\' THEN \'not raised\' WHEN TRY_TO_NUMBER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) IS NOT NULL THEN \'raised\' WHEN UPPER(TRIM(TO_VARCHAR(g.\"Ticket No\"))) LIKE \'%RAISED%\' THEN \'raised\' ELSE \'not eligible\' END END AS main FROM gs_bad_sheet1 g;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            