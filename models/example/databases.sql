{{ config(
            materialized='table',
                post_hook={
                    "sql": "WITH temp_table AS ( SELECT \"Apple\" AS fruit, 100 AS quantity UNION ALL SELECT \"Banana\", 200 UNION ALL SELECT \"Cherry\", 150 ) SELECT * FROM temp_table;",
                    "transaction": true
                }
            ) }}
            with sample_data as (
 
                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            