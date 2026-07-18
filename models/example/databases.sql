{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.store_zone_map AS SELECT branch_code, zone FROM ( SELECT branch_code::VARCHAR AS branch_code, UPPER(zone) AS zone, ROW_NUMBER() OVER (PARTITION BY branch_code::VARCHAR ORDER BY zone) AS rn FROM snitch_db.maplemonk.store_category_delta WHERE UPPER(zone) IN (\'NORTH\', \'SOUTH\') ) WHERE rn = 1;",
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
            