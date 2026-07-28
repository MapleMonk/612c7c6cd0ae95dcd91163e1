{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.store_zone_map AS SELECT branch_code, store_name, zone, cluster_name FROM ( SELECT s.branch_code::VARCHAR AS branch_code, s.branch_name::VARCHAR AS store_name, UPPER(s.zone) AS zone, m.cluster::VARCHAR AS cluster_name, ROW_NUMBER() OVER ( PARTITION BY s.branch_code::VARCHAR ORDER BY UPPER(s.zone) ) AS rn FROM snitch_db.maplemonk.store_category_delta s LEFT JOIN snitch_db.maplemonk.offline_master m ON s.branch_code::VARCHAR = m.branch_code::VARCHAR WHERE UPPER(s.zone) IN (\'NORTH\', \'SOUTH\') ) WHERE rn = 1;",
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
            