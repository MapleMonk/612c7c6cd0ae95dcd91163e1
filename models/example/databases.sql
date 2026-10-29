{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS snitch_db.maplemonk.table_for_winter_and_collections_analysis_logs LIKE snitch_db.maplemonk.table_for_winter_and_collections_analysis; ALTER TABLE snitch_db.maplemonk.table_for_winter_and_collections_analysis_logs ADD COLUMN IF NOT EXISTS LOG_DATE DATE; CREATE TABLE IF NOT EXISTS snitch_db.maplemonk.pipeline_for_winter_wear_check_logs LIKE snitch_db.maplemonk.pipeline_for_winter_wear_check; ALTER TABLE snitch_db.maplemonk.pipeline_for_winter_wear_check_logs ADD COLUMN IF NOT EXISTS LOG_DATE DATE;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            