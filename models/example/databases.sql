{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM production_pipeline_data_v2_logs WHERE log_date = CURRENT_DATE(); INSERT INTO production_pipeline_data_v2_logs SELECT *, CURRENT_TIMESTAMP() AS log_time FROM production_pipeline_data_v2;",
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
            