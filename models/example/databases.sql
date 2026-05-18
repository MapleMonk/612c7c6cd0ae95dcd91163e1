{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM snitch_db.maplemonk.hit_product_info_inv_logs WHERE log_date = CURRENT_DATE() ; INSERT INTO snitch_db.maplemonk.hit_product_info_inv_logs SELECT CURRENT_DATE() AS log_date, CURRENT_TIMESTAMP() AS run_timestamp, a.* FROM snitch_db.maplemonk.hit_product_info_inv a ;",
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
            