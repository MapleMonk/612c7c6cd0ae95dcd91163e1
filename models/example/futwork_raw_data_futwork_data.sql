{{ config(
            materialized='table',
                post_hook={
                    "sql": "",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.MAPLEMONK.futwork_raw_data_futwork_data
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            