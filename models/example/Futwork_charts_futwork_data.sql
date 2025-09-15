{{ config(
            materialized='table',
                post_hook={
                    "sql": "",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.MAPLEMONK.Futwork_charts_futwork_data
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            