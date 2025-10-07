{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER TABLE TRUEGRADIENT_FORECAST_ALL_CHANNEL MODIFY COLUMN pred FLOAT;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EGGOZDB.MAPLEMONK.Truegradient_forecast_all_channel
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            