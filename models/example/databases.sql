{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.inwards_freq_multiplier_eoq as with sku_inward_dates as ( select sku_group, count(*) as inwards_freq, datediff(day,max(inward_date),current_date) as days_since_last_inward from snitch_db.maplemonk.inwards_date_and_quantity_eoq group by 1 ), confidence_multiplier as ( select sku_group, inwards_freq, days_since_last_inward, case when days_since_last_inward <= 120 then 1.0 when days_since_last_inward <= 240 then 0.9 when days_since_last_inward <= 360 then 0.8 else 0.6 end as confidence_multiplier, case when inwards_freq = 1 then 0.8 when inwards_freq = 2 then 0.9 else 1 end as inward_freq_multiplier from sku_inward_dates ) select *,(confidence_multiplier+inward_freq_multiplier)/2 as inwards_multiplier from confidence_multiplier order by confidence_multiplier asc;",
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
            