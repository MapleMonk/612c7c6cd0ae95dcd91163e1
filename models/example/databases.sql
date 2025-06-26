{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.inwards_freq_multiplier_eoq as with sku_inward_dates as ( select sku_group, count(*) as inwards_freq, datediff(day,max(inward_date),current_date) as days_since_last_inward from snitch_db.maplemonk.inwards_date_and_quantity_eoq group by 1 ), confidence_multiplier as ( select sku_group, inwards_freq, days_since_last_inward, case when days_since_last_inward < 120 and inwards_freq = 1 then 1 when days_since_last_inward < 120 and inwards_freq = 2 then 1 when days_since_last_inward < 120 and inwards_freq > 2 then 1 when days_since_last_inward >= 120 and inwards_freq = 1 then 0.7 when days_since_last_inward >= 120 and inwards_freq = 2 then 0.9 when days_since_last_inward >= 120 and inwards_freq > 2 then 1 when days_since_last_inward >= 240 and inwards_freq = 1 then 0.5 when days_since_last_inward >= 240 and inwards_freq = 2 then 0.7 when days_since_last_inward >= 240 and inwards_freq > 2 then 0.9 when days_since_last_inward >= 360 and inwards_freq = 1 then 0.3 when days_since_last_inward >= 360 and inwards_freq = 2 then 0.7 when days_since_last_inward >= 360 and inwards_freq > 2 then 0.9 else 1 end as inward_freq_multiplier from sku_inward_dates ) select *,inward_freq_multiplier as inwards_multiplier from confidence_multiplier ;",
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
            