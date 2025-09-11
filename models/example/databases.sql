{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.discount_code_usage as select order_timestamp::date as order_date, discount_code, count(distinct order_name) as orders, sum(gross_sales) as sales, sum(case when lower(discount_code) like \'%rms%\' then 0 else discount end) as discount, sum(quantity) as quantity from snitch_db.maplemonk.fact_items_snitch where discount_code is not null and discount_code not like \'%rms%\' and discount_code not like \'%exchange%\' and discount_code not like \'INFLU%\' group by 1,2 ;",
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
            