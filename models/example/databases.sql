{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.discount_code_usage_metrics as ( with main as ( select *, case when lower(ifnull(discount_code,\'others\')) like \'%eco%\' or lower(ifnull(discount_code,\'others\')) like \'%influ%\' or lower(ifnull(discount_code,\'others\')) like \'%rms%\' or tags = \'FLITS_LOGICERP\' or lower(ifnull(discount_code,\'others\')) like \'%creator%\' then 1 else 0 end as filters from snitch_db.maplemonk.fact_items_snitch where order_status = \'Shopify_Processed\' ), agg as ( select order_timestamp::date as order_date, discount_code, sum(gross_sales)::int as gross_sales, sum(quantity)::int as gross_quantity, sum(case when lower(discount_code) is not null and lower(discount_code) like \'%rms%\' then 0 else discount end)::int as discount, count(distinct order_name) as gross_orders, (sum(gross_sales)/count(distinct combined_order_name))::int as gross_aov from main where filters = 0 group by 1,2 ) select *, sum(gross_sales) over (partition by order_date) as day_gross_sales, gross_sales / nullif(sum(gross_sales) over (partition by order_date), 0) as sales_share from agg order by order_date, sales_share desc );",
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
            