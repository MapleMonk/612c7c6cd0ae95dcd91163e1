{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.store_wise_daily_orders as select SKU, SKU_GROUP, category, coalesce(sum(suborder_quantity),0) as total_units_sold, coalesce(sum(selling_price),0) as total_sales, coalesce(sum(return_quantity),0) as total_return_quantity, coalesce(sum(discount),0) as total_discount, order_date, extract (month from order_date) as month, SUBSTRING(sku, LEN(sku) - CHARINDEX(\'-\', REVERSE(sku)) + 2, LEN(sku)) AS size FROM snitch_db.MAPLEMONK.UNICOMMERCE_FACT_ITEMS_SNITCH WHERE source in (\'POS1\') GROUP BY sku, sku_group, order_date, month,category, size",
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
                        