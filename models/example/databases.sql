{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.BabyGo_Blinkit_Fact_items AS SELECT distinct concat(mrp,\'-\',item_id,City_name,\'-\',city_id,\'-\',\'-\',CAST(qty_sold as float64),date) as order_id, cast(date as date) order_date, cast(mrp as float64) mrp, city_name as city, item_id as product_id, cast(replace(qty_sold,\'.0\',\'\') as int64) quantity, item_name as product_name FROM `maplemonk.BabyGo_Blinkit_sales_partner_biz` ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            