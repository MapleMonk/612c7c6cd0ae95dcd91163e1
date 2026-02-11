{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Carlton_London_Blinkit_Fact_items AS SELECT distinct concat(mrp,\'-\',item_id,City_name,\'-\',city_id,\'-\',\'-\',CAST(qty_sold as float64),date) as order_id, cast(date as date) order_date, cast(mrp as float64) mrp, city_name as city, item_id as product_id, cast(replace(qty_sold,\'.0\',\'\') as int64) quantity, item_name as product_name, coalesce(bs.category) as product_category, sm.name as product_name_final, sm.product_sub_category, sm.combo as combo, sm.master_sku as commonsku FROM `maplemonk.Blinkit_Carlton_London_sales_partner_biz` bs LEFT JOIN (SELECT blinkit_sku, master_sku, product_name as name, combo, sub_category as product_sub_category from maplemonk.carlton_london_sku_master qualify row_number() over (partition by blinkit_sku order by master_sku) = 1 ) sm on upper(sm.blinkit_sku) = upper(COALESCE(bs.item_id)) ;",
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
            