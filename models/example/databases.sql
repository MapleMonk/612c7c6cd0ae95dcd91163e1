{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.sirona_bigbasket_fact_items as select cast(total_mrp as float64) as mrp, date(PARSE_DATE(\'%Y%m%d\', SPLIT(date_range, \' - \')[OFFSET(0)])) as order_date, CONCAT(date(PARSE_DATE(\'%Y%m%d\', SPLIT(date_range, \' - \')[OFFSET(0)])),\'_\',source_city_name,\'_\',total_sales) AS order_id, CAST(source_sku_id AS STRING) AS SKU, cast(total_sales as float64) as selling_price, cast(replace(total_quantity,\'.0\',\'\') as int64) as quantity, source_city_name as city, replace(replace(upper(sku_description),\'SIRONA\',\'\'),\'-\',\' \') as product_name, upper(replace(replace(leaf_slug,\'snacks\',\'\'),\'-\',\' \')) as product_category FROM Maplemonk.BigBasket_sirona_db_analytics_manufacturer_sales",
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
            