{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.healthy_master_bigbasket_fact_items as select cast(total_mrp as float64) as mrp, date(PARSE_DATE(\'%Y%m%d\', SPLIT(date_range, \' - \')[OFFSET(0)])) as order_date, cast(total_sales as float64) as selling_price, cast(replace(total_quantity,\'.0\',\'\') as int64) as quantity, source_city_name as city, replace(replace(upper(sku_description),\'HEALTHY MASTER \',\'\'),\'-\',\' \') as product_name, upper(replace(replace(leaf_slug,\'snacks\',\'\'),\'-\',\' \')) as product_category from maplemonk.HM_BigBasket_analytics_manufacturer_sales UNION ALL select cast(total_mrp as float64) as mrp, date(PARSE_DATE(\'%Y%m%d\', SPLIT(date_range, \' - \')[OFFSET(0)])) as order_date, cast(total_sales as float64) as selling_price, cast(replace(total_quantity,\'.0\',\'\') as int64) as quantity, source_city_name as city, replace(replace(upper(sku_description),\'HEALTHY MASTER \',\'\'),\'-\',\' \') as product_name, upper(replace(replace(leaf_slug,\'snacks\',\'\'),\'-\',\' \')) as product_category from maplemonk.Healthy_master_db_analytics_manufacturer_sales;",
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
            