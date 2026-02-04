{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.Final_sku_master as select product_code, coalesce(category_name,category_code) as category, scan_identifier as sku_identifier, safe_cast(cost_price as float64) as cost_price, brand, name as product_name, safe_cast(mrp as float64) as mrp from maplemonk.Unicommerce_myhyuman_get_product_master qualify row_number() over(partition by product_code order by datetime(updated) desc) = 1;",
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
            