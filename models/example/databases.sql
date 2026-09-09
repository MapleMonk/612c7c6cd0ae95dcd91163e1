{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table pronk-wh.maplemonk.sku_1st_sale as select distinct a.product_id, trim(upper(a.sku)) as SKU, trim(upper(a.product_name)) as product_name, date(trim(upper(a.created_at))) as SKU_created_date, min(b.Order_Date) as SKU_min_sale from `maplemonk.pronk_product_master` a left join `maplemonk.pronk_sales_consolidated` b on trim(upper(a.product_id)) = trim(upper(b.product_id)) group by 1,2,3,4;",
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
            