{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.l1_category_horizontal_sales as ( with l1_category as ( select distinct sku_group, l1_Category, category from snitch_db.maplemonk.category_journey ) select a.sku_group, a.type, a.date, b.category, a.gross_quantity, a.gross_sales, b.l1_category from snitch_db.maplemonk.horizontal_sales_categories a left join l1_category b on upper(trim(a.sku_group)) = upper(trim(b.sku_group)) );",
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
            