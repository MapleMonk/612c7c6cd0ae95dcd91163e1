{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table long_tail_revamp as WITH Product_Names AS ( SELECT sku_group, MAX(PRODUCT_NAME) AS PRODUCT_NAME FROM SNITCH_DB.MAPLEMONK.category_journey GROUP BY sku_group ) SELECT a.date, a.sku_group, a.image_url, a.type, a.category, a.channel, a.gross_sales, a.gross_quantity, a.price, b.inv AS total_daily_inventory, c.PRODUCT_NAME FROM SNITCH_DB.MAPLEMONK.horizontal_sales_categories a LEFT JOIN SNITCH_DB.MAPLEMONK.dod_all_channels_sku_inventory b ON a.date = b.date AND a.sku_group = b.sku_group LEFT JOIN Product_Names c ON a.sku_group = c.sku_group WHERE a.category IN (\'Shoes\', \'Bags\', \'Perfumes\', \'Sunglasses\', \'Accessories\', \'Belts\', \'Caps\', \'Socks\');",
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
            