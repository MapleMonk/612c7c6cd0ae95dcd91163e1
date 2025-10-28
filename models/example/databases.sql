{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table long_tail_revamp as select a.date, a.sku_group,a.image_url, a.type, a.category, a.channel, a.gross_sales, a.gross_quantity, a.price, b.PRODUCT_NAME, b.online_inventory, b.offline_inventory, b.offline_jit_inventory,b.live_date, b.days_since_live, b.days_since_inward from horizontal_sales_categories a left join category_journey b on a.sku_group = b.sku_group where a.category in (\'Shoes\', \'Bags\', \'Perfumes\', \'Sunglasses\', \'Accessories\', \'Belts\', \'Caps\', \'Socks\')",
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
            