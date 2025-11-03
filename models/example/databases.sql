{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table long_tail_revamp as WITH Product_Names AS ( SELECT sku_group, MAX(PRODUCT_NAME) AS PRODUCT_NAME FROM SNITCH_DB.MAPLEMONK.category_journey GROUP BY sku_group ) SELECT a.date, a.sku_group, a.image_url, a.type, a.category, a.channel, a.gross_sales, a.gross_quantity, a.price, b.inv AS total_daily_inventory, c.PRODUCT_NAME FROM SNITCH_DB.MAPLEMONK.horizontal_sales_categories a LEFT JOIN SNITCH_DB.MAPLEMONK.dod_all_channels_sku_inventory b ON a.date = b.date AND a.sku_group = b.sku_group LEFT JOIN Product_Names c ON a.sku_group = c.sku_group WHERE a.category IN (\'Shoes\', \'Bags\', \'Perfumes\', \'Sunglasses\', \'Accessories\', \'Belts\', \'Caps\', \'Socks\') or a.SKU_GROUP IN ( \'SN0113-01\', \'SN0113-02\', \'SN0113-03\', \'SN0113-04\', \'SN0113-05\', \'SN0113-06\', \'SN0114-01\', \'SN0114-02\', \'SN0114-03\', \'SN0114-04\', \'SN0114-05\', \'SN0114-06\', \'SN0115-01\', \'SN0115-02\', \'SN0115-03\', \'SN0115-04\', \'SN0116-01\', \'SN0116-02\', \'SN0117-01\', \'SN0117-02\', \'SN0117-03\', \'SN0118-01\', \'SN0118-02\', \'SN0118-03\', \'SN0119-01\', \'SN0119-02\', \'SN0119-03\', \'SN0119-04\', \'SN0119-05\', \'SN0120-01\', \'SN0120-02\', \'SN0F120-03\' ) or a.sku_group IN ( \'BP0015-01\', \'BP0015-02\', \'BP0015-03\', \'BP0016-01\', \'BP0016-02\', \'BP0017-01\', \'BP0017-02\', \'BP0017-03\', \'BP0017-04\', \'BP0018-01\', \'BP0018-02\', \'BP0019-01\', \'BP0019-02\', \'BP0019-03\');",
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
            