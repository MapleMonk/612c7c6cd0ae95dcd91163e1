{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.sku_group_ad_inventory_check as with sku_group as ( SELECT CAST(id AS VARCHAR) AS itemid, sku_group FROM ( SELECT *, REVERSE(SUBSTRING(REVERSE(REPLACE(A.value:sku, \'\"\"\', \'\')), CHARINDEX(\'-\', REVERSE(REPLACE(A.value:sku, \'\"\"\', \'\'))) + 1)) AS sku_group, ROW_NUMBER() OVER (PARTITION BY sku_group ORDER BY updated_at DESC) AS rw FROM snitch_db.maplemonk.shopify_all_products, LATERAL FLATTEN (input => variants) A ) WHERE rw = 1 ), itemid_clicks as ( select to_date(a.date,\'YYYYMMDD\') as ga_date, \'app\' as type, a.itemId, b.sku_group, a.sessionSourceMedium as source, a.itemsViewed as clicks, a.sessions from snitch_db.maplemonk.itemid_sourcemedium_clicks_sessions_app a left join sku_group b on a.itemId=b.itemid union select to_date(a.date,\'YYYYMMDD\') as ga_date, \'web\' as type, a.itemId, b.sku_group, a.sessionSourceMedium as source, a.itemsViewed as clicks, a.sessions from snitch_db.maplemonk.itemid_sourcemedium_clicks_sessions_web a left join sku_group b on a.itemId=b.itemid ), inventory as ( select sku_group, product_name, case when category in (\'Shirt\', \'Shirts\') then \'Shirts\' when category = \'Denim\' then \'Jeans\' else category end as new_category, available_units, price, sales_last_7_days, sales_last_15_days, sales_last_30_days, from snitch_db.maplemonk.availability_master_v2 where sku_class not in (\'Draft\',\'Not-Cataloged\') ) select a.*,b.product_name,b.new_category,b.available_units,b.price,b.sales_last_7_days,b.sales_last_15_days,b.sales_last_30_days from itemid_clicks a left join inventory b on a.sku_group=b.sku_group",
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
            