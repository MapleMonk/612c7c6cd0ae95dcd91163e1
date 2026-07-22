{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.GA4_PRODUCT_LEVEL_FUNNEL AS select SPLIT(itemId, \'_\')[SAFE_OFFSET(2)] as item_id, itemName, coalesce(v.style,p.style) as style, cast(startDate as date) as date, \'WEBSITE\' AS source, sum(cast(itemsViewed as int64)) as items_viewed, sum(cast(itemsPurchased as int64)) as items_purchased, sum(cast(itemsCheckedOut as int64)) as checked_out, sum(cast(itemsAddedToCart as int64)) as add_to_carts, sum(cast(itemsViewedInList as int64)) as items_viewed_in_list, sum(cast(itemsClickedInList as int64)) as items_clicked from (select * from maplemonk.ga4_website_item__level__funnel qualify row_number() over (partition by itemId,endDate,startDate,itemName order by datetime(_airbyte_normalized_at) desc) =1) i left join (select distinct variant_id,style from maplemonk.goodtribe_wh_shopify_fact_items qualify row_number() over (partition by variant_id order by 1)=1 ) V on SPLIT(itemId, \'_\')[SAFE_OFFSET(2)] = v.variant_id OR SPLIT(itemId, \'_\')[SAFE_OFFSET(3)] = v.variant_id left join (select distinct PRODUCT_id,style from maplemonk.goodtribe_wh_shopify_fact_items qualify row_number() over (partition by PRODUCT_id order by 1)=1 ) p on SPLIT(itemId, \'_\')[SAFE_OFFSET(2)] = p.product_id OR SPLIT(itemId, \'_\')[SAFE_OFFSET(3)] = p.product_id group by 1,2,3,4 union all select itemId as item_id, itemName, coalesce(v.style,p.style) as style, cast(startDate as date) as date, \'APP\' AS source, sum(cast(itemsViewed as int64)) as items_viewed, sum(cast(itemsPurchased as int64)) as items_purchased, sum(cast(itemsCheckedOut as int64)) as checked_out, sum(cast(itemsAddedToCart as int64)) as add_to_carts, sum(cast(itemsViewedInList as int64)) as items_viewed_in_list, sum(cast(itemsClickedInList as int64)) as items_clicked from (select * from maplemonk.ga4_app_item__level__funnel qualify row_number() over (partition by itemId,endDate,startDate,itemName order by datetime(_airbyte_normalized_at) desc) =1 ) i left join (select distinct variant_id,style from maplemonk.goodtribe_wh_shopify_fact_items qualify row_number() over (partition by variant_id order by 1)=1 ) V on i.itemid = v.variant_id left join (select distinct PRODUCT_id,style from maplemonk.goodtribe_wh_shopify_fact_items qualify row_number() over (partition by PRODUCT_id order by 1)=1 ) p on i.itemid = p.product_id group by 1,2,3,4",
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
            