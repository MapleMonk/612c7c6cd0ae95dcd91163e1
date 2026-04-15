{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Fytika_db_Product_GOOGLEADS_CONSOLIDATED AS select segments_product_item_id product_id ,segments_date AS DATE ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,coalesce(d.sku, e.sku, f.sku, g.sku) AS SKU ,pm.name as product_name_final ,pm.category_name AS PRODUCT_CATEGORY ,SUM(cast (metrics_cost_micros as FLOAT64))/1000000 spend ,SUM(cast (metrics_conversions_value as FLOAT64)) sales from maplemonk-analytics.maplemonk.Fytika_google_ads_product_GADS_PRODUCT_LEVEL_SPENDS a left join ( SELECT LOWER(TRIM(SPLIT(channel_product_id, \'-\')[SAFE_OFFSET(1)])) AS product_id, LOWER(TRIM(sku_code)) AS SKU FROM `maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing` WHERE channelname = \'SHOPIFY\' QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(SPLIT(channel_product_id, \'-\')[SAFE_OFFSET(1)])) ORDER BY DATE(updated) DESC) = 1 ) d on lower(a.segments_product_item_id) = lower(d.product_id) left join ( SELECT LOWER(TRIM(SPLIT(channel_product_id, \'-\')[SAFE_OFFSET(2)])) AS variant_id, LOWER(TRIM(sku_code)) AS SKU FROM `maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing` WHERE channelname = \'SHOPIFY\' QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(TRIM(SPLIT(channel_product_id, \'-\')[SAFE_OFFSET(2)])) ORDER BY DATE(updated) DESC) = 1 ) e on lower(a.segments_product_item_id) = lower(e.variant_id) left join ( SELECT LOWER(replace(channel_product_id,\'-\',\'\')) AS product_id, LOWER(TRIM(sku_code))AS SKU FROM `maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing` WHERE channelname = \'SHOPIFY\' QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(replace(channel_product_id,\'-\',\'\')) ORDER BY DATE(updated) DESC) = 1 ) f on lower(replace(replace(a.segments_product_item_id,\'shopify_in\',\'\'),\'_\',\'\')) = lower(f.product_id) left join ( SELECT LOWER(seller_sku_on_channel) AS product_id, LOWER(TRIM(sku_code)) AS SKU FROM `maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing` WHERE channelname = \'SHOPIFY\' QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(seller_sku_on_channel) ORDER BY DATE(updated) DESC) = 1 ) g on lower(a.segments_product_item_id) = lower(g.product_id) left join ( select product_code, name, category_name, mrp, from maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_master qualify row_number() over (partition by product_code order by date(updated) desc) = 1 ) pm on lower(pm.producT_code) = lower(coalesce( d.sku, e.sku,f.sku, g.sku, a.segments_product_item_id)) group by 1,2,3,4,5,6,7 ;",
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
            