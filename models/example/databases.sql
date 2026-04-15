{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Fytika_db_Product_Flipkart_FSN AS SELECT fc.sku_id AS PRODUCT_ID, DATE(TIMESTAMP(start_time)) AS DATE, \'FLIPKART\' AS Channel, \'FLIPKART ADS\' AS ACCOUNT, coalesce(pl.sku_code,pp.sku_code) AS SKU, pm.name as product_name_final, pm.category_name AS PRODUCT_CATEGORY, SUM(safe_divide(SAFE_CAST(fc.total_revenue__rs__ AS FLOAT64), SAFE_CAST(roi AS FLOAT64))) AS SPEND, sum(safe_CAST(fc.total_revenue__rs__ as float64)) AS SALES FROM `maplemonk.Fytika_flipkart_ads___fsn_seller_portal_consolidated_fsn_pla` fc left join ( select distinct sku_code, channel_product_id from maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing qualify row_number() over (partition by channel_product_id order by date(updated) desc) = 1 ) pl on pl.channel_product_id = REPLACE(fc.sku_id, \'\"\', \'\') left join ( select distinct sku_code, seller_sku_on_channel from maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_listing qualify row_number() over (partition by seller_sku_on_channel order by date(updated) desc) = 1 ) pp on pp.seller_sku_on_channel = REPLACE(fc.sku_id, \'\"\', \'\') left join ( select product_code, name, category_name, mrp, from maplemonk-analytics.maplemonk.unicommerce_fytika_get_product_master qualify row_number() over (partition by product_code order by date(updated) desc) = 1 ) pm on pm.product_code = coalesce(pl.sku_code,pp.sku_code,REPLACE(fc.sku_id, \'\"\', \'\')) group by 1,2,3,4,5,6,7;",
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
            