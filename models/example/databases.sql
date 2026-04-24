{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.beastlife_db_Product_Flipkart_FSN AS SELECT DATE(TIMESTAMP(start_time)) AS DATE, sku_id, product_name, campaign_id, campaign_name, adgroup_id, adgroup_name, \'FLIPKART\' AS Channel, \'FLIPKART ADS\' AS ACCOUNT, SUM(safe_divide(SAFE_CAST(fc.total_revenue__rs__ AS FLOAT64), SAFE_CAST(roi AS FLOAT64))) AS SPEND, sum(safe_CAST(fc.total_revenue__rs__ as float64)) AS conversion_value, sum(safe_cast(direct_units_sold as float64) + safe_Cast(indirect_units_sold as float64)) as conversions, sum(safe_Cast(views as float64)) views, sum(safe_Cast(clicks as float64)) clicks, FROM maplemonk.beastlife_flipkart_seller_portal_consolidated_fsn_pla fc group by 1,2,3,4,5,6,7,8,9;",
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
            