{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Plaeto_db_Product_GOOGLEADS_CONSOLIDATED AS select NULL AS ADSET_NAME ,NULL AS ADSET_ID ,NULL AS AD_ID ,NULL AS AD_NAME ,\'GOOGLE ADS\' AS ACCOUNT_NAME ,CAST(customer_id AS STRING) AS ACCOUNT_ID ,campaign_name AS CAMPAIGN_NAME ,campaign_id AS CAMPAIGN_ID ,segments_date AS DATE ,NULL AS AD_TYPE ,NULL AS AD_STRENGTH ,campaign_advertising_channel_type AS AD_NETWORK_TYPE ,NULL AS AD_FINAL_URL ,NULL AS DAY_OF_WEEK ,EXTRACT(YEAR FROM cast(segments_date as date)) AS YEAR ,EXTRACT(MONTH FROM cast(segments_date as date)) AS MONTH ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,segments_product_item_id ,SUM(cast (metrics_clicks as FLOAT64)) clicks ,SUM(cast (metrics_cost_micros as FLOAT64))/1000000 spend ,SUM(cast (metrics_impressions as FLOAT64)) Impressions ,SUM(cast (metrics_conversions as FLOAT64)) Conversions ,SUM(cast (metrics_conversions_value as FLOAT64)) Conversion_Value from maplemonk.Plaeto_db_gads_GADS_PRODUCT_LEVEL_SPENDS group by customer_id,campaign_name, campaign_id, segments_date,campaign_advertising_channel_type,segments_product_item_id",
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
            