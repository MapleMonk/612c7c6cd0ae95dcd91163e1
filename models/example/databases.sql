{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE thd-pharma-wh.MAPLEMONK.thd_pharma_nykaa_ads_fact_items AS select \'NYKAA ADS\' ACCOUNT_NAME ,cast(null as string) ACCOUNT_ID ,cast(upper(ad_request_name) as string) CAMPAIGN_NAME ,cast(null as string) CAMPAIGN_ID ,cast(null as string) ADSET_NAME ,cast(null as string) ADSET_ID ,cast(null as string) AD_ID ,cast(null as string) AD_NAME ,cast(ad_live_date as date) as DATE ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,cast(null as string) Day_of_Week ,EXTRACT(YEAR FROM cast(ad_live_date as date)) AS YEAR ,EXTRACT(MONTH FROM cast(ad_live_date as date)) AS MONTH ,\'NYKAA ADS\' CHANNEL ,cast(null as string) ACCOUNT ,Clicks ,cast(spends as float64) Spend ,cast(Impressions as float64) Impressions ,cast(revenue as float64) Conversions ,cast(revenue as float64) Conversion_Value ,NULL AS Add_to_carts ,NULL AS Add_to_cart_value ,NULL AS Landing_page_views ,NULL AS Initiate_checkouts ,NULL AS Initiate_checkouts_value from thd-pharma-wh.MAPLEMONK.Pharma_Nykaa_Ads_ads_report ;",
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
            