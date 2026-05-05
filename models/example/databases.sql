{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace TABLE prolicious-wh.MapleMonk.prolicious_swiggy_ads_Fact_items AS select \'SWIGGY ADS\' ACCOUNT_NAME ,cast(ACCOUNT_ID as string) ACCOUNT_ID ,cast(upper(CAMPAIGN_NAME) as string) CAMPAIGN_NAME ,cast(CAMPAIGN_ID as string) CAMPAIGN_ID ,NULL AS ADSET_NAME ,NULL AS ADSET_ID ,NULL AS AD_ID ,NULL AS AD_NAME ,CAMPAIGN_START_DATE AS Date ,NULL AS AD_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_FINAL_URL ,NULL AS Day_of_Week ,EXTRACT(YEAR FROM cast(CAMPAIGN_START_DATE AS Date)) AS YEAR ,EXTRACT(MONTH FROM cast(CAMPAIGN_START_DATE AS Date)) AS MONTH ,\'SWIGGY ADS\' CHANNEL ,cast(NULL as string) AS ACCOUNT ,TOTAL_CLICKS AS Clicks ,cast(TOTAL_BUDGET as float64) AS spend ,cast(TOTAL_IMPRESSIONS as float64) AS impressions ,cast(TOTAL_CONVERSIONS as float64) AS conversions ,cast(TOTAL_GMV as float64) conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from prolicious-wh.maplemonk.Prolicious_Swiggy_campaign_x_report ;",
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
            