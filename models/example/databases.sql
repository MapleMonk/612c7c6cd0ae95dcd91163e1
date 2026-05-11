{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace TABLE prolicious-wh.MapleMonk.prolicious_blinkit_ads_Fact_items AS select \'BLINKIT ADS\' ACCOUNT_NAME ,NULL AS ACCOUNT_ID ,cast(upper(Campaign_Name) as string) CAMPAIGN_NAME ,NULL AS CAMPAIGN_ID ,NULL AS ADSET_NAME ,NULL AS ADSET_ID ,NULL AS AD_ID ,NULL AS AD_NAME ,date(parse_date(\'%d-%m-%Y\', Date)) AS formatted_date ,NULL AS AD_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_FINAL_URL ,cast(NULL AS string) Day_of_Week ,EXTRACT(YEAR FROM parse_DATE(\'%d-%m-%Y\', Date)) AS YEAR ,EXTRACT(MONTH FROM parse_DATE(\'%d-%m-%Y\', Date)) AS MONTH ,\'BLINKIT ADS\' CHANNEL ,cast(NULL as string) AS ACCOUNT ,NULL AS Clicks ,cast(Estimated_Budget_Consumed as float64) AS spend ,cast(Impressions as float64) AS impressions ,cast(NULL as float64) AS conversions ,cast(NULL as float64) conversion_value ,cast(Direct_ATC as float64) + cast(Indirect_ATC as float64) as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from prolicious-wh.maplemonk.Prolicious_Blinkit_ads ;",
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
            