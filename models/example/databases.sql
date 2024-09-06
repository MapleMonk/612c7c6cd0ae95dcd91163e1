{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.Marketing_Consolidated_Global AS select ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,\'FACEBOOK\' CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,sum(TOTAL_OUTBOUND_CLICKS) as Outbound_clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from \"VAHDAM_DB\".\"MAPLEMONK\".FACEBOOK_Global_CONSOLIDATED group by ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT union all select AD_GROUP_NAME ,AD_GROUP_AD_ID ,null as ACCOUNT_NAME ,null as ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,SEGMENTS_DATE ,AD_GROUP_AD_AD_TYPE ,AD_GROUP_AD_AD_STRENGTH ,SEGMENTS_AD_NETWORK_TYPE ,AD_GROUP_AD_AD_FINAL_URLS ,SEGMENTS_DAY_OF_WEEK ,YEAR ,MONTH ,\'GOOGLE\' CHANNEL ,ACCOUNT ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.Global_GOOGLE_ADS_CONSOLIDATED",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from VAHDAM_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            