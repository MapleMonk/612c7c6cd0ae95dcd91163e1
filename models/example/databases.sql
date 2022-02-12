{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.MARKETING_CONSOLIDATED AS select ADNAME,ADID,ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL, FACEBOOK_ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from VAHDAM_DB.MAPLEMONK.FACEBOOK_US_CONSOLIDATED group by ADNAME,ADID,ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION select NULL,NULL,"ad_group.name","ad_group_ad.ad.id",NULL,NULL ,"campaign.name", "campaign.id","segments.date" ,YEAR("segments.date") AS YEAR ,MONTH("segments.date") AS MONTH ,'Google Ads' Channel, NULL ,SUM("metrics.clicks") Clicks ,SUM("metrics.cost_micros") Spend,SUM("metrics.impressions") Impressions ,SUM("metrics.conversions") Conversions,SUM("metrics.conversions_value") Conversion_Value from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_US_AD_GROUP_AD_REPORT group by "ad_group.name","ad_group_ad.ad.id" ,"segments.date","campaign.name", "campaign.id"",
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
                        