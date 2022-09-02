{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE ghc_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_ghc AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook MARS\' Facebook_Account from ghc_db.MAPLEMONK.FB_MARS_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from ghc_db.MAPLEMONK.fb_mars_ads_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from ghc_db.MAPLEMONK.fb_mars_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Saturn\' Facebook_Account from ghc_db.MAPLEMONK.fb_saturn_ads_insights Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from ghc_db.MAPLEMONK.fb_saturn_ads_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from ghc_db.MAPLEMONK.fb_saturn_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; CREATE OR REPLACE TABLE ghc_db.MAPLEMONK.MARKETING_CONSOLIDATED_GHC AS select ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from GHC_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_ghc group by ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google MARS\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value from ghc_db.maplemonk.gads_mars_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" UNION select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google MARS 2\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value from ghc_db.maplemonk.gads_mars2_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google SATURN\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value from ghc_db.maplemonk.gads_saturn_campaign_data where \"segments.date\" between \'2022-07-21\' and \'2022-07-27\' group by \"campaign.name\", \"campaign.id\", \"segments.date\"",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from GHC_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        