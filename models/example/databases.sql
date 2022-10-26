{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE pomme_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_pomme AS select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Pomme\' Facebook_Account from pomme_db.MAPLEMONK.fb_ads_insights FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:value) Conversions from pomme_db.MAPLEMONK.fb_ads_insights ,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value from pomme_db.MAPLEMONK.fb_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; CREATE OR REPLACE TABLE pomme_db.MAPLEMONK.MARKETING_CONSOLIDATED_pomme AS select ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from pomme_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_pomme group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION select NULL, NULL, NULL ,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\"::varchar, \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Pomme\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from pomme_db.maplemonk.gads_pomme_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\" UNION select NULL, NULL,NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\"::varchar, \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Pomme Prospecting\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from pomme_db.maplemonk.gads_pomme2_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\";",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Pomme_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        