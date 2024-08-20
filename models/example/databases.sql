{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.FACEBOOK_GLOBAL_CONSOLIDATED AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Influenccer Global\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_ADS_INSIGHTS,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start <= \'2022-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Influenccer Global\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_ADS_INSIGHTS,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start >= \'2022-08-11\' and Fb.date_start <= \'2024-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Influenccer Global\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_CUSTOMCAMPAIGNS_DATA Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_INFLUENCER_GLOBAL_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start > \'2024-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Global Main\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_ADS_INSIGHTS,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start <= \'2022-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Global Main\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_ADS_INSIGHTS,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start >= \'2022-08-11\' and Fb.date_start <= \'2024-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB Global Main\' Facebook_Account from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_CUSTOMCAMPAIGNS_DATA Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.FB_GLOBAL_MAIN_NEW_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start > \'2024-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.GLOBAL_MARKETING_CONSOLIDATED AS select ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,sum(TOTAL_OUTBOUND_CLICKS) as Outbound_clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from VAHDAM_DB.MAPLEMONK.FACEBOOK_GLOBAL_CONSOLIDATED group by ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION select \"ad_group.name\",\"ad_group_ad.ad.id\",NULL,NULL ,\"campaign.name\", \"campaign.id\",\"segments.date\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google Ads\' Channel ,\'Google Global\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,NULL as Outbound_clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GLOBAL_AD_GROUP_AD_REPORT group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" union select NULL, NULL, NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google Ads\' Channel ,\'Google Global\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,NULL as Outbound_clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversions_value from VAHDAM_DB.MAPLEMONK.GOOGLE_ADS_GLOBAL_CAMPAIGN_DATA where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\"",
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
            