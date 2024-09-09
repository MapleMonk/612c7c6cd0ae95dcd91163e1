{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_DRV AS with pre_facebook as ( select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas CL H2T\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Union all select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks ,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Herbobuild\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start < \'2023-10-01\' group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks ,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Ayurvedic Source\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ) select * from pre_facebook where date < \'2024-03-01\' union all select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas CL H2T\' Facebook_Account from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas CHRONIC\' Facebook_Account from rpsg_db.maplemonk.DRV_FACEBOOK_CHRONIC_CUSTOMCAMPAIGNS_DATA FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from rpsg_db.maplemonk.DRV_FACEBOOK_CHRONIC_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from rpsg_db.maplemonk.DRV_FACEBOOK_CHRONIC_CUSTOMCAMPAIGNS_DATA ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas FITNESS\' Facebook_Account from rpsg_db.maplemonk.DRV_FACEBOOK_FITNESS_CUSTOMCAMPAIGNS_DATA FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from rpsg_db.maplemonk.DRV_FACEBOOK_FITNESS_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from rpsg_db.maplemonk.DRV_FACEBOOK_FITNESS_CUSTOMCAMPAIGNS_DATA ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas MENS HEALTH\' Facebook_Account from rpsg_db.maplemonk.DRV_FACEBOOK_MENS_HEALTH_CUSTOMCAMPAIGNS_DATA FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from rpsg_db.maplemonk.DRV_FACEBOOK_MENS_HEALTH_CUSTOMCAMPAIGNS_DATA,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from rpsg_db.maplemonk.DRV_FACEBOOK_MENS_HEALTH_CUSTOMCAMPAIGNS_DATA ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV AS select ADSET_NAME, ADSET_ID ::varchar as ADSET_ID ,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID:: varchar as campaign_id, DATE ,NULL AS AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,case when lower(campaign_name) like \'retention%\' then \'RETENTION\' else CHANNEL end as channel ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value ,null as sku from RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_DRV group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\" ::varchar ,NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\":: varchar as campaign_id,\"segments.date\" ,\"segments.ad_network_type\" as d, \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value ,null as sku from rpsg_db.maplemonk.gads_drv_ad_group_ad_report group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\" :: varchar as campaign_id , \"segments.date\" ,\"campaign.advertising_channel_type\", NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value ,null as sku from RPSG_DB.maplemonk.gads_drv_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",\"campaign.advertising_channel_type\" UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\" ::varchar ,NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\" :: varchar as campaign_id,\"segments.date\" ,\"segments.ad_network_type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value ,null as sku from RPSG_DB.maplemonk.gads_drv2_ad_group_ad_report group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\":: varchar campaign_id, \"segments.date\" ,\"campaign.advertising_channel_type\", NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value ,null as sku from RPSG_DB.maplemonk.gads_drv2_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",\"campaign.advertising_channel_type\" UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\" ::varchar ,NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\" :: varchar as campaign_id,\"segments.date\" ,\"segments.ad_network_type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2S\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value ,null as sku from RPSG_DB.maplemonk.gads_drv_2s_ad_group_ad_report group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\":: varchar campaign_id, \"segments.date\" ,\"campaign.advertising_channel_type\", NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2S\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value ,null as sku from RPSG_DB.maplemonk.gads_drv_2s_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",\"campaign.advertising_channel_type\" union all Select NULL AS ACCOUNT_NAME ,PROFILEID::varchar ,ADID ::varchar as ad_id ,NULL AS AD_NAME ,upper(adGroupName) adGroupName ,adGroupId ::varchar as adGroupId ,upper(CAMPAIGNNAME) as CAMPAIGN_NAME ,CAMPAIGNID ::varchar as CAMPAIGN_ID ,DATE ::DATE DATE ,CAMPAIGN_TYPE ,NULL AS AD_STRENGTH ,NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL ,dayname(try_to_date(date)) DAY_OF_WEEK ,year(try_to_date(date)) YEAR ,month(try_to_date(date)) MONTH ,\'AMAZON\' AS CHANNEL ,adGroupId::varchar AS ACCOUNT ,sum(CLICKS) CLICKS ,sum(SPEND) as SPEND ,sum(IMPRESSIONS)IMPRESSIONS ,sum(CONVERSIONS) CONVERSIONS ,sum(AdSales) AdSales ,asin FROM RPSG_DB.MAPLEMONK.RPSG_DB_AMAZONADS_MARKETING where not(profileid in (\'3938387603612472\',\'3437629653399519\')) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,24 union all select \"AdGroup Name\" AS ACCOUNT_NAME, \"Ad Group ID\"::varchar as PROFILEID, \"Ad Group ID\"::varchar ad_id, \"AdGroup Name\" AS ad_name, upper(\"AdGroup Name\") adGroupName, \"Ad Group ID\" ::varchar as adGroupId, \"Campaign Name\" as CAMPAIGN_NAME, \"Campaign ID\"::varchar as CAMPAIGN_ID, coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) AS DATE, \'PLA\' as campaign_type, NULL AS AD_STRENGTH, NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL, dayname(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) Day_of_Week, YEAR(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"YEAR\", MONTH(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) AS \"MONTH\", \'Flipkart\' as CHANNEL, \'adGroupId\' as ACCOUNT, SUM(Clicks) Clicks, sum(\"Ad Spend\") as SPEND, SUM(VIEWS) AS Impressions, SUM(\"Units Sold (Direct)\" + \"Units Sold (Indirect)\") AS Conversions, sum(\"Direct Revenue\" + \"Indirect Revenue\" ) as AdSales, null as asin from RPSG_DB.MAPLEMONK.FLIPKART_DRV_ADS_FLIPKART_ADS FK1 group by ACCOUNT_NAME,ad_id,ad_name, CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, ACCOUNT, \"Ad Group ID\" UNION ALL select \"AdGroup Name\" AS ACCOUNT_NAME, \"Ad Group ID\"::varchar as PROFILEID, \"Ad Group ID\"::varchar ad_id, \"AdGroup Name\" AS ad_name, upper(\"AdGroup Name\") adGroupName, \"Ad Group ID\" ::varchar as adGroupId, \"Campaign Name\" as CAMPAIGN_NAME, \"Campaign ID\"::varchar as CAMPAIGN_ID, coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) AS DATE, \'PCA\' as campaign_type, NULL AS AD_STRENGTH, NULL AS AD_NETWORK_TYPE ,NULL AS AD_URL, dayname(coalesce(try_TO_DATE(\"DATE\", \'MM/DD/YYYY\'),try_TO_DATE(\"DATE\", \'DD-MM-YYYY\')) ::DATE) Day_of_Week, YEAR, MONTH, \'Flipkart\' as CHANNEL, \'adGroupId\' as ACCOUNT, SUM(Clicks) Clicks, sum(FINAL_AD_SPENDS) as SPEND, SUM(VIEWS) AS Impressions, SUM(\"Units Sold (Direct)\" + \"Units Sold (Indirect)\") AS Conversions, sum(\"Direct Revenue\" + \"Indirect Revenue\" ) as AdSales, null as asin from (WITH calculated_values AS ( SELECT *, EXTRACT(MONTH FROM TO_DATE(a.DATE, \'MM/DD/YYYY\')) AS month, EXTRACT(YEAR FROM TO_DATE(a.DATE, \'MM/DD/YYYY\')) AS year, SUM(a.\"Direct Revenue\" + a.\"Indirect Revenue\") OVER (PARTITION BY EXTRACT(MONTH FROM TO_DATE(a.DATE, \'MM/DD/YYYY\'))) AS monthly_total, (a.\"Direct Revenue\" + a.\"Indirect Revenue\") AS ad_sales, CASE WHEN SUM(a.\"Direct Revenue\" + a.\"Indirect Revenue\") OVER (PARTITION BY EXTRACT(MONTH FROM TO_DATE(a.DATE, \'MM/DD/YYYY\'))) != 0 THEN ( (a.\"Direct Revenue\" + a.\"Indirect Revenue\") / SUM(a.\"Direct Revenue\" + a.\"Indirect Revenue\") OVER (PARTITION BY EXTRACT(MONTH FROM TO_DATE(a.DATE, \'MM/DD/YYYY\'))) ) * 100 ELSE NULL END AS percentage FROM RPSG_DB.MAPLEMONK.FLIPKART_ADS_DRV_PCA_FLIPKART_ADS a ), joined_values AS ( SELECT cv.*, r.\"Ad spends\" AS ad_spends, CASE WHEN cv.percentage IS NOT NULL AND r.\"Ad spends\" IS NOT NULL THEN TO_CHAR(cv.percentage * r.\"Ad spends\") ELSE \'N/A\' END AS final_AD_Spends FROM calculated_values cv LEFT JOIN (select sum(\"Ad spends\") as \"Ad spends\" , EXTRACT(MONTH FROM TO_DATE(DATE, \'DD/MM/YYYY\')) AS month1, EXTRACT(YEAR FROM TO_DATE(DATE, \'DD/MM/YYYY\')) AS year1, from RPSG_DB.MAPLEMONK.FP_DRV_PCA_SPENDS_FK_DRV_PCA group by 2,3) r ON cv.month = r.MONTH1 AND cv.year = r.YEAR1 ) SELECT * FROM joined_values) group by ACCOUNT_NAME,ad_id,ad_name, CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, ACCOUNT, \"Ad Group ID\" , YEAR, MONTH",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from RPSG_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            