{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_DRV AS with pre_facebook as ( select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas CL H2T\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_DRVCLH2T_ADS_INSIGHTS ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Union all select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_DRVAIDYAS_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks ,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Herbobuild\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_HERBOBUILD_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start where Fb.date_start < \'2023-10-01\' group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name,Adset_ID,fb.ad_id,fb.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks ,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Ayurvedic Source\' Facebook_Account from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from RPSG_DB.MAPLEMONK.FBADS_AYURVEDICSOURCE_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.ad_name, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ) select * from pre_facebook where date < \'2024-03-01\' union all select Adset_Name,Adset_ID,FB.ad_id,FB.ad_name,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks_previous ,sum(ifnull(inline_link_clicks,0)) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value1 ,SUM(NVL(Fcv.Conversion_Value_1d,0)) Conversion_Value2 ,SUM(NVL(Fcv.Conversion_Value_7d,0)) Conversion_Value ,\'Facebook\' Channel ,\'Facebook Dr.Vaidyas CL H2T\' Facebook_Account from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data FB left join ( select ad_id,ad_name,date_start ,SUM(C.value:\"7d_click\") Conversions from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,ad_name,date_start ,SUM(CV.value:value) Conversion_Value ,SUM(CV.value:\"1d_click\" ) Conversion_Value_1d ,SUM(CV.value:\"7d_click\" ) Conversion_Value_7d from rpsg_db.maplemonk.fb_drv_updated_customcampaigns_data ,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,ad_name,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ; CREATE OR REPLACE TABLE RPSG_DB.MAPLEMONK.MARKETING_CONSOLIDATED_DRV AS select ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID:: varchar as campaign_id, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value from RPSG_DB.MAPLEMONK.FACEBOOK_CONSOLIDATED_DRV group by ADSET_NAME, ADSET_ID,ad_id,ad_name, ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\",NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\":: varchar as campaign_id,\"segments.date\" ,\"segments.ad_network_type\" as d, \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from rpsg_db.maplemonk.gads_drv_ad_group_ad_report group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\" :: varchar as campaign_id , \"segments.date\" ,\"campaign.advertising_channel_type\", NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from RPSG_DB.maplemonk.gads_drv_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",\"campaign.advertising_channel_type\" UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\",NULL,NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\" :: varchar as campaign_id,\"segments.date\" ,\"segments.ad_network_type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from RPSG_DB.maplemonk.gads_drv2_ad_group_ad_report group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION all select NULL, NULL, NULL, NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\":: varchar campaign_id, \"segments.date\" ,\"campaign.advertising_channel_type\", NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google Dr.Vaidyas 2\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from RPSG_DB.maplemonk.gads_drv2_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\",\"campaign.advertising_channel_type\" ;",
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
                        