{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.Facebook_snitch_CONSOLIDATED AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Snitch_FACEBOOK\' Facebook_Account from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS Facebook left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from Snitch_db.MAPLEMONK.FACEBOOK_snitch_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Facebook.ad_id = Fc.ad_id and Facebook.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Facebook.ad_id = Fcv.ad_id and Facebook.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Facebook.ad_id = Fd.ad_id and Facebook.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Facebook.ad_id = Fdv.ad_id and Facebook.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Facebook.ad_id = Fe.ad_id and Facebook.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Facebook.ad_id = Ff.ad_id and Facebook.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from Snitch_db.MAPLEMONK.Facebook_snitch_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Facebook.ad_id = Ffv.ad_id and Facebook.date_start=Ffv.date_start where facebook.date_start < \'2023-09-01\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start union select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Snitch_FACEBOOK_new\' Facebook_Account from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS Facebook left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Facebook.ad_id = Fc.ad_id and Facebook.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Facebook.ad_id = Fcv.ad_id and Facebook.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Facebook.ad_id = Fd.ad_id and Facebook.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Facebook.ad_id = Fdv.ad_id and Facebook.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Facebook.ad_id = Fe.ad_id and Facebook.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Facebook.ad_id = Ff.ad_id and Facebook.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from Snitch_db.MAPLEMONK.FACEBOOK_NEW_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Facebook.ad_id = Ffv.ad_id and Facebook.date_start=Ffv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start union select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Snitch_FACEBOOK_additional\' Facebook_Account from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS Facebook left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Facebook.ad_id = Fc.ad_id and Facebook.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Facebook.ad_id = Fcv.ad_id and Facebook.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Facebook.ad_id = Fd.ad_id and Facebook.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Facebook.ad_id = Fdv.ad_id and Facebook.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Facebook.ad_id = Fe.ad_id and Facebook.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Facebook.ad_id = Ff.ad_id and Facebook.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from Snitch_db.MAPLEMONK.FACEBOOK_ADDITIONAL_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Facebook.ad_id = Ffv.ad_id and Facebook.date_start=Ffv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start union select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Snitch_FACEBOOK\' Facebook_Account from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights Facebook left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Facebook.ad_id = Fc.ad_id and Facebook.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_purchase\',\'app_custom_event.fb_mobile_purchase\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Facebook.ad_id = Fcv.ad_id and Facebook.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Facebook.ad_id = Fd.ad_id and Facebook.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_add_to_cart\',\'app_custom_event.fb_mobile_add_to_cart\') group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Facebook.ad_id = Fdv.ad_id and Facebook.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Facebook.ad_id = Fe.ad_id and Facebook.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTIONS) C where C.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Facebook.ad_id = Ff.ad_id and Facebook.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from Snitch_db.MAPLEMONK.facebook_snitch_customad_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type in (\'offsite_conversion.fb_pixel_initiate_checkout\', \'app_custom_event.fb_mobile_initiate_checkout\') group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Facebook.ad_id = Ffv.ad_id and Facebook.date_start=Ffv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Facebook.date_start ; CREATE OR REPLACE TABLE Snitch_db.MAPLEMONK.MARKETING_CONSOLIDATED_SNITCH AS select ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID::varchar CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,sum(reach) Reach ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value ,sum(nvl(add_to_carts,0)) Add_to_carts ,sum(nvl(add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(landing_page_views,0)) Landing_page_views ,sum(nvl(initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(initiate_checkouts_value,0)) Initiate_checkouts_value from Snitch_db.MAPLEMONK.Facebook_snitch_CONSOLIDATED group by ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT UNION select \"ad_group.name\",\"ad_group_ad.ad.id\",NULL,NULL ,\"campaign.name\", \"campaign.id\"::varchar,\"segments.date\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google\' Channel ,\'Google_Snitch\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions , null as reach ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value , null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from Snitch_db.MAPLEMONK.GOOGLEADS_snitch_AD_GROUP_AD_REPORT group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" UNION select NULL, NULL, NULL, NULL ,\"campaign.name\", \"campaign.id\"::varchar, \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google_Snitch\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions , null as reach ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value , null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from snitch_db.maplemonk.google_ads_googleads_snitch_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\" UNION select NULL, NULL, NULL, NULL ,\"Campaign Name\", CODE::varchar code, to_date(DATE,\'dd/mm/yyyy\') date ,NULL, NULL,NULL, NULL, NULL ,YEAR(to_date(DATE,\'dd/mm/yyyy\')) YEAR ,MONTH(to_date(DATE,\'dd/mm/yyyy\')) MONTH ,CHANNEL ,CHANNEL ACCOUNT ,NULL clicks ,case when lower(CHANNEL) = \'sms\' then replace(\"Total Sent\",\',\',\'\')::float*0.12 when lower(CHANNEL) = \'whatsapp\' then replace(\"Total Sent\",\',\',\'\')::float*0.75 when lower(CHANNEL) = \'push\' then replace(\"Total Sent\",\',\',\'\')::float*0 when lower(CHANNEL) = \'inapp\' then replace(\"Total Sent\",\',\',\'\')::float*0 end spend ,replace(\"Total Sent\",\',\',\'\')::float impressions , null as reach ,NULL conversions ,ifnull(f.gross_Sales,0) + ifnull(g.gross_Sales,0) conversions_value , null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from snitch_db.maplemonk.wsms_spends_sheet1 m left join ( select discount_code,sum(gross_sales) gross_sales from snitch_db.maplemonk.order_level_ga_data group by 1 )f on lower(m.code) = lower(f.discount_code) left join ( select sessioncampaignname,sum(gross_sales) gross_sales from snitch_db.maplemonk.order_level_ga_data where lower(final_channel_code) in (\'sms\',\'whatsapp\',\'push\',\'inapp\') and discount_code is null group by 1 )g on lower(m.\"Campaign Name\") = lower(g.sessioncampaignname) UNION select NULL AS ADSET_NAME,NULL AS ADSET_ID,NULL AS ACCOUNT_NAME, NULL AS ACCOUNT_ID ,NULL AS CAMPAIGN_NAME, NULL AS CAMPAIGN_ID, order_timestamp::date as DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,\'Influencers\' AS Channel ,NULL AS ACCOUNT ,NULL AS Clicks ,SUM(discount) Spend ,NULL AS Impressions ,NULL AS Reach ,NULL AS Conversions ,NULL AS Conversion_Value ,NULL AS Add_to_carts ,NULL AS Add_to_cart_value ,NULL AS Landing_page_views ,NULL AS Initiate_checkouts ,NULL AS Initiate_checkouts_value from Snitch_db.MAPLEMONK.fact_items_snitch where lower(discount_code) like \'%influ%\' group by ADSET_NAME,ADSET_ID,ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, ACCOUNT ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from snitch_db.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        