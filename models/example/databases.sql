{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE hilodesign_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_hilo AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook India\' Facebook_Account from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id,date_start ,SUM(C.value:value) add_to_carts from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Add_to_cart_Value from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id,date_start ,SUM(C.value:value) Landing_page_views from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id,date_start ,SUM(C.value:value) Initiate_checkouts from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from hilodesign_db.MAPLEMONK.FB_INDIA_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook StyleAssistLeads\' Facebook_Account from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id,date_start ,SUM(C.value:value) add_to_carts from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Add_to_cart_Value from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id,date_start ,SUM(C.value:value) Landing_page_views from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id,date_start ,SUM(C.value:value) Initiate_checkouts from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from hilodesign_db.MAPLEMONK.FB_INFLUENCER_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start; CREATE OR REPLACE TABLE hilodesign_db.MAPLEMONK.MARKETING_CONSOLIDATED_HILO AS select ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE ,NULL AD_TYPE,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE,NULL AD_URL ,NULL Day_of_Week ,YEAR(DATE) AS YEAR ,MONTH(DATE) AS MONTH ,CHANNEL ,FACEBOOK_ACCOUNT AS ACCOUNT ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(CONVERSIONS) Conversions ,SUM(CONVERSION_VALUE) Conversion_Value ,sum(Add_to_carts) Add_to_carts ,sum(Add_to_cart_value) Add_to_cart_value ,sum(Landing_page_views) Landing_page_views ,sum(Initiate_checkouts) Initiate_checkouts ,sum(Initiate_checkouts_value) Initiate_checkouts_value from hilodesign_db.MAPLEMONK.FACEBOOK_CONSOLIDATED_HILO group by ACCOUNT_NAME, ACCOUNT_ID ,CAMPAIGN_NAME, CAMPAIGN_ID, DATE, CHANNEL, FACEBOOK_ACCOUNT union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google\' Channel ,\'Google India\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversion_value ,null as Add_to_carts ,null as Add_to_cart_value ,null as Landing_page_views ,null as Initiate_checkouts ,null as Initiate_checkouts_value from HILODESIGN_DB.maplemonk.gads_in_campaign_data group by \"campaign.name\", \"campaign.id\", \"segments.date\"",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from HILODESIGN_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        