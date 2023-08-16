{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE MYMUSE_DB.MAPLEMONK.MYMUSE_DB_FACEBOOK_CONSOLIDATED AS select Adset_Name ,Adset_ID ,FB.ad_id ,FB.ad_name ,\'Facebook_Monthly_1\' as Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,Fb.date_start::date Date ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook_Monthly_1\' Account from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS FB left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Conversions from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(CV.value:value) Conversion_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION ALL select Adset_Name ,Adset_ID ,FB.ad_id ,FB.ad_name ,\'Facebook_Performance\' as Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,Fb.date_start::date Date ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook_Performance\' Account from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS FB left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Conversions from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(CV.value:value) Conversion_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Perf_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name ,Adset_ID ,FB.ad_id ,FB.ad_name ,\'Facebook_Monthly_2\' as Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,Fb.date_start::date Date ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook_Monthly_2\' Account from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS FB left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Conversions from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(CV.value:value) Conversion_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_2_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start union all select Adset_Name ,Adset_ID ,FB.ad_id ,FB.ad_name ,\'Facebook_Monthly_3\' as Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,Fb.date_start::date Date ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,\'Facebook_Monthly_3\' Account from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS FB left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Conversions from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(CV.value:value) Conversion_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from MYMUSE_DB.MAPLEMONK.Custom_Facebook_Marketing_MyMuse_Monthly_3_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MYMUSE_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        