{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET AS select * from SELECT_DB.MAPLEMONK.FACEBOOK_KYARI_POSTPAID_CUSTOM_CAMPAIGN_DATA_WO_URL_ASSET union all select * from SELECT_DB.MAPLEMONK.FACEBOOK_KYARI_PREPAID_CUSTOM_CAMPAIGN__DATA_WO_URL_ASSET ; CREATE OR REPLACE TABLE SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_WO_URL AS select Adset_Name ,Adset_ID ,FB.ad_id ,FB.ad_name ,case when ACCOUNT_ID = \'182042518234740\' then \'Facebook_Kyari_Postpaid\' when ACCOUNT_ID = \'504061961539745\' then \'Facebook_Kyari_Prepaid\' end as Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,null link_url_asset ,Fb.date_start::date Date ,SUM(CLICKS) Clicks ,sum(ifnull(FLc.link_clicks,0)) Link_Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,sum(nvl(fd.add_to_carts,0)) Add_to_carts ,sum(nvl(fdv.add_to_cart_value,0)) Add_to_cart_value ,sum(nvl(fe.landing_page_views,0)) Landing_page_views ,sum(nvl(ff.initiate_checkouts,0)) Initiate_checkouts ,sum(nvl(ffv.initiate_checkouts_value,0)) Initiate_checkouts_value ,\'Facebook\' Channel ,case when ACCOUNT_ID = \'182042518234740\' then \'Facebook_Kyari_Postpaid\' when ACCOUNT_ID = \'504061961539745\' then \'Facebook_Kyari_Prepaid\' end Account from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET FB left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Conversions from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(CV.value:value) Conversion_Value from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id ,ad_name ,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id ,ad_name ,date_start ,SUM(C.value:value) Link_Clicks from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTIONS) C where C.value:action_type=\'link_click\' group by ad_id ,ad_name ,date_start having SUM(C.value:value) is not null )FLc ON Fb.ad_id = FLc.ad_id and Fb.date_start=FLc.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) add_to_carts from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(C.value:value) is not null )Fd ON Fb.ad_id = Fd.ad_id and Fb.date_start=Fd.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Add_to_cart_Value from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_add_to_cart\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fdv ON Fb.ad_id = Fdv.ad_id and Fb.date_start=Fdv.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Landing_page_views from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTIONS) C where C.value:action_type=\'landing_page_view\' group by ad_id,date_start having SUM(C.value:value) is not null )Fe ON Fb.ad_id = Fe.ad_id and Fb.date_start=Fe.date_start left join ( select ad_id ,date_start ,SUM(C.value:value) Initiate_checkouts from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(C.value:value) is not null )Ff ON Fb.ad_id = Ff.ad_id and Fb.date_start=Ff.date_start left join ( select ad_id ,date_start ,SUM(CV.value:value) Initiate_checkouts_Value from SELECT_DB.MAPLEMONK.SELECT_DB_FACEBOOK_CONSOLIDATED_DATA_WO_URL_ASSET,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_initiate_checkout\' group by ad_id,date_start having SUM(CV.value:value) is not null )Ffv ON Fb.ad_id = Ffv.ad_id and Fb.date_start=Ffv.date_start group by Adset_Name, Adset_ID,fb.ad_id,fb.AD_NAME, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SELECT_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            