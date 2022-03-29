{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE UGAOO_DB.MAPLEMONK.MARKETING_CONSOLIDATED AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,month(Fb.date_start) as month ,year(Fb.date_start) as year ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel from UGAOO_DB.MAPLEMONK.FACEBOOK_ADS_INSIGHTS Fb left join( select ad_id,date_start ,SUM(C.value:value) Conversions from UGAOO_DB.MAPLEMONK.FACEBOOK_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null)Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from UGAOO_DB.MAPLEMONK.FACEBOOK_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null)Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from UGAOO_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        