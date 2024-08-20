{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE VAHDAM_DB.MAPLEMONK.FACEBOOK_US_CONSOLIDATED AS select ADSET_NAME ,ADSET_ID ,ACCOUNT_NAME ,ACCOUNT_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,DATE ,CLICKS ,SPEND ,IMPRESSIONS ,UNIQUE_INLINE_LINK_CLICKS ,TOTAL_INLINE_LINK_CLICKS ,UNIQUE_OUTBOUND_CLICKS ,TOTAL_OUTBOUND_CLICKS ,CONVERSIONS ,CONVERSION_VALUE ,CHANNEL ,FACEBOOK_ACCOUNT from VAHDAM_DB.MAPLEMONK.FACEBOOK_US_CONSOLIDATED_HISTORICAL Fbh UNION select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,sum(UNIQUE_INLINE_LINK_CLICKS) UNIQUE_INLINE_LINK_CLICKS ,sum(UNIQUE_INLINE_LINK_CLICKS) Total_INLINE_LINK_CLICKS ,sum(FBUOC.UNIQUE_OUTBOUND_CLICKS) UNIQUE_OUTBOUND_CLICKS ,sum(FBOC.TOTAL_OUTBOUND_CLICKS) TOTAL_OUTBOUND_CLICKS ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel ,\'FB CONSOLIDATED US\' Facebook_Account from VAHDAM_DB.MAPLEMONK.US_FB_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from VAHDAM_DB.MAPLEMONK.US_FB_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from VAHDAM_DB.MAPLEMONK.US_FB_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start left join ( select ad_id,date_start ,SUM(C.value:value) UNIQUE_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.US_FB_ADS_INSIGHTS,lateral flatten(Input => UNIQUE_OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBUOC on Fb.ad_id = FBUOC.ad_id and Fb.date_start=FBUOC.date_start left join ( select ad_id,date_start ,SUM(C.value:value) TOTAL_OUTBOUND_CLICKS from VAHDAM_DB.MAPLEMONK.US_FB_ADS_INSIGHTS,lateral flatten(Input => OUTBOUND_CLICKS) C where C.value:action_type=\'outbound_click\' group by ad_id,date_start having SUM(C.value:value) is not null ) FBOC on Fb.ad_id = FBOC.ad_id and Fb.date_start=FBOC.date_start where Fb.date_start::date <= \'2024-08-10\' group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start;",
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
            