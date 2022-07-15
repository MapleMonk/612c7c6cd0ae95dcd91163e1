{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE av_db.MAPLEMONK.MARKETING_CONSOLIDATED_AV AS select Adset_Name ,Adset_ID ,Account_Name ,Account_ID ,Campaign_Name ,Campaign_ID ,Fb.date_start Date ,NULL AD_TYPE ,NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE ,NULL AD_URL ,NULL DAY_OF_WEEK ,year(Fb.date_start) as YEAR ,month(Fb.date_start) as MONTH ,SUM(CLICKS) Clicks ,SUM(SPEND) Spend ,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook_Ads\' Channel from av_db.maplemonk.facebook_evocus_ads_insights Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from av_db.MAPLEMONK.facebook_evocus_ads_insights,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from av_db.MAPLEMONK.facebook_evocus_ads_insights,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION select NULL ,NULL ,NULL ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversions_value ,\'Google Ads\' Channel from av_db.maplemonk.GADS_IN_CAMPAIGN_DATA group by \"campaign.name\", \"campaign.id\", \"segments.date\";",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from AV_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        