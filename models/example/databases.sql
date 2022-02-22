{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PUBLIC.MARKETING_CONSOLIDATED AS select Ad_Name AdName,Fb.ad_id AdId,Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,Year(Fb.date_start) Year ,Month(Fb.date_start) Month ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook Ads\' Channel from MAPLEMONK.PUBLIC.FBANVESHAN_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from MAPLEMONK.PUBLIC. FBANVESHAN_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from MAPLEMONK.PUBLIC. FBANVESHAN_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Ad_Name,Fb.ad_id,Adset_Name, Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION select NULL,NULL,ad_group.name,ad_group_ad.ad.id,NULL,NULL ,campaign.name, campaign.id,segments.date ,Year(segments.date) ,Month(segments.date) ,SUM(metrics.clicks) Clicks ,SUM(metrics.cost_micros)/1000000 Spend,SUM(metrics.impressions) Impressions ,SUM(metrics.conversions) Conversions,SUM(metrics.conversions_value) Conversion_Value ,\'Google Ads\' Channel from MAPLEMONK.PUBLIC.gads_anveshan_AD_GROUP_AD_REPORT group by ad_group.name,ad_group_ad.ad.id ,segments.date,campaign.name, campaign.id",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from MapleMonk.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        