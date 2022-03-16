{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE LILGOODNESS_DB.MAPLEMONK.MARKETING_CONSOLIDATED AS select Adset_Name,Adset_ID,Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start Date ,NULL AD_TYPE, NULL AD_STRENGTH ,NULL AD_NETWORK_TYPE, NULL AD_URL ,NULL DAY_OF_WEEK ,year(Fb.date_start) as YEAR ,month(Fb.date_start) as MONTH ,SUM(CLICKS) Clicks,SUM(SPEND) Spend,SUM(IMPRESSIONS) Impressions ,SUM(NVL(Fc.Conversions,0)) Conversions ,SUM(NVL(Fcv.Conversion_Value,0)) Conversion_Value ,\'Facebook_Ads\' Channel from LILGOODNESS_DB.MAPLEMONK.FACEBOOK_LILGOODNESS_ADS_INSIGHTS Fb left join ( select ad_id,date_start ,SUM(C.value:value) Conversions from LILGOODNESS_DB.MAPLEMONK.FACEBOOK_LILGOODNESS_ADS_INSIGHTS,lateral flatten(input => ACTIONS) C where C.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(C.value:value) is not null )Fc ON Fb.ad_id = Fc.ad_id and Fb.date_start=Fc.date_start left join ( select ad_id,date_start ,SUM(CV.value:value) Conversion_Value from LILGOODNESS_DB.MAPLEMONK.FACEBOOK_LILGOODNESS_ADS_INSIGHTS,lateral flatten(input => ACTION_VALUES) CV where CV.value:action_type=\'offsite_conversion.fb_pixel_purchase\' group by ad_id,date_start having SUM(CV.value:value) is not null )Fcv ON Fb.ad_id = Fcv.ad_id and Fb.date_start=Fcv.date_start group by Adset_Name, Adset_ID, Account_Name, Account_ID, Campaign_Name, Campaign_ID,Fb.date_start UNION select \"ad_group.name\",NULL,NULL,NULL ,\"campaign.name\", \"campaign.id\",\"segments.date\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value ,\'Google_Ads\' Channel from LILGOODNESS_DB.MAPLEMONK.GADS_LILGOODNESS_AD_GROUP_AD_REPORT group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\"",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from LILGOODNESS_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        