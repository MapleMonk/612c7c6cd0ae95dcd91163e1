{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE Perfora_DB.MapleMonk.Perfora_DB_GOOGLEADS_CONSOLIDATED AS select \"ad_group.name\" ADSET_NAME ,\"ad_group_ad.ad.id\" as ADSET_ID ,NULL as AD_ID ,NULL as AD_NAME ,NULL as ACCOUNT_NAME ,NULL as ACCOUNT_ID ,\"campaign.name\" as CAMPAIGN_NAME ,\"campaign.id\" as CAMPAIGN_ID ,\"segments.date\"::date as DATE ,\"ad_group_ad.ad.type\" as AD_TYPE ,\"ad_group_ad.ad_strength\" as AD_STRENGTH ,\"segments.ad_network_type\" as AD_NETWORK_TYPE ,\"ad_group_ad.ad.final_urls\" as AD_FINAL_URL ,\"segments.day_of_week\" as DAY_OF_WEEK ,YEAR(\"segments.date\") AS YEAR1 ,MONTH(\"segments.date\") AS MONTH1 ,\'Google\' Channel ,\'Google_Google_ads_Google_Ads___Perfora\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from Perfora_DB.MapleMonk.Google_ads_Google_Ads___Perfora_ad_group_ad_report group by \"ad_group.name\" ,\"ad_group_ad.ad.id\" ,\"segments.date\" ,\"campaign.name\" ,\"campaign.id\" ,\"ad_group_ad.ad.type\" ,\"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\" ,\"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" UNION all select NULL ,NULL ,NULL ,NULL ,NULL ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,YEAR(\"segments.date\") YEAR1 ,MONTH(\"segments.date\") MONTH1 ,\'Google\' Channel ,\'Google_Google_ads_Google_Ads___Perfora\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from Perfora_DB.MapleMonk.Google_ads_Google_Ads___Perfora_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from Perfora_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        