{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE FUAARK_DB.MAPLEMONK.FUAARK_DB_GOOGLEADS_CONSOLIDATED AS select \"ad_group.name\" ADSET_NAME ,\"ad_group_ad.ad.id\" as ADSET_ID ,NULL as AD_ID ,NULL as AD_NAME ,\'Google_Google_ads_Google_ads_fuaark\' as ACCOUNT_NAME ,NULL as ACCOUNT_ID ,\"campaign.name\" as CAMPAIGN_NAME ,\"campaign.id\" as CAMPAIGN_ID ,\"segments.date\"::date as DATE ,\"ad_group_ad.ad.type\" as AD_TYPE ,\"ad_group_ad.ad_strength\" as AD_STRENGTH ,\"segments.ad_network_type\" as AD_NETWORK_TYPE ,\"ad_group_ad.ad.final_urls\" as AD_FINAL_URL ,\"segments.day_of_week\" as DAY_OF_WEEK ,YEAR(\"segments.date\"::date) AS YEAR1 ,MONTH(\"segments.date\"::date) AS MONTH1 ,\'Google\' Channel ,\'Google_Google_ads_Google_ads_fuaark\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from FUAARK_DB.MAPLEMONK.Google_ads_Google_ads_fuaark_ad_group_ad_report group by \"ad_group.name\" ,\"ad_group_ad.ad.id\" ,\"segments.date\" ,\"campaign.name\" ,\"campaign.id\" ,\"ad_group_ad.ad.type\" ,\"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\" ,\"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" UNION all select NULL ,NULL ,NULL ,NULL ,\'Google_Google_ads_Google_ads_fuaark\' as ACCOUNT_NAME ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,YEAR(\"segments.date\"::date) YEAR1 ,MONTH(\"segments.date\"::date) MONTH1 ,\'Google\' Channel ,\'Google_Google_ads_Google_ads_fuaark\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conversions ,sum(\"metrics.conversions_value\") conversions_value from FUAARK_DB.MAPLEMONK.Google_ads_Google_ads_fuaark_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from FUAARK_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        