{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table vahdam_db.maplemonk.US_GOOGLE_ADS_CONSOLIDATED as select AD_GROUP_NAME ,AD_GROUP_AD_ID ,CAMPAIGN_NAME ,CAMPAIGN_ID ,SEGMENTS_DATE ,AD_GROUP_AD_AD_TYPE ,AD_GROUP_AD_AD_STRENGTH ,SEGMENTS_AD_NETWORK_TYPE ,AD_GROUP_AD_AD_FINAL_URLS ,SEGMENTS_DAY_OF_WEEK ,YEAR ,MONTH ,CHANNEL ,ACCOUNT ,CLICKS ,OUTBOUND_CLICKS ,SPEND ,IMPRESSIONS ,CONVERSIONS ,CONVERSION_VALUE from vahdam_db.maplemonk.US_GOOGLE_ADS_HISTORICAL UNION ALL select \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"campaign.name\", \"campaign.id\",\"segments.date\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\" ,\"segments.day_of_week\" ,YEAR(\"segments.date\") AS YEAR ,MONTH(\"segments.date\") AS MONTH ,\'Google Ads\' Channel ,\'Google US CONSOLIDATED\' ACCOUNT ,SUM(\"metrics.clicks\") Clicks ,NULL as Outbound_clicks ,SUM(\"metrics.cost_micros\")/1000000 Spend ,SUM(\"metrics.impressions\") Impressions ,SUM(\"metrics.conversions\") Conversions ,SUM(\"metrics.conversions_value\") Conversion_Value from VAHDAM_DB.MAPLEMONK.US_GADS_AD_GROUP_AD_REPORT group by \"ad_group.name\",\"ad_group_ad.ad.id\" ,\"segments.date\",\"campaign.name\", \"campaign.id\" ,\"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\", \"segments.day_of_week\" union select NULL, NULL ,\"campaign.name\", \"campaign.id\", \"segments.date\" ,NULL, NULL,NULL, NULL, NULL ,YEAR(\"segments.date\") YEAR ,MONTH(\"segments.date\") MONTH ,\'Google Ads\' Channel ,\'Google US CONSOLIDATED\' ACCOUNT ,sum(\"metrics.clicks\") clicks ,NULL as Outbound_clicks ,sum(\"metrics.cost_micros\")/1000000 spend ,sum(\"metrics.impressions\") impressions ,sum(\"metrics.conversions\") conveersions ,sum(\"metrics.conversions_value\") conversions_value from vahdam_db.maplemonk.US_GADS_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\";",
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
            