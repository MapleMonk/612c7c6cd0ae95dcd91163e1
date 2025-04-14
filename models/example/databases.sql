{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE EMMASLEEP_DB.maplemonk.EMMASLEEP_DB_GOOGLEADS_CONSOLIDATED AS select \"ad_group.name\" ADSET_NAME ,\"ad_group.id\" as ADSET_ID ,\"ad_group_ad.ad.id\" as AD_ID ,cast(NULL as string) as AD_NAME ,\'GOOGLE ADS\' as ACCOUNT_NAME ,NULL as ACCOUNT_ID ,\"campaign.name\" as CAMPAIGN_NAME ,\"campaign.id\" as CAMPAIGN_ID ,\"segments.date\" as DATE ,\"ad_group_ad.ad.type\" as AD_TYPE ,\'ad_group_ad.ad.strength\' as AD_STRENGTH ,\"segments.ad_network_type\" as AD_NETWORK_TYPE ,CASE WHEN ARRAY_SIZE(PARSE_JSON(\"ad_group_ad.ad.final_urls\"::STRING)) > 0 THEN PARSE_JSON(\"ad_group_ad.ad.final_urls\"::STRING)[0]::STRING ELSE NULL END AS AD_FINAL_URL ,\"segments.day_of_week\" as DAY_OF_WEEK ,EXTRACT(YEAR FROM cast(\"segments.date\" as date)) AS YEAR ,EXTRACT(MONTH FROM cast(\"segments.date\" as date)) AS MONTH ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,SUM(cast (\"metrics.clicks\" as FLOAT)) Clicks ,SUM(cast (\"metrics.cost_micros\" as FLOAT))/1000000 Spend ,SUM(cast (\"metrics.impressions\" as FLOAT)) Impressions ,SUM(cast (\"metrics.conversions\" as FLOAT)) Conversions ,SUM(cast (\"metrics.conversions_value\" as FLOAT)) Conversion_Value from EMMASLEEP_DB.maplemonk.Google_ads_emma_ad_group_ad_report group by \"ad_group.name\" ,\"ad_group.id\" ,\"ad_group_ad.ad.id\" ,\"segments.date\" ,\"campaign.name\" ,\"campaign.id\" ,\"ad_group_ad.ad.type\" ,\"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\" ,CASE WHEN ARRAY_SIZE(PARSE_JSON(\"ad_group_ad.ad.final_urls\"::STRING)) > 0 THEN PARSE_JSON(\"ad_group_ad.ad.final_urls\"::STRING)[0]::STRING ELSE NULL END ,\"segments.day_of_week\" UNION all select NULL ,NULL ,NULL ,cast(NULL as string) ,\'GOOGLE ADS\' as ACCOUNT_NAME ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,EXTRACT(YEAR FROM cast(\"segments.date\" as date)) AS YEAR ,EXTRACT(MONTH FROM cast(\"segments.date\" as date)) AS MONTH ,\'GOOGLE\' Channel ,\'GOOGLE ADS\' ACCOUNT ,SUM(cast (\"metrics.clicks\" as FLOAT)) clicks ,SUM(cast (\"metrics.cost_micros\" as FLOAT))/1000000 spend ,SUM(cast (\"metrics.impressions\" as FLOAT)) Impressions ,SUM(cast (\"metrics.conversions\" as FLOAT)) Conversions ,SUM(cast (\"metrics.conversions_value\" as FLOAT)) Conversion_Value from EMMASLEEP_DB.maplemonk.Google_ads_emma_campaign_data where \"campaign.advertising_channel_type\" in (\'PERFORMANCE_MAX\',\'SMART\') group by \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from EMMASLEEP_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            