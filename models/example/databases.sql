{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE appldatabase.maplemonk.alphanso_wh_GOOGLEADS_CONSOLIDATED AS SELECT \"ad_group.name\" AS ADSET_NAME ,\"ad_group.id\" AS ADSET_ID ,\"ad_group_ad.ad.id\"AS AD_ID ,CAST(NULL AS STRING) AS AD_NAME ,\'GOOGLE ADS\' AS ACCOUNT_NAME ,NULL AS ACCOUNT_ID ,\"campaign.name\" AS CAMPAIGN_NAME ,\"campaign.id\" AS CAMPAIGN_ID ,\"segments.date\" AS DATE ,\"ad_group_ad.ad.type\" AS AD_TYPE ,\"ad_group_ad.ad_strength\" AS AD_STRENGTH ,\"segments.ad_network_type\" AS AD_NETWORK_TYPE ,IFF(ARRAY_SIZE(\"ad_group_ad.ad.final_urls\") > 0, \"ad_group_ad.ad.final_urls\"[0]::STRING, NULL) AS AD_FINAL_URL ,\"segments.day_of_week\" AS DAY_OF_WEEK ,EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS YEAR ,EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS MONTH ,\'GOOGLE\' AS Channel ,\'GOOGLE ADS\' AS ACCOUNT ,SUM(CAST(\"metrics.clicks\" AS FLOAT)) AS Clicks ,SUM(CAST(\"metrics.cost_micros\" AS FLOAT))/1000000 AS Spend ,SUM(CAST(\"metrics.impressions\" AS FLOAT)) AS Impressions ,SUM(CAST(\"metrics.conversions\" AS FLOAT)) AS Conversions ,SUM(CAST(\"metrics.conversions_value\" AS FLOAT)) AS Conversion_Value FROM appldatabase.maplemonk.alphanso_google_ad_group_ad_report GROUP BY \"ad_group.name\" ,\"ad_group.id\" ,\"ad_group_ad.ad.id\" ,\"segments.date\" ,\"campaign.name\" ,\"campaign.id\" ,\"ad_group_ad.ad.type\" ,\"ad_group_ad.ad_strength\" ,\"segments.ad_network_type\" ,IFF(ARRAY_SIZE(\"ad_group_ad.ad.final_urls\") > 0, \"ad_group_ad.ad.final_urls\"[0]::STRING, NULL) ,\"segments.day_of_week\" UNION ALL SELECT NULL ,NULL ,NULL ,CAST(NULL AS STRING) ,\'GOOGLE ADS\' AS ACCOUNT_NAME ,NULL ,\"campaign.name\" ,\"campaign.id\" ,\"segments.date\" ,NULL ,NULL ,NULL ,NULL ,NULL ,EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS YEAR ,EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS MONTH ,\'GOOGLE\' AS Channel ,\'GOOGLE ADS\' AS ACCOUNT ,SUM(CAST(\"metrics.clicks\" AS FLOAT)) AS clicks ,SUM(CAST(\"metrics.cost_micros\" AS FLOAT))/1000000 AS spend ,SUM(CAST(\"metrics.impressions\" AS FLOAT)) AS Impressions ,SUM(CAST(\"metrics.conversions\" AS FLOAT)) AS Conversions ,SUM(CAST(\"metrics.conversions_value\" AS FLOAT)) AS Conversion_Value FROM appldatabase.maplemonk.alphanso_google_campaign_data WHERE \"campaign.advertising_channel_type\" IN (\'PERFORMANCE_MAX\',\'SMART\') GROUP BY \"campaign.name\", \"campaign.id\", \"segments.date\" ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from APPLDATABASE.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            