{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anweshan_googleads_consolidated; CREATE TABLE public.anweshan_googleads_consolidated AS SELECT \"ad_group.name\" AS adset_name, \"ad_group.id\" AS adset_id, \"ad_group_ad.ad.id\" AS ad_id, NULL AS ad_name, \'GOOGLE ADS\' AS account_name, NULL AS account_id, \"campaign.name\" AS campaign_name, \"campaign.id\" AS campaign_id, \"segments.date\" AS date, \"ad_group_ad.ad.type\" AS ad_type, \"ad_group_ad.ad_strength\" AS ad_strength, \"segments.ad_network_type\" AS ad_network_type, \"ad_group_ad.ad.final_urls\"[0]::VARCHAR AS ad_final_url, \"segments.day_of_week\" AS day_of_week, EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS year, EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS month, \'GOOGLE\' AS channel, \'GOOGLE ADS\' AS account, SUM(CAST(\"metrics.clicks\" AS FLOAT)) AS clicks, SUM(CAST(\"metrics.cost_micros\" AS FLOAT))/1000000 AS spend, SUM(CAST(\"metrics.impressions\" AS FLOAT)) AS impressions, SUM(CAST(\"metrics.conversions\" AS FLOAT)) AS conversions, SUM(CAST(\"metrics.conversions_value\" AS FLOAT)) AS conversion_value FROM public.google_ads_google_ads_ad_group_ad_report GROUP BY \"ad_group.name\", \"ad_group.id\", \"ad_group_ad.ad.id\", \"segments.date\", \"campaign.name\", \"campaign.id\", \"ad_group_ad.ad.type\", \"ad_group_ad.ad_strength\", \"segments.ad_network_type\", \"ad_group_ad.ad.final_urls\"[0]::VARCHAR, \"segments.day_of_week\" UNION ALL SELECT NULL AS adset_name, NULL AS adset_id, NULL AS ad_id, NULL AS ad_name, \'GOOGLE ADS\' AS account_name, NULL AS account_id, \"campaign.name\" AS campaign_name, \"campaign.id\" AS campaign_id, \"segments.date\" AS date, NULL AS ad_type, NULL AS ad_strength, NULL AS ad_network_type, NULL AS ad_final_url, NULL AS day_of_week, EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS year, EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS month, \'GOOGLE\' AS channel, \'GOOGLE ADS\' AS account, SUM(CAST(\"metrics.clicks\" AS FLOAT)) AS clicks, SUM(CAST(\"metrics.cost_micros\" AS FLOAT))/1000000 AS spend, SUM(CAST(\"metrics.impressions\" AS FLOAT)) AS impressions, SUM(CAST(\"metrics.conversions\" AS FLOAT)) AS conversions, SUM(CAST(\"metrics.conversions_value\" AS FLOAT)) AS conversion_value FROM public.google_ads_google_ads_campaign_data WHERE \"campaign.advertising_channel_type\" IN (\'PERFORMANCE_MAX\', \'SMART\') GROUP BY \"campaign.name\", \"campaign.id\", \"segments.date\";",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from STV_TBL_PERM
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            