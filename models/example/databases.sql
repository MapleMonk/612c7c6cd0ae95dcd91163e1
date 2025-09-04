{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS dev.maplemonk.Google_Ads_Fact_items; CREATE TABLE dev.maplemonk.Google_Ads_Fact_items AS SELECT NULL AS col1, NULL AS col2, NULL AS col3, CAST(NULL AS VARCHAR) AS col4, \'GOOGLE ADS\' AS ACCOUNT_NAME, NULL AS col6, \"campaign.id\" AS campaign_ID, \"segments.date\", NULL AS col10, NULL AS col11, NULL AS col12, NULL AS col13, NULL AS col14, EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS YEAR, EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS MONTH, \'GOOGLE\' AS \"Channel\", \'GOOGLE ADS\' AS \"ACCOUNT\", SUM(CAST(\"metrics.clicks\" AS DOUBLE PRECISION)) AS clicks, SUM(CAST(\"metrics.cost_micros\" AS DOUBLE PRECISION)) / 1000000 AS spend, SUM(CAST(\"metrics.impressions\" AS DOUBLE PRECISION)) AS impressions, SUM(CAST(\"metrics.conversions\" AS DOUBLE PRECISION)) AS conversions, SUM(CAST(\"metrics.conversions_value\" AS DOUBLE PRECISION)) AS conversion_value FROM dev.public.maplemonk_redshift_campaign_data WHERE \"campaign.advertising_channel_type\" IN (\'PERFORMANCE_MAX\', \'SMART\') GROUP BY \"campaign.id\", \"segments.date\";",
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
            