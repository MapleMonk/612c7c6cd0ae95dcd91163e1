{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS PUBLIC.ANVESHAN_GOOGLE_ADS_CITY_LEVEL_CONSOLIDATED; CREATE TABLE PUBLIC.ANVESHAN_GOOGLE_ADS_CITY_LEVEL_CONSOLIDATED AS select \'GOOGLE\' AS channel, \'GOOGLE ADS\' AS account, \"customer.id\" AS account_customer_id, \"campaign.id\" AS campaign_id, \"campaign.name\" AS campaign_name, \"campaign.status\" AS campaign_status, \"segments.date\" AS date, SPLIT_PART(\"segments.geo_target_city\", \'/\', 2) as city_code, g.city, EXTRACT(YEAR FROM CAST(\"segments.date\" AS DATE)) AS year, EXTRACT(MONTH FROM CAST(\"segments.date\" AS DATE)) AS month, \"campaign.advertising_channel_type\" as ad_type, SUM(CAST(\"metrics.clicks\" AS FLOAT)) AS clicks, SUM(CAST(\"metrics.impressions\" AS FLOAT)) AS impressions, SUM(CAST(\"metrics.conversions\" AS FLOAT)) AS conversions, SUM(CAST(\"metrics.cost_micros\" AS FLOAT))/1000000 AS spend, SUM(CAST(\"metrics.conversions_value\" AS FLOAT)) AS conversion_value from public.anveshan_google_ads_city_level_campaign_data gads left join (select * from (select \"criteria id\" as city_code, name as city, row_number() over (partition by \"criteria id\" order by 1) as rw from public.s3_google_ads_geo_city_mapping where \"country code\" = \'IN\') where rw = 1 ) g on g.city_code = SPLIT_PART(\"segments.geo_target_city\", \'/\', 2) GROUP BY \"customer.id\", \"campaign.id\", \"campaign.name\", \"campaign.status\", \"segments.date\", \"segments.geo_target_city\", \"campaign.advertising_channel_type\", g.city",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select database, schema, "table" from SVV_TABLE_INFO limit 1
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            