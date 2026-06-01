{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE or replace TABLE MapleMonk.Buildskill_Blinkit_ads_Fact_items AS WITH blinkit_data AS ( SELECT * FROM `maplemonk.Buildskill_Blinkit_Ads_ads` ), blinkit_ads AS ( SELECT COALESCE( SAFE.PARSE_DATE(\'%d-%m-%Y\', TRIM(ba.date)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(ba.date)), SAFE.PARSE_DATE(\'%d/%m/%Y\', TRIM(ba.date)) ) AS date, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.Estimated_Budget_Consumed, r\'[^0-9.]\', \'\')) AS FLOAT64) AS estimated_budget_consumed, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.total_roas, r\'[^0-9.]\', \'\')) AS FLOAT64) AS total_roas, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.cpm, r\'[^0-9.]\', \'\')) AS FLOAT64) AS cpm, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.direct_atc, r\'[^0-9.]\', \'\')) AS FLOAT64) AS direct_atc, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.Direct_Quantities_Sold, r\'[^0-9.]\', \'\')) AS NUMERIC) AS direct_quantities_sold, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.Indirect_Quantities_Sold, r\'[^0-9.]\', \'\')) AS NUMERIC) AS indirect_quantities_sold, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.direct_sales, r\'[^0-9.]\', \'\')) AS FLOAT64) AS direct_sales, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.indirect_sales, r\'[^0-9.]\', \'\')) AS FLOAT64) AS indirect_sales, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.impressions, r\'[^0-9.]\', \'\')) AS NUMERIC) AS impressions, ba.campaign_name AS campaign_name, NULL AS collection, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.match_type, r\'[^0-9.]\', \'\')) AS FLOAT64) AS ctr, CAST(NULL AS NUMERIC) AS reach, ba.targeting_type AS match_type, SAFE_CAST(TRIM(REGEXP_REPLACE(ba.indirect_atc, r\'[^0-9.]\', \'\')) AS NUMERIC) AS unique_clicks, ba.targeting_type AS targeting_type, ba.targeting_value AS targeting_value, report_type, ba._airbyte_normalized_at, REGEXP_EXTRACT(campaign_name, r\'[^-]+$\') AS product_id FROM blinkit_data ba ) SELECT date, product_id, estimated_budget_consumed, total_roas, cpm, direct_atc, direct_quantities_sold, indirect_quantities_sold, direct_sales, indirect_sales, impressions, b.campaign_name, collection, ctr, reach, match_type, unique_clicks as clicks, targeting_type, targeting_value, report_type, b._airbyte_normalized_at FROM blinkit_ads b ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            