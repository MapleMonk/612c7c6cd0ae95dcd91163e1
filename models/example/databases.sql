{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE IFB_DB.MAPLEMONK.IFB_BLINKIT_ADS_FACT_ITEMS AS WITH blinkit_data AS ( SELECT *, DENSE_RANK() OVER ( PARTITION BY date, \"Campaign Name\" ORDER BY _airbyte_normalized_at DESC ) AS rank FROM IFB_DB.MAPLEMONK.IFB_Blinkit_Ads ), blinkit_ads AS ( SELECT COALESCE( TRY_TO_DATE(TRIM(ba.date), \'DD-MM-YYYY\'), TRY_TO_DATE(TRIM(ba.date), \'YYYY-MM-DD\'), TRY_TO_DATE(TRIM(ba.date), \'DD/MM/YYYY\') ) AS date, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Estimated Budget Consumed\", \'[^0-9.]\', \'\')) AS FLOAT ) AS estimated_budget_consumed, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Total RoAS\", \'[^0-9.]\', \'\')) AS FLOAT ) AS total_roas, TRY_CAST( TRIM(REGEXP_REPLACE(ba.CPM, \'[^0-9.]\', \'\')) AS FLOAT ) AS cpm, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Direct ATC\", \'[^0-9.]\', \'\')) AS FLOAT ) AS direct_atc, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Direct Quantities Sold\", \'[^0-9.]\', \'\')) AS NUMBER ) AS direct_quantities_sold, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Indirect Quantities Sold\", \'[^0-9.]\', \'\')) AS NUMBER ) AS indirect_quantities_sold, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Direct Sales\", \'[^0-9.]\', \'\')) AS FLOAT ) AS direct_sales, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Indirect Sales\", \'[^0-9.]\', \'\')) AS FLOAT ) AS indirect_sales, TRY_CAST( TRIM(REGEXP_REPLACE(ba.IMPRESSIONS, \'[^0-9.]\', \'\')) AS NUMBER ) AS impressions, ba.\"Campaign Name\" AS campaign_name, NULL AS collection, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Match Type\", \'[^0-9.]\', \'\')) AS FLOAT ) AS ctr, CAST(NULL AS NUMBER) AS reach, ba.\"Targeting Type\" AS match_type, TRY_CAST( TRIM(REGEXP_REPLACE(ba.\"Indirect ATC\", \'[^0-9.]\', \'\')) AS NUMBER ) AS unique_clicks, ba.\"Targeting Type\" AS targeting_type, ba.\"Targeting Value\" AS targeting_value, ba.REPORT_TYPE, ba._airbyte_normalized_at FROM blinkit_data ba WHERE ba.rank = 1 ) SELECT date, estimated_budget_consumed, total_roas, cpm, direct_atc, direct_quantities_sold, indirect_quantities_sold, direct_sales, indirect_sales, impressions, b.campaign_name, collection, ctr, reach, match_type, unique_clicks AS clicks, targeting_type, targeting_value, report_type, b._airbyte_normalized_at FROM blinkit_ads b;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from IFB_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            