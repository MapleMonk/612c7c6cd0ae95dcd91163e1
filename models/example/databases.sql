{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_Blinkit_ads_Fact_items; CREATE TABLE public.anveshan_Blinkit_ads_Fact_items AS WITH blinkit_ads AS ( SELECT COALESCE( TO_DATE(TRIM(ba.\"date\"), \'DD-MM-YYYY\'), TO_DATE(TRIM(ba.\"date\"), \'YYYY-MM-DD\'), TO_DATE(TRIM(ba.\"date\"), \'DD/MM/YYYY\') ) AS Date, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"estimated budget consumed\", \'[^0-9.]\')) AS NUMERIC) AS Estimated_Budget_Consumed, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"total roas\", \'[^0-9.]\')) AS NUMERIC) AS Total_RoAS, TRY_CAST(TRIM(REGEXP_REPLACE(ba.cpm, \'[^0-9.]\')) AS NUMERIC) AS CPM, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct atc\", \'[^0-9.]\')) AS NUMERIC) AS Direct_ATC, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct quantities sold\", \'[^0-9.]\')) AS NUMERIC) AS Direct_Quantities_Sold, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct sales\", \'[^0-9.]\')) AS NUMERIC) AS Direct_Sales, TRY_CAST(TRIM(REGEXP_REPLACE(ba.impressions, \'[^0-9.]\')) AS NUMERIC) AS Impressions, ba.\"campaign name\" as campaign_name, NULL AS collection, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"match type\", \'[^0-9.]\')) AS NUMERIC) AS CTR, NULL::NUMERIC AS Reach, ba.\"targeting type\" AS Match_Type, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"indirect atc\", \'[^0-9.]\')) AS NUMERIC) AS Unique_Clicks, ba.\"targeting type\" AS Targeting_Type, ba.\"targeting value\" AS Targeting_Value, \'regular\' AS Type FROM public.anveshan_blinkit_ads ba LEFT JOIN ( SELECT * FROM ( SELECT ba2.*, ROW_NUMBER() OVER ( PARTITION BY LOWER(ba2.\"campaign name\") ORDER BY ba2.\"date\" ) AS rn FROM public.anveshan_blinkit_ads ba2 ) subq WHERE subq.rn = 1 ) bm ON LOWER(ba.\"campaign name\") = LOWER(bm.\"campaign name\") ) SELECT Date, Estimated_Budget_Consumed, Total_RoAS, CPM, Direct_ATC, Direct_Quantities_Sold, Direct_Sales, Impressions, campaign_name, collection, CTR, Reach, Match_Type, Unique_Clicks AS clicks, Targeting_Type, Targeting_Value, Type FROM blinkit_ads;",
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
            