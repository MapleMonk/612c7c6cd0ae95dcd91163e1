{{ config(
            materialized='table',
                post_hook={
                    "sql": "DROP TABLE IF EXISTS public.anveshan_Blinkit_ads_Fact_items; CREATE TABLE public.anveshan_Blinkit_ads_Fact_items AS WITH blinkit_ads AS ( SELECT COALESCE( TO_DATE(TRIM(ba.\"date\"), \'DD-MM-YYYY\'), TO_DATE(TRIM(ba.\"date\"), \'YYYY-MM-DD\'), TO_DATE(TRIM(ba.\"date\"), \'DD/MM/YYYY\') ) AS Date, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"estimated budget consumed\", \'[^0-9.]\')) AS NUMERIC) AS Estimated_Budget_Consumed, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"total roas\", \'[^0-9.]\')) AS NUMERIC) AS Total_RoAS, TRY_CAST(TRIM(REGEXP_REPLACE(ba.cpm, \'[^0-9.]\')) AS NUMERIC) AS CPM, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct atc\", \'[^0-9.]\')) AS NUMERIC) AS Direct_ATC, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct quantities sold\", \'[^0-9.]\')) AS NUMERIC) AS Direct_Quantities_Sold, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"direct sales\", \'[^0-9.]\')) AS NUMERIC) AS Direct_Sales, TRY_CAST(TRIM(REGEXP_REPLACE(ba.impressions, \'[^0-9.]\')) AS NUMERIC) AS Impressions, ba.\"campaign name\" as campaign_name, NULL AS collection, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"match type\", \'[^0-9.]\')) AS NUMERIC) AS CTR, NULL::NUMERIC AS Reach, ba.\"targeting type\" AS Match_Type, TRY_CAST(TRIM(REGEXP_REPLACE(ba.\"indirect atc\", \'[^0-9.]\')) AS NUMERIC) AS Unique_Clicks, ba.\"targeting type\" AS Targeting_Type, ba.\"targeting value\" AS Targeting_Value, \'regular\' AS Type FROM public.blinkit_anveshan_ads ba where upper(ba.report_type) in (\'PRODUCT_LISTING\',\'PRODUCT_RECOMMENDATION\') qualify row_number() over (partition by date,report_type,campaign_name,\"pacing type\",Targeting_Type,Targeting_Value,\"match type\" order by date desc, _airbyte_emitted_at desc ) =1 ) SELECT Date, Estimated_Budget_Consumed, Total_RoAS, CPM, Direct_ATC, Direct_Quantities_Sold, Direct_Sales, Impressions, campaign_name, collection, CTR, Reach, Match_Type, Unique_Clicks AS clicks, Targeting_Type, Targeting_Value, Type FROM blinkit_ads;",
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
            