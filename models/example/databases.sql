{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.eat_anytime_Blinkit_ads_Fact_items AS WITH blinkit_ads AS ( SELECT COALESCE( SAFE.PARSE_DATE(\'%d-%m-%Y\', TRIM(Date)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(Date)), SAFE.PARSE_DATE(\'%d/%m/%Y\', TRIM(Date)) ) AS Date, SAFE_CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, SAFE_CAST(REPLACE(Total_RoAS, \',\', \'\') AS FLOAT64) AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, SAFE_CAST(REPLACE(Direct_ATC, \',\', \'\') AS FLOAT64) AS Direct_ATC, SAFE_CAST(REPLACE(Direct_Quantities_Sold, \',\', \'\') AS FLOAT64) AS Direct_Quantities_Sold, SAFE_CAST(REPLACE(Direct_Sales, \',\', \'\') AS FLOAT64) AS Direct_Sales, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, ba.Campaign_Name, null AS collection, NULL AS CTR, NULL AS Reach, CAST(NULL AS STRING) AS Match_Type, CAST(NULL AS INT64) AS Unique_Clicks, CAST(NULL AS STRING) AS Targeting_Type, CAST(NULL AS STRING) AS Targeting_Value, \'regular\' AS Type FROM MapleMonk.eat_anytime_blinkit_ads ba ) SELECT Date, Estimated_Budget_Consumed, Total_RoAS, CPM, Direct_ATC, Direct_Quantities_Sold, Direct_Sales, Impressions, Campaign_Name, collection, CTR, Reach, Match_Type, Unique_Clicks, Targeting_Type, Targeting_Value, Type FROM blinkit_ads ;",
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
            