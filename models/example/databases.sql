{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Ritualistic_Blinkit_ads_Fact_items AS SELECT COALESCE( SAFE.PARSE_DATE(\'%d-%m-%Y\', TRIM(Date)), SAFE.PARSE_DATE(\'%Y-%m-%d\', TRIM(Date)), SAFE.PARSE_DATE(\'%d/%m/%Y\', TRIM(Date)) ) AS Date, SAFE_CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, SAFE_CAST(REPLACE(Total_RoAS, \',\', \'\') AS FLOAT64) AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, SAFE_CAST(REPLACE(Direct_ATC, \',\', \'\') AS FLOAT64) AS Direct_ATC, SAFE_CAST(REPLACE(Direct_Quantities_Sold, \',\', \'\') AS FLOAT64) AS Direct_Quantities_Sold, SAFE_CAST(REPLACE(Direct_Sales, \',\', \'\') AS FLOAT64) AS Direct_Sales, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, Campaign_Name, NULL AS CTR, NULL AS Reach, CAST(NULL AS STRING) AS Match_Type, CAST(NULL AS INT64) AS Unique_Clicks, CAST(NULL AS STRING) AS Targeting_Type, CAST(NULL AS STRING) AS Targeting_Value, \'regular\' AS Type FROM MapleMonk.Ritualistic_Blinkit_Ads;",
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
            