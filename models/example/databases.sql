{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_db_Blinkit_ads_Fact_items AS WITH blinkit_ads AS ( SELECT PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, SAFE_CAST(REPLACE(Total_RoAS, \',\', \'\') AS FLOAT64) AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, SAFE_CAST(REPLACE(Direct_ATC, \',\', \'\') AS FLOAT64) AS Direct_ATC, SAFE_CAST(REPLACE(Direct_Quantities_Sold, \',\', \'\') AS FLOAT64) AS Direct_Quantities_Sold, SAFE_CAST(REPLACE(Direct_Sales, \',\', \'\') AS FLOAT64) AS Direct_Sales, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, ba.Campaign_Name, bm.collection, NULL AS CTR, NULL AS Reach, CAST(NULL AS STRING) AS Match_Type, CAST(NULL AS INT64) AS Unique_Clicks, CAST(NULL AS STRING) AS Targeting_Type, CAST(NULL AS STRING) AS Targeting_Value, \'regular\' AS Type FROM MapleMonk.zouk_db_Blinkit_Ads ba LEFT JOIN ( SELECT * FROM MapleMonk.zouk_Blinkit QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(CAMPAIGN_NAME) ORDER BY 1) = 1 ) bm ON LOWER(ba.Campaign_Name) = LOWER(bm.Campaign_Name) ), Banner_Ads AS ( SELECT PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, NULL AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, NULL AS Direct_ATC, NULL AS Direct_Quantities_Sold, NULL AS Direct_Sales, CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, Campaign_Name, CAST(NULL AS STRING) AS collection, CAST(REPLACE(CTR, \',\', \'\') AS FLOAT64) AS CTR, CAST(REPLACE(Reach, \',\', \'\') AS FLOAT64) AS Reach, SAFE_CAST(REPLACE(Match_Type, \',\',\'\' )AS STRING) AS Match_Type, CAST(REPLACE(Unique_Clicks, \',\', \'\') AS INT64) AS Unique_Clicks, Targeting_Type, Targeting_Value, \'banner\' AS Type FROM `MapleMonk.Zouk_Blinkit_Banner_Ads` ) SELECT Date, Estimated_Budget_Consumed, Total_RoAS, CPM, Direct_ATC, Direct_Quantities_Sold, Direct_Sales, Impressions, Campaign_Name, collection, CTR, Reach, Match_Type, Unique_Clicks, Targeting_Type, Targeting_Value, Type FROM blinkit_ads UNION ALL SELECT Date, Estimated_Budget_Consumed, Total_RoAS, CPM, Direct_ATC, Direct_Quantities_Sold, Direct_Sales, Impressions, Campaign_Name, collection, CTR, Reach, Match_Type, Unique_Clicks, Targeting_Type, Targeting_Value, Type FROM Banner_Ads;",
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
            