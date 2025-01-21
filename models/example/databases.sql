{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table MapleMonk.zouk_db_Blinkit_ads_Fact_items as SELECT PARSE_DATE(\'%Y-%m-%d\',Date ) AS Date, SAFE_CAST(REPLACE(Estimated_Budget_Consumed, \',\', \'\') AS FLOAT64) AS Estimated_Budget_Consumed, SAFE_CAST(REPLACE(Total_RoAS, \',\', \'\') AS FLOAT64) AS Total_RoAS, SAFE_CAST(REPLACE(CPM, \',\', \'\') AS FLOAT64) AS CPM, SAFE_CAST(REPLACE(Direct_ATC, \',\', \'\') AS FLOAT64) AS Direct_ATC, SAFE_CAST(REPLACE(Direct_Quantities_Sold, \',\', \'\') AS FLOAT64) AS Direct_Quantities_Sold, SAFE_CAST(REPLACE(Direct_Sales, \',\', \'\') AS FLOAT64) AS Direct_Sales, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, ba.Campaign_Name, bm.collection FROM MapleMonk.zouk_db_Blinkit_Ads ba left join ( select * from MapleMonk.zouk_Blinkit qualify row_number() over(partition by lower(CAMPAIGN_NAME) order by 1) = 1 )bm on lower(ba.CAMPAIGN_NAME) = lower(bm.CAMPAIGN_NAME)",
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
            