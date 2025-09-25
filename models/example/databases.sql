{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.HealthyMaster_Amazon_ads_Fact_Items AS SELECT CAST(REPLACE(REPLACE(CPC, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS CPC, CAST(CTR AS FLOAT64) AS CTR, CAST(ACOS AS FLOAT64) AS ACOS, CAST(ROAS AS FLOAT64) AS ROAS, CAST(Type AS STRING) AS Type, CAST(REPLACE(REPLACE(Sales, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Sales, CAST(REPLACE(REPLACE(Spend, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Spend, CAST(State AS STRING) AS State, CAST(REPLACE(REPLACE(Budget, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Budget, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(Status AS STRING) AS Status, CAST(Country AS STRING) AS Country, CAST(End_date AS STRING) AS End_date, CAST(Campaigns AS STRING) AS Campaigns, CAST(Portfolio AS STRING) AS Portfolio, CAST(Targeting AS STRING) AS Targeting, FORMAT_DATE(\'%Y-%m-%d\', PARSE_DATE(\'%d/%m/%Y\', Start_date)) AS Start_date, CAST(Impressions AS INT64) AS Impressions, CAST(REPLACE(REPLACE(CPC__converted_, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS CPC_converted_, CAST(REPLACE(REPLACE(Sales__converted_, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Sales_converted, CAST(REPLACE(REPLACE(Spend__converted_, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Spend_converted, CAST(REPLACE(REPLACE(Budget__converted_, \'â‚¹\', \'\'), \',\', \'\') AS FLOAT64) AS Budget_converted, CAST(Top_of_search_bid_adjustment AS FLOAT64) AS Top_of_search_bid_adjustment, CAST(Top_of_search_impression_share AS STRING) AS Top_of_search_impression_share, from `maplemonk.Google_sheet_HM_AD_Amazon_ads`;",
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
            