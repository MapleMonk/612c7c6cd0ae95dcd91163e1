{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table MapleMonk.zouk_db_Zepto_Fact_items as SELECT PARSE_DATE(\'%Y-%m-%d\', \'2025-01-15\') AS Date, SAFE_CAST(REPLACE(Spend, \',\', \'\') AS FLOAT64) AS Spend, SAFE_CAST(REPLACE(Roas, \',\', \'\') AS FLOAT64) AS Roas, SAFE_CAST(REPLACE(Ctr, \',\', \'\') AS FLOAT64) AS Ctr, SAFE_CAST(REPLACE(Cpc, \',\', \'\') AS FLOAT64) AS Cpc, SAFE_CAST(REPLACE(Atc, \',\', \'\') AS FLOAT64) AS Atc, SAFE_CAST(REPLACE(Clicks, \',\', \'\') AS FLOAT64) AS Clicks, SAFE_CAST(REPLACE(Orders, \',\', \'\') AS FLOAT64) AS Orders, BrandID, SAFE_CAST(REPLACE(Revenue, \',\', \'\') AS FLOAT64) AS Revenue, BrandName, ProductID, Campaign_id, SAFE_CAST(REPLACE(Impressions, \',\', \'\') AS FLOAT64) AS Impressions, ProductName, Campaign_name, fsm.category, fsm.collection FROM MapleMonk.zouk_db_Zepto_Ads za LEFT JOIN ( SELECT * FROM maplemonk.final_sku_master QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(Channel_Product_Id) ORDER BY IFNULL(collection, \'\') DESC) = 1 ) fsm ON LOWER(fsm.Channel_Product_Id) = LOWER(za.ProductID);",
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
            