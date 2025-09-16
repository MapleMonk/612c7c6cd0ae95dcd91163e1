{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.HealthyMaster_Zepto_Ads_Fact_Items AS WITH Sponsored_Display AS ( SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Display\' AS Type FROM `MapleMonk.zepto_HM_sponsored_display` sd ), Sponsored_Products AS ( SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Product\' AS Type FROM `MapleMonk.zepto_HM_sponsored_products` sp ) SELECT Atc, Cpm, Ctr, Date, Roas, Spend, Clicks, Orders, BrandID, Revenue, BrandName, ProductID, Same_skus, Other_skus, Campaign_id, Impressions, ProductName, Campaign_name, Type FROM Sponsored_Display UNION ALL SELECT Atc, Cpm, Ctr, Date, Roas, Spend, Clicks, Orders, BrandID, Revenue, BrandName, ProductID, Same_skus, Other_skus, Campaign_id, Impressions, ProductName, Campaign_name, Type FROM Sponsored_Products;",
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
            