{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Miss_Chase_Zepto_Ads_Fact_Items AS SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, Category AS Category, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Display\' AS Type FROM `MapleMonk.Zepto_Ads_sponsored_display` union all SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, Category AS Category, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Product\' AS Type FROM `MapleMonk.Zepto_Ads_sponsored_products` union all SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, Category AS Category, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Brand\' AS Type FROM `MapleMonk.Zepto_Ads_sponsored_brands` red_Products;",
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
            