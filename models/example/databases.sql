{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.Zepto_Sales_Fact_items AS SELECT _airbyte_unique_key, EAN, SAFE_CAST(MRP AS NUMERIC) AS MRP, City, SAFE_CAST(Date AS DATE) AS Date, SKU_Name, Brand_Name, SKU_Number, SKU_Category, SAFE_CAST(Selling_Price AS NUMERIC) AS Selling_Price, Manufacturer_ID, SKU_Sub_Category, Manufacturer_Name, SAFE_CAST(Gross_Selling_Value AS NUMERIC) AS Gross_Selling_Value, SAFE_CAST(Sales__Qty____Units AS INT64) AS Sales_Qty_Units, SAFE_CAST(Gross_Merchandise_Value AS NUMERIC) AS Gross_Merchandise_Value, FROM `MapleMonk.KAL_DB_Zepto_sales` ; CREATE OR REPLACE TABLE MapleMonk.KAL_DB_Zepto_Ads_Fact_Items AS WITH Sponsored_Display AS ( SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Display\' AS Type FROM `MapleMonk.KAL_db_zepto_sponsored_display` sd ), Sponsored_Products AS ( SELECT CAST(Atc AS INT64) AS Atc, CAST(Cpm AS FLOAT64) AS Cpm, CAST(Ctr AS FLOAT64) AS Ctr, PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(Roas AS FLOAT64) AS Roas, CAST(Spend AS FLOAT64) AS Spend, CAST(Clicks AS INT64) AS Clicks, CAST(Orders AS INT64) AS Orders, CAST(BrandID AS STRING) AS BrandID, CAST(Revenue AS FLOAT64) AS Revenue, CAST(BrandName AS STRING) AS BrandName, CAST(ProductID AS STRING) AS ProductID, CAST(Same_skus AS STRING) AS Same_skus, CAST(Other_skus AS STRING) AS Other_skus, CAST(Campaign_id AS STRING) AS Campaign_id, CAST(Impressions AS INT64) AS Impressions, CAST(ProductName AS STRING) AS ProductName, CAST(Campaign_name AS STRING) AS Campaign_name, \'Product\' AS Type FROM `MapleMonk.KAL_db_zepto_sponsored_products` sp ) SELECT Atc, Cpm, Ctr, Date, Roas, Spend, Clicks, Orders, BrandID, Revenue, BrandName, ProductID, Same_skus, Other_skus, Campaign_id, Impressions, ProductName, Campaign_name, Type FROM Sponsored_Display UNION ALL SELECT Atc, Cpm, Ctr, Date, Roas, Spend, Clicks, Orders, BrandID, Revenue, BrandName, ProductID, Same_skus, Other_skus, Campaign_id, Impressions, ProductName, Campaign_name, Type FROM Sponsored_Products;",
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
            