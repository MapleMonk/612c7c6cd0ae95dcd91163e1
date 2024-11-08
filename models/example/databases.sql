{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Daily_Vendor_Wise_Inward_Data AS SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', i.Date) AS Inward_Date, i.Vendor AS Vendor, i.SKU_ID AS SKU, SUM(IFNULL(SAFE_CAST(i.Inward_Quantity AS INT64), 0)) AS Total_Inward_Quantity, COALESCE(CAST(sku_data.category AS STRING), \'\') AS Product_Category, COALESCE(CAST(sku_data.name AS STRING), \'\') AS Product_Name, COALESCE(CAST(sku_data.collection AS STRING), \'\') AS COLLECTION, COALESCE(CAST(sku_data.print AS STRING), \'\') AS PRINT, COALESCE(CAST(sku_data.PRODUCT_TYPE AS STRING), \'\') AS PRODUCT_TYPE, COALESCE(CAST(sku_data.BAU_ONLINE AS STRING), \'\') AS BAU_ONLINE FROM MapleMonk.Inward_Data i LEFT JOIN ( SELECT * FROM ( SELECT marketplace_sku AS skucode, name, category, sub_category, category_code, collection, print, PRODUCT_TYPE, commonsku, BAU_OFFLINE, BAU_ONLINE, TAX_RATE, ROW_NUMBER() OVER (PARTITION BY marketplace_sku ORDER BY 1) AS rw FROM `zouk-wh.maplemonk.final_sku_master` ) WHERE rw = 1 ) AS sku_data ON i.SKU_ID = sku_data.skucode GROUP BY Inward_Date, Vendor, SKU, Product_Category, Product_Name, COLLECTION, PRINT, PRODUCT_TYPE, BAU_ONLINE ORDER BY Inward_Date, SKU;",
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
            