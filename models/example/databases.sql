{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Inventory_Value_Report AS WITH Sales AS ( SELECT Date, SKU, PRODUCT_CATEGORY, COLLECTION, category_code, SUM(IFNULL(BAU_MRP_SALES,0)) - SUM(IFNULL(return_mrp_sales,0)) - SUM(IFNULL(BAU_DISCOUNT,0)) - SUM(IFNULL(TRADE_MARGIN,0)) + SUM(IFNULL(return_trade_margin,0)) - SUM(IFNULL(Returns,0)) - SUM(IFNULL(gst,0)) + SUM(IFNULL(Return_GST,0)) - SUM(IFNULL(cogs,0)) + SUM(IFNULL(return_cogs,0)) AS Gross_Margin FROM MapleMonk.zouk_pandl GROUP BY 1,2,3,4,5 ), Inventory AS ( SELECT DATA_FETCH_DATE AS Date, COMMONSKU, PRODUCT_CATEGORY, COLLECTION, category_code, SUM(Available_Inventory) AS Available_Inventory FROM MapleMonk.ZOUK_INVENTORY_FACT_ITEMS GROUP BY 1,2,3,4,5 ), Cogs_Map AS ( SELECT PARSE_DATE(\'%d-%b-%y\', START_DATE) AS START_DATE, PARSE_DATE(\'%d-%b-%y\', END_DATE) AS END_DATE, TRIM(LOWER(category_code)) AS category_code, CAST(REPLACE(cogs, \',\', \'\') AS FLOAT64) AS cogs FROM MapleMonk.zouk_db_sku_mrp_cogs QUALIFY ROW_NUMBER() OVER (PARTITION BY START_DATE, END_DATE, LOWER(category_code) ORDER BY 1) = 1 ), Final as ( SELECT COALESCE(I.Date,S.Date) AS Date, COALESCE(I.COMMONSKU,S.SKU) AS COMMONSKU, I.PRODUCT_CATEGORY, I.COLLECTION, COALESCE(I.category_code,S.category_code) AS Category_Code, I.Available_Inventory, S.Gross_Margin, CAT.cogs, I.Available_Inventory * CAT.cogs AS Inventory_Value FROM Inventory I FULL OUTER JOIN Sales S ON Date(I.Date) = S.Date AND TRIM(LOWER(I.commonsku)) = TRIM(LOWER(S.sku)) LEFT JOIN Cogs_Map CAT ON Date(I.Date) BETWEEN CAT.START_DATE AND CAT.END_DATE AND TRIM(LOWER(I.category_code)) = CAT.category_code ) SELECT f.*, FORMAT_DATE(\'%B\', f.Date) AS Month_Name, CASE WHEN EXTRACT(YEAR FROM f.Date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM f.Date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN EXTRACT(DAY FROM CURRENT_DATE()) ELSE EXTRACT(DAY FROM LAST_DAY(f.Date)) END AS Days_In_Month, Inventory_Value / CASE WHEN EXTRACT(YEAR FROM f.Date) = EXTRACT(YEAR FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM f.Date) = EXTRACT(MONTH FROM CURRENT_DATE()) THEN EXTRACT(DAY FROM CURRENT_DATE()) ELSE EXTRACT(DAY FROM LAST_DAY(f.Date)) END AS Avg_Inventory_Value FROM Final f",
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
            