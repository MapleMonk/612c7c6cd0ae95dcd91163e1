{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Inventory_Aging_Report AS WITH latest_inventory AS ( SELECT SKU, Available_Inventory, ROW_NUMBER() OVER ( PARTITION BY SKU ORDER BY DATA_FETCH_DATE DESC ) AS rn FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` ), Inventory AS ( SELECT SKU, SUM(Available_Inventory) AS current_inventory FROM latest_inventory WHERE rn = 1 GROUP BY SKU ) , Inward AS ( SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', Date) AS inward_date, SKU_ID AS SKU, SUM(SAFE_CAST(Inward_Quantity AS FLOAT64)) AS inward_qty FROM `MapleMonk.Inward_Data` GROUP BY inward_date, SKU ), ordered_inward AS ( SELECT i.SKU, i.inward_date, i.inward_qty, inv.current_inventory, SUM(i.inward_qty) OVER ( PARTITION BY i.SKU ORDER BY i.inward_date DESC ) AS cumulative_inward FROM Inward i JOIN Inventory inv ON i.SKU = inv.SKU ), final_allocation AS ( SELECT SKU, inward_date, inward_qty, current_inventory, cumulative_inward, CASE WHEN cumulative_inward - inward_qty >= current_inventory THEN 0 WHEN cumulative_inward > current_inventory THEN current_inventory - (cumulative_inward - inward_qty) ELSE inward_qty END AS inventory_qty FROM ordered_inward ) SELECT ift.SKU, SM.COMMONSKU, SM.Category AS PRODUCT_CATEGORY, SM.COLLECTION, SM.PRINT, SM.Name AS PRODUCT_NAME_FINAL, Inward_Date, inward_qty AS Inward_Quantity, inventory_qty AS Inventory, CASE WHEN inventory_qty = 0 THEN NULL ELSE DATE_DIFF(CURRENT_DATE(), inward_date, DAY) END AS inventory_age_days, CASE WHEN inventory_qty = 0 THEN NULL WHEN DATE_DIFF(CURRENT_DATE(), inward_date, DAY) BETWEEN 0 AND 30 THEN \'0-30\' WHEN DATE_DIFF(CURRENT_DATE(), inward_date, DAY) BETWEEN 31 AND 60 THEN \'31-60\' WHEN DATE_DIFF(CURRENT_DATE(), inward_date, DAY) BETWEEN 61 AND 90 THEN \'61-90\' ELSE \'90+\' END AS age_bucket FROM final_allocation ift LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY commonsku ORDER BY 1) AS rw FROM MAPLEMONK.FINAL_SKU_MASTER ) WHERE rw = 1 ) SM ON LOWER(IFT.SKU) = LOWER(SM.commonsku) ORDER BY SKU, inward_date DESC ;",
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
            