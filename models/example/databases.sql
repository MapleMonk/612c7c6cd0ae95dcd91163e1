{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Top_SKU_Analysis AS WITH WIP_Latest AS ( SELECT LOWER(CAST(Item_id AS STRING)) AS SKU, MAX(SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date)) AS Latest_Date FROM MapleMonk.Zouk_Zouk_S3_TranZact_WIP WHERE Item_id IS NOT NULL GROUP BY SKU ), WIP_Final AS ( SELECT LOWER(CAST(w.Item_id AS STRING)) AS SKU, SUM(SAFE_CAST(w.Estimated_Quantity AS INT64) - SAFE_CAST(w.Quantity_Produced AS INT64)) AS WIP FROM MapleMonk.Zouk_Zouk_S3_TranZact_WIP w JOIN WIP_Latest l ON LOWER(CAST(w.Item_id AS STRING)) = l.SKU AND SAFE.PARSE_DATE(\'%Y-%m-%d\', w.Data_Updated_Date) = l.Latest_Date GROUP BY SKU ), Inventory AS ( SELECT PARSE_DATE(\'%Y-%m-%d\', Date) AS Date, CAST(i.COMMONSKU AS STRING) AS COMMONSKU, SM.Name AS FINAL_PRODUCT_NAME, SM.print AS PRINT, SM.Category AS PRODUCT_CATEGORY, SM.Category_Code, SM.COLLECTION, SM.PRODUCT_TYPE, CAST(Rank AS INT64) AS Rank, CAST(Target_Stock AS INT64) AS Target_Stock, CAST(Estimated_DRR AS INT64) AS Estimated_DRR, IFT.Available_Inventory, w.WIP AS WIP_Inventory FROM `MapleMonk.Zouk_Inventory_Norms` i LEFT JOIN WIP_Final w ON LOWER(i.COMMONSKU) = w.SKU LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY commonsku ORDER BY 1) AS rw FROM MAPLEMONK.FINAL_SKU_MASTER ) WHERE rw = 1 ) SM ON LOWER(i.COMMONSKU) = LOWER(SM.commonsku) LEFT JOIN ( SELECT DATA_FETCH_DATE, SKU, SUM(Available_Inventory) AS Available_Inventory FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` GROUP BY 1,2 ) IFT ON LOWER(i.COMMONSKU) = LOWER(IFT.SKU) and PARSE_DATE(\'%Y-%m-%d\', i.Date) = Date(IFT.DATA_FETCH_DATE) ) select Date, CAST(i.COMMONSKU AS STRING) AS COMMONSKU, FINAL_PRODUCT_NAME, PRINT, PRODUCT_CATEGORY, Category_Code, COLLECTION, PRODUCT_TYPE, Rank, sum(ifnull(Target_Stock,0))AS Target_Stock, sum(ifnull(Estimated_DRR,0)) AS Estimated_DRR, sum(ifnull(Available_Inventory,0)) as Available_Inventory, sum(ifnull(i.WIP_Inventory,0)) as WIP_Inventory, greatest(greatest(sum(ifnull(target_stock,0)) - sum(ifnull(Available_Inventory,0)),0) - sum(ifnull(wip_inventory,0)),0) as WIP_Gap, greatest(sum(ifnull(target_stock,0)) - sum(ifnull(Available_Inventory,0)),0) AS Gap from Inventory i group by 1,2,3,4,5,6,7,8,9",
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
            