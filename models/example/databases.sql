{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.STPL_FG_Inventory_Valuation AS WITH Inventory AS ( SELECT * FROM ( SELECT DATETIME(TIMESTAMP(Updated), \"Asia/Kolkata\") AS DATA_FETCH_DATE, INITCAP(Facility) AS Facility, TRIM(UPPER(Item_SkuCode)) AS SKU, CAST(Inventory AS FLOAT64) AS Available_Inventory, CAST(Open_Sale AS FLOAT64) AS Open_Sale, CAST(Open_Purchase AS FLOAT64) AS Open_Purchase, CAST(Bad_Inventory AS FLOAT64) AS Bad_Inventory, CAST(Putaway_Pending AS FLOAT64) AS Putaway_Pending, CAST(Inventory_Blocked AS FLOAT64) AS Inventory_Blocked, CAST(Stock_In_Transfer AS FLOAT64) AS Stock_In_Transfer, ROW_NUMBER() OVER ( PARTITION BY INITCAP(Facility), TRIM(UPPER(Item_SkuCode)) ORDER BY DATETIME(TIMESTAMP(Updated), \"Asia/Kolkata\") DESC ) AS rw FROM `MapleMonk.stlp_uc_get_inventory_snapshot_export_full_refresh` ) WHERE rw = 1 ), SKU_Mapping AS ( SELECT i.*, SM.Commonsku, SM.Category AS Product_Category, SM.Category_Code, SM.Collection, SM.name AS Product_Name_Final FROM Inventory i LEFT JOIN ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku) ORDER BY 1) AS rw FROM `MAPLEMONK.FINAL_SKU_MASTER` ) WHERE rw = 1 ) SM ON LOWER(i.SKU) = LOWER(SM.commonsku) ), Cogs_Map AS ( SELECT PARSE_DATE(\'%d-%b-%y\', START_DATE) AS START_DATE, PARSE_DATE(\'%d-%b-%y\', END_DATE) AS END_DATE, TRIM(LOWER(category_code)) AS category_code, CAST(REPLACE(cogs, \',\', \'\') AS FLOAT64) AS cogs FROM MapleMonk.zouk_db_sku_mrp_cogs QUALIFY ROW_NUMBER() OVER (PARTITION BY START_DATE, END_DATE, LOWER(category_code) ORDER BY 1) = 1 ), Final AS ( SELECT SM.*, CAT.COGS, SM.Available_Inventory * CAT.COGS AS Available_Inventory_Value, SM.Bad_Inventory * CAT.COGS AS Bad_Inventory_Value, SM.Putaway_Pending * CAT.COGS AS Putaway_Pending_Value, SM.Inventory_Blocked * CAT.COGS AS Blocked_Inventory_Value FROM SKU_Mapping SM LEFT JOIN Cogs_Map CAT ON Date(SM.DATA_Fetch_Date) BETWEEN CAT.START_DATE AND CAT.END_DATE AND TRIM(LOWER(SM.category_code)) = CAT.category_code ) SELECT * From Final",
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
            