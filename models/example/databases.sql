{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE snitch_db.maplemonk.logic_combined_inventory AS WITH store AS ( SELECT DATE::date AS date, LOT_MRP, PACKING, ITEM_MRP, LOT_CODE, ITEM_CF_1, ITEM_CF_2, ITEM_CF_3, ITEM_NAME, PACK_NAME as size, STOCK_QTY, LOT_NUMBER, SHADE_NAME, BRANCH_CODE, BRANCH_NAME, GODOWN_CODE, GODOWN_NAME, LOT_SPRATE1, ADDLITEMCODE , CARTON_STOCK, TRY_TO_DATE(LOT_PUR_DATE, \'DD/MM/YYYY\') AS LOT_PUR_DATE, LOGICUSERCODE as sku, LOT_SALE_RATE, ITEM_SALE_RATE, LOT_BASIC_RATE FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand WHERE DATE::date = CURRENT_DATE() ), warehouse_name AS ( SELECT DATE::date AS date, LOT_MRP, PACKING, ITEM_MRP, LOT_CODE, ITEM_CF_1, ITEM_CF_2, ITEM_CF_3, ITEM_NAME, PACK_NAME as size, STOCK_QTY, LOT_NUMBER, SHADE_NAME, BRANCH_CODE, BRANCH_NAME, GODOWN_CODE, GODOWN_NAME, LOT_SPRATE1, ADDLITEMCODE, CARTON_STOCK, TRY_TO_DATE(LOT_PUR_DATE, \'DD/MM/YYYY\') AS LOT_PUR_DATE, LOGICUSERCODE as sku, LOT_SALE_RATE, ITEM_SALE_RATE, LOT_BASIC_RATE FROM snitch_db.maplemonk.logicerp_warehouse_get_stock_in_hand WHERE DATE::date = CURRENT_DATE() ) SELECT * FROM store UNION ALL SELECT * FROM warehouse_name; create or replace table snitch_db.maplemonk.logic_final_inventory as SELECT LEFT(a.sku, LENGTH(a.sku) - CHARINDEX(\'-\', REVERSE(a.sku))) AS sku_group, c.price, c.category, c.sku_class, c.status, a.*, CASE WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 0 AND 30 THEN \'0-30 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 31 AND 60 THEN \'30-60 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 61 AND 90 THEN \'60-90 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 91 AND 120 THEN \'90-120 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 121 AND 150 THEN \'120-150 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 151 AND 180 THEN \'150-180 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 181 AND 210 THEN \'180-210 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 211 AND 240 THEN \'210-240 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 241 AND 270 THEN \'240-270 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 271 AND 300 THEN \'270-300 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 301 AND 330 THEN \'300-330 Days\' WHEN DATEDIFF(DAY, lot_pur_date, CURRENT_DATE) BETWEEN 331 AND 360 THEN \'330-360 Days\' ELSE \'360+ Days\' END as ageing_group FROM snitch_db.maplemonk.logic_combined_inventory a LEFT JOIN snitch_db.maplemonk.availability_master_v2 AS c ON UPPER(TRIM(LEFT(a.sku, LENGTH(a.sku) - CHARINDEX(\'-\', REVERSE(a.sku))))) = c.\"SKU_GROUP\";",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            