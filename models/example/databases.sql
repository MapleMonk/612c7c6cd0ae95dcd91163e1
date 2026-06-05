{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.ATOVIO_BLINKIT_DARKSTORE_FACTITEMS AS WITH DARK_STORE AS ( SELECT DATE(FORMAT_DATE(\'%Y-%m-%d\',CAST(SUBSTR(DATE, 1, 10) AS DATE))) ORDER_DATE ,COALESCE(Item_ID,item_id_1) ITEM_ID ,Darkstore_name DARK_STORE_NAME ,Serving_warehouse WAREHOUSE ,sum(cast(Available__Yes_No_ as int64)) Available_Qty ,SUM(CAST(MRP AS FLOAT64)) AS TOTAL_SALES ,SUM(COALESCE(CAST(NULLIF(Total_orders, \'\') AS INT64), 0)) AS TOTAL_ORDERS FROM `Maplemonk.Product_DarkStore_Report_product_dark_store_report` WHERE Darkstore_remark !=\'This store has been closed.\' GROUP BY 1,2,3,4 ), blinkit_inventory as ( select cast(created_date as date) date, item_id, Warehouse_Facility_Name, sum(cast(Total_sellable as int)) as inventory from Maplemonk.atovio_blinkit_seller_inventory group by 1,2,3 ) SELECT DS.*, COALESCE(BI.INVENTORY,0) INVENTORY FROM DARK_STORE DS LEFT JOIN BLINKIT_INVENTORY BI ON DATE(DS.ORDER_DATE) = DATE(BI.DATE) AND DS.ITEM_ID = BI.ITEM_ID AND UPPER(TRIM(REGEXP_REPLACE(DS.WAREHOUSE,r\'\s*-\s*.*$| Feeder$\',\'\'))) = UPPER(TRIM(REGEXP_REPLACE(BI.Warehouse_Facility_Name,r\'\s*-\s*.*$| Feeder$\',\'\'))) ;",
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
            