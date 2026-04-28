{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.PHILIPS_BLINKIT_FACT_ITEMS AS SELECT DISTINCT CONCAT( MRP, \'-\', ITEM_ID, CITY_NAME, \'-\', CITY_ID, \'-\', \'-\', CAST(QTY_SOLD AS FLOAT), DATE ) AS ORDER_ID, TO_DATE(DATE) AS ORDER_DATE, CAST(MRP AS FLOAT) AS MRP, CITY_NAME AS CITY, ITEM_ID AS PRODUCT_ID, CAST(REPLACE(QTY_SOLD, \'.0\', \'\') AS INTEGER) AS QUANTITY, ITEM_NAME AS PRODUCT_NAME, UPPER(CATEGORY) AS PRODUCT_CATEGORY FROM MAPLEMONK.PHILIPS_BLINKIT_SALES_PARTNER_BIZ UNION ALL SELECT DISTINCT CONCAT( MRP, \'-\', ITEM_ID, CITY_NAME, \'-\', CITY_ID, \'-\', \'-\', CAST(QTY_SOLD AS FLOAT), DATE ) AS ORDER_ID, TO_DATE(DATE) AS ORDER_DATE, CAST(MRP AS FLOAT) AS MRP, CITY_NAME AS CITY, ITEM_ID AS PRODUCT_ID, CAST(REPLACE(QTY_SOLD, \'.0\', \'\') AS INTEGER) AS QUANTITY, ITEM_NAME AS PRODUCT_NAME, UPPER(CATEGORY) AS PRODUCT_CATEGORY FROM MAPLEMONK.PH_BLINKIT_SALES_HISTORICAL H WHERE (H.DATE, H.ITEM_ID) NOT IN ( SELECT DATE, ITEM_ID FROM MAPLEMONK.PHILIPS_BLINKIT_SALES_PARTNER_BIZ );",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from philips_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            