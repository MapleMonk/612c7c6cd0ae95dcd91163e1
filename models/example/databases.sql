{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_production_sales_report AS SELECT commonsku AS SKU, PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, COLLECTION, PRINT, PRODUCT_TYPE, SUM(CASE WHEN order_date >= DATE_TRUNC(CURRENT_DATE(), MONTH) AND order_date < CURRENT_DATE() THEN QUANTITY ELSE 0 END) AS Quantity_Sold_MTD, SUM(CASE WHEN order_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN QUANTITY ELSE 0 END) AS Quantity_Sold_Last, SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN QUANTITY ELSE 0 END) AS Quantity_Sold_30, SUM(CASE WHEN order_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN QUANTITY ELSE 0 END) AS Quantity_Sold_90 FROM `MapleMonk.zouk_sales_consolidated` GROUP BY 1,2,3,4,5,6",
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
            