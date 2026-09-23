{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `Maplemonk.Zouk_Flipkart_New_Inventory_Fact_Items` AS WITH inv AS ( SELECT PARSE_DATE(\'%d-%m-%Y\', i.Data_Fetch_Date) AS DATA_FETCH_DATE, SAFE_CAST(i.SOH AS FLOAT64) AS SOH, sku.CHANNEL_PRODUCT_ID, sku.COMMONSKU, sku.Name, sku.Category FROM `Maplemonk.ZOUK_FLIPKART_NEW_INVENTORY` i LEFT JOIN ( SELECT CHANNEL_PRODUCT_ID, COMMONSKU, Name, Category FROM `Maplemonk.Final_SKU_Master` WHERE MARKETPLACE = \'FLIPKART\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY CHANNEL_PRODUCT_ID ORDER BY CASE WHEN COMMONSKU IS NOT NULL THEN 0 ELSE 1 END ) = 1 ) sku ON sku.CHANNEL_PRODUCT_ID LIKE CONCAT(\'%\', i.INV_FSN, \'%\') ), sales AS ( SELECT commonsku, order_date AS ORDER_DATE, SUM(IFNULL(SAFE_CAST(quantity AS FLOAT64), 0)) AS QUANTITY FROM `MapleMonk.zouk_Secondary_sales_consolidated` WHERE Marketplace = \'FLIPKART\' GROUP BY 1, 2 ) SELECT inv.DATA_FETCH_DATE, inv.Name, inv.Category, ROUND(IFNULL(inv.SOH, 0), 0) AS SOH, inv.CHANNEL_PRODUCT_ID, inv.COMMONSKU, ROUND(IFNULL( (SELECT SUM(s.QUANTITY) FROM sales s WHERE s.commonsku = inv.COMMONSKU AND DATE_DIFF(inv.DATA_FETCH_DATE, s.ORDER_DATE, DAY) BETWEEN 1 AND 30), 0 ), 0) AS TOTAL_QTY_SOLD_30D, ROUND(IFNULL( SAFE_DIVIDE( inv.SOH, (SELECT SUM(s.QUANTITY) FROM sales s WHERE s.commonsku = inv.COMMONSKU AND DATE_DIFF(inv.DATA_FETCH_DATE, s.ORDER_DATE, DAY) BETWEEN 1 AND 30) ), 0 ), 0) AS DOH_30D, ROUND(IFNULL( SAFE_DIVIDE( IFNULL( SAFE_DIVIDE( inv.SOH, (SELECT SUM(s.QUANTITY) FROM sales s WHERE s.commonsku = inv.COMMONSKU AND DATE_DIFF(inv.DATA_FETCH_DATE, s.ORDER_DATE, DAY) BETWEEN 1 AND 30) ), 0 ), 30 ), 0 ), 0) AS DRR_L30D FROM inv ORDER BY inv.DATA_FETCH_DATE DESC;",
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
            