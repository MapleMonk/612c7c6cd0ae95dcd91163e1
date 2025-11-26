{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Inventory_Availability_Scorecard AS WITH sku_sales_data AS ( SELECT commonsku, product_category, SUM(selling_price) AS total_sales, SUM(quantity) AS total_quantity FROM `Maplemonk.zouk_secondary_sales_consolidated` WHERE order_Date BETWEEN DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 1 DAY) AND LOWER(IFNULL(ORDER_STATUS,\'\')) NOT LIKE \'%cancel%\' GROUP BY 1, 2 ), weights as ( SELECT commonsku, product_category, total_sales as Total_Sales_L90, total_quantity as Total_Quantity_L90, SUM(total_sales) over() as sku_total_sales, SUM(total_sales) OVER(PARTITION BY product_category) as category_total_sales, ROUND( SAFE_DIVIDE( total_sales, SUM(total_sales) OVER() ), 4 ) AS sku_weightage, ROUND( SAFE_DIVIDE( total_sales, SUM(total_sales) OVER(PARTITION BY product_category) ), 4 ) AS category_weightage FROM sku_sales_data ORDER BY product_category, category_weightage DESC ) , Inventory as ( SELECT DATA_FETCH_DATE, COMMONSKU, PRODUCT_CATEGORY, Available_Inventory, Availability AS Availability_Flag FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` ) SELECT i.*, w.Total_Sales_L90, w.Total_Quantity_L90, w.category_Weightage, w.sku_Weightage, i.Availability_Flag * w.category_Weightage AS Category_Availability_Score, i.Availability_Flag * w.sku_Weightage AS SKU_Availability_Score FROM Inventory i LEFT JOIN weights w ON lower(i.commonsku) = lower(w.commonsku) AND lower(i.product_category) = lower(w.product_category) ; CREATE OR REPLACE TABLE MapleMonk.zouk_Inventory_Scorecard_Category AS SELECT DATA_FETCH_DATE, PRODUCT_CATEGORY, sum(Total_Sales_L90) AS Total_Sales_L90, sum(Total_Quantity_L90) AS Total_Quantity_L90, sum(Category_Availability_Score) AS Category_Availability_Score FROM MapleMonk.zouk_Inventory_Availability_Scorecard GROUP BY 1,2 ;",
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
            