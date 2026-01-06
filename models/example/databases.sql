{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Inventory_Availability_Scorecard AS WITH sku_sales_data AS ( SELECT commonsku, collection, product_category, SUM(selling_price) AS total_sales, SUM(quantity) AS total_quantity FROM `Maplemonk.zouk_secondary_sales_consolidated` WHERE order_Date BETWEEN DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(\'Asia/Kolkata\'), INTERVAL 1 DAY) AND LOWER(IFNULL(ORDER_STATUS,\'\')) NOT LIKE \'%cancel%\' GROUP BY 1, 2,3 ), weights as ( SELECT commonsku, collection, product_category, total_sales as Total_Sales_L90, total_quantity as Total_Quantity_L90, SUM(total_sales) over() as sku_total_sales, SUM(total_sales) OVER(PARTITION BY collection) AS collection_total_sales, SUM(total_sales) OVER(PARTITION BY product_category) as category_total_sales, ROUND( SAFE_DIVIDE( total_sales, SUM(total_sales) OVER() ), 4 ) AS sku_weightage, ROUND( SAFE_DIVIDE( total_sales, SUM(total_sales) OVER(PARTITION BY collection) ), 4 ) AS collection_weightage, ROUND( SAFE_DIVIDE( total_sales, SUM(total_sales) OVER(PARTITION BY product_category) ), 4 ) AS category_weightage FROM sku_sales_data ORDER BY product_category, category_weightage DESC ) , Inventory as ( SELECT DATA_FETCH_DATE, COMMONSKU, PRODUCT_CATEGORY, COLLECTION, PRODUCT_TYPE, Product_Final_Name, Available_Inventory, Availability AS Availability_Flag FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` ) SELECT i.*, w.Total_Sales_L90, w.Total_Quantity_L90, w.category_Weightage, w.collection_Weightage, w.sku_Weightage, i.Availability_Flag * w.category_Weightage * LEAST(safe_divide(Available_Inventory,Total_Quantity_L90) * 90,1) AS Category_Availability_Score, i.Availability_Flag * w.collection_Weightage * LEAST(safe_divide(Available_Inventory,Total_Quantity_L90) * 90,1) AS Collection_Availability_Score, i.Availability_Flag * w.sku_Weightage * LEAST(safe_divide(Available_Inventory,Total_Quantity_L90) * 90,1) AS SKU_Availability_Score FROM Inventory i LEFT JOIN weights w ON lower(i.commonsku) = lower(w.commonsku) AND lower(i.product_category) = lower(w.product_category) AND lower(i.collection) = lower(w.collection) ; CREATE OR REPLACE TABLE MapleMonk.zouk_Inventory_Scorecard_Category AS SELECT DATA_FETCH_DATE, PRODUCT_CATEGORY, COLLECTION, PRODUCT_TYPE, sum(Total_Sales_L90) AS Total_Sales_L90, sum(Total_Quantity_L90) AS Total_Quantity_L90, sum(Category_Availability_Score) AS Category_Availability_Score FROM MapleMonk.zouk_Inventory_Availability_Scorecard GROUP BY 1,2,3,4 ;",
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
            