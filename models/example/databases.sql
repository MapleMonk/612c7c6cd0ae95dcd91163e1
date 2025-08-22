{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_D2C_Inventory_Report AS WITH Sales_90 AS ( SELECT COMMONSKU, SUM(IFNULL(quantity,0)) AS Quantity_90, SUM(IFNULL(BAU_MRP_SALES,0)) - SUM(IFNULL(return_mrp_sales,0)) - SUM(IFNULL(BAU_DISCOUNT,0)) - SUM(IFNULL(TRADE_MARGIN,0)) + SUM(IFNULL(return_trade_margin,0)) - SUM(IFNULL(Returns,0)) - SUM(IFNULL(gst,0)) + SUM(IFNULL(Return_GST,0)) AS Net_Sales_90 FROM `MapleMonk.zouk_pandl_DTC` WHERE Date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) GROUP BY 1 ), Availability_90 AS ( SELECT COMMONSKU, COUNTIF(Available_Inventory > 1) AS Available_Days_90, COUNT(*) AS Total_Days_90 FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` WHERE DATA_FETCH_DATE BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) GROUP BY 1 ) SELECT ti.DATA_FETCH_DATE, ti.COMMONSKU, ti.Category_Code, ti.Collection, ti.Product_Category, ti.Print, ti.Available_Inventory, IFNULL(ti.Sold_Quantity_30_Days,0) AS Sales_Quantity_30, IFNULL(av.Available_Days_90,0) AS Available_Days_90, IFNULL(sa.Quantity_90,0) AS Sales_Quantity_90, IFNULL(sa.Net_Sales_90,0) AS Net_Sales_90 FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` ti LEFT JOIN Sales_90 sa ON LOWER(ti.COMMONSKU) = LOWER(sa.COMMONSKU) LEFT JOIN Availability_90 av ON LOWER(ti.COMMONSKU) = LOWER(av.COMMONSKU)",
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
            