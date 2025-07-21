{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Marketplace_Inventory_Report AS WITH Inventory AS ( SELECT DATA_FETCH_DATE, category_code, TRIM(UPPER(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY, SUM(IFNULL(Available_Inventory,0)) AS Available_Inventory, SAFE_DIVIDE(SUM(IFNULL(Available_Inventory, 0)), SUM(IFNULL(Sold_Quantity_30_Days, 0))/30) AS DOH_30_Days FROM `MapleMonk.ZOUK_INVENTORY_FACT_ITEMS` GROUP BY 1,2,3 ) , Sales_Data AS ( SELECT Date, TRIM(UPPER(Marketplace)) AS Marketplace, TRIM(UPPER(FINAL_MARKETPLACE)) AS Final_Marketplace, TRIM(UPPER(Marketplace_Segment)) AS Marketplace_Segment, SKU, COMMONSKU, category_code, TRIM(UPPER(PRODUCT_CATEGORY)) AS PRODUCT_CATEGORY, TRIM(UPPER(Collection)) AS Collection, SUM(IFNULL(SALES,0)) AS Sales, SUM(IFNULL(Spend,0)) AS Spends, SUM(IFNULL(quantity,0)) AS Quantity, SUM(IFNULL(BAU_MRP_SALES,0)) AS BAU_MRP_SALES, SUM(IFNULL(return_mrp_sales,0)) AS return_mrp_sales, SUM(IFNULL(BAU_DISCOUNT,0)) AS BAU_DISCOUNT, SUM(IFNULL(TRADE_MARGIN,0)) AS TRADE_MARGIN, SUM(IFNULL(return_trade_margin,0)) AS return_trade_margin, SUM(IFNULL(Returns,0)) AS Returns, SUM(IFNULL(gst,0)) AS gst, SUM(IFNULL(Return_GST,0)) AS Return_GST, SUM(IFNULL(cogs,0)) AS cogs, SUM(IFNULL(return_cogs,0)) AS return_cogs FROM `MapleMonk.zouk_secondary_pandl` GROUP BY 1,2,3,4,5,6,7,8,9 ) SELECT sd.*, inv.Available_Inventory, inv.DOH_30_Days, CASE WHEN inv.DOH_30_Days < 10 THEN \'Below 10 DOH\' ELSE \'Above 10 DOH\' END AS DOH_Check FROM Sales_Data sd LEFT JOIN Inventory inv ON sd.category_code = inv.category_code AND sd.PRODUCT_CATEGORY = inv.PRODUCT_CATEGORY AND sd.Date = DATE(inv.DATA_FETCH_DATE)",
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
            