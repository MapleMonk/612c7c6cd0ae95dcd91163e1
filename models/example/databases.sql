{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.Banana_club_product_perform_factitems AS SELECT sc.marketplace AS final_marketplace, CASE WHEN UPPER(sc.marketplace) = \'WEBSITE\' THEN \'WEBSITE\' WHEN UPPER(sc.marketplace) IN (\'MYNTRAPPMP\',\'MYNTRAPPMP_EMIZA\') THEN \'MYNTRA\' WHEN UPPER(sc.marketplace) LIKE \'%AJIO%\' OR UPPER(sc.marketplace) LIKE \'%FLIPKART%\' THEN \'OTHERS\' ELSE UPPER(sc.marketplace) END AS marketplace, sc.channel, sc.Order_Date, COALESCE(sr.QUANTITY, mr.QUANTITY, sc.QUANTITY) AS Quantity, COALESCE(sc.TAX, mr.TAX) AS Tax, sc.SELLING_PRICE AS Selling_price, ROUND(SAFE_DIVIDE((IFNULL(sc.selling_price,0) - IFNULL(cc.cost,0)), NULLIF(sc.selling_price,0))* 100,2) AS Gross_Profit, UPPER(COALESCE(inv.name, sc.PRODUCT_NAME)) AS Product_name, sc.city, sc.state, COALESCE(sr.return_reason, mr.return_reason) AS Return_reason, COALESCE(sr.returned_quantity, mr.returned_quantity, sc.returned_quantity) AS returned_quantity, COALESCE(sr.cancelled_quantity, sc.cancelled_quantity) AS cancelled_quantity, cc.cost, inv.inventory AS current_inventory, COALESCE(inv.image_link, sc.image_link) AS image_link, FROM maplemonk.bananaclub_sales_consolidated sc LEFT JOIN ( SELECT REPLACE(order_name, \'#\', \'\') AS order_name, order_id, sku, CAST(order_timestamp AS DATE) AS Order_Date, sales_quantity AS QUANTITY, name AS PRODUCT_NAME_FINAL, city, state, return_quantity AS returned_quantity, cancelled_quantity, customer_reason_label AS return_reason FROM maplemonk.shopify_return_exchange QUALIFY ROW_NUMBER() OVER( PARTITION BY order_name, sku, CAST(order_timestamp AS DATE) ORDER BY order_timestamp DESC ) = 1 ) sr ON LOWER(REPLACE(sc.reference_code, \'#\', \'\')) = LOWER(REPLACE(sr.order_name, \'#\', \'\')) AND LOWER(sc.sku) = LOWER(sr.sku) LEFT JOIN ( SELECT sellerOrderId, sku, CAST(created_on AS DATE) AS Order_Date, created_on AS Order_Time, quantity, Taxable AS TAX, final_amount AS SELLING_PRICE, PRODUCT_NAME AS PRODUCT_NAME_FINAL, city, state, return_quantity AS returned_quantity, return_reason FROM MapleMonk.BananaClub_db_Myntra_Master QUALIFY ROW_NUMBER() OVER( PARTITION BY sellerOrderId, sku, CAST(created_on AS DATE) ORDER BY created_on DESC ) = 1 ) mr ON DATE(sc.order_date) = DATE(mr.Order_Date) AND LOWER(REPLACE(sc.reference_code, \'#\', \'\')) = LOWER(REPLACE(mr.sellerOrderId, \'#\', \'\')) AND LOWER(sc.sku) = LOWER(mr.sku) LEFT JOIN ( SELECT sku_code, CAST(cost AS INT64) AS cost FROM maplemonk.banana_club_Google_sheet_cost ) cc ON LOWER(sc.sku) = lower(cc.sku_code) LEFT JOIN ( SELECT sku, date, name, image_link, SUM(inventory) AS inventory FROM maplemonk.daily_inventory GROUP BY 1,2,3,4 ) inv ON LOWER(sc.sku) = lower(inv.sku) AND DATE(sc.order_date) = DATE(inv.date);",
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
            