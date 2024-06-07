{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.HSR_STORE_RESTOCK AS WITH stock AS ( SELECT date, LOGICUSERCODE, SUM(stock_qty) AS OPENING_INVENTORY FROM snitch_db.maplemonk.logicerp23_24_get_stock_in_hand WHERE DATE = CURRENT_DATE AND branch_name LIKE \'%HSR%\' and LOGICUSERCODE not like (\'CB%\') GROUP BY date, LOGICUSERCODE ), sales AS ( SELECT SKU, SUM(SHIPPING_QUANTITY) AS QTY FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE order_date = current_date AND ORDER_TIMESTAMP >= CURRENT_TIMESTAMP - INTERVAL \'1.5 HOUR\' AND MARKETPLACE_MAPPED LIKE \'%HSR%\' GROUP BY SKU ), today_sales AS ( SELECT SKU, SUM(SHIPPING_QUANTITY) AS TODAY_SALES FROM SNITCH_DB.MAPLEMONK.STORE_fact_items_offline WHERE order_date = current_date AND ORDER_TIMESTAMP <= CURRENT_TIMESTAMP - INTERVAL \'1.5 HOUR\' AND MARKETPLACE_MAPPED LIKE \'%HSR%\' GROUP BY SKU ), ranked_products AS ( SELECT Distinct sku, Category, HANDLE, sku_url from ( SELECT product_dim.sku as sku, product_category as Category, HANDLE, concat(\'https://www.snitch.co.in/products/\', product_dim.handle) as sku_url, ROW_NUMBER() OVER (PARTITION BY product_dim.sku_group ORDER BY product_updated desc) as rn FROM snitch_db.snitch.product_dim ) WHERE sku_url is not null ) SELECT a.*, K.sku_url, K.Category, K.HANDLE, COALESCE(c.TODAY_SALES, 0) AS TODAY_SALES, COALESCE(b.QTY, 0) AS SALES_IN_LAST_90_MINUTES, Case when COALESCE(b.QTY, 0) > 0 then b.QTY else 0 end restock, CASE WHEN A.OPENING_INVENTORY - COALESCE(c.TODAY_SALES, 0)- COALESCE(b.QTY, 0) > 0 THEN \'Available\' else \'No Stock\' End as Status FROM stock a LEFT JOIN sales b ON a.LOGICUSERCODE = b.SKU LEFT JOIN today_sales c ON a.LOGICUSERCODE = c.SKU LEFT JOIN ranked_products K ON a.LOGICUSERCODE = K.SKU ;",
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
                        