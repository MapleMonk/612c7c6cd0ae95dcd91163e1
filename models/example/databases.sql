{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.matrixstore_flipkart_fact_items_EM AS WITH base AS ( SELECT CONCAT( REPLACE(Order_Date, \'-\', \'\'), \'_\', SKU_ID, \'_\', Product_Id, \'_\', Location_Id, \'_\', Fulfillment_Type ) AS ORDER_ID, PARSE_DATE(\'%Y-%m-%d\', Order_Date) AS Order_Date, SKU_ID AS SKU, Product_Id, Location_Id, Fulfillment_Type, Brand, Category, Vertical, CAST(Gross_Units AS INT64) AS Gross_Units, CAST(GMV AS FLOAT64) AS GMV, CAST(Final_Sale_Units AS INT64) AS Final_Sale_Units, CAST(Final_Sale_Amount AS FLOAT64) AS Final_Sale_Amount, CAST(Return_Units AS INT64) AS Return_Units, CAST(Return_Amount AS FLOAT64) AS Return_Amount, CAST(Cancellation_Units AS INT64) AS Cancellation_Units, CAST(Cancellation_Amount AS FLOAT64) AS Cancellation_Amount FROM maplemonk.matrixstore_flipkart_earn_more_report ) SELECT ORDER_ID, CONCAT(ORDER_ID, \'_SALE\') AS line_item_id, \'FLIPKART\' AS marketplace, \'FLIPKART\' AS shop_name, CAST(NULL AS STRING) AS NAME, CAST(NULL AS STRING) AS EMAIL, SKU, Product_Id AS PRODUCT_ID, SKU AS PRODUCT_NAME, Category, CAST(NULL AS STRING) AS CITY, CAST(NULL AS STRING) AS STATE, CAST(NULL AS STRING) AS PINCODE, Order_Date AS ORDER_DATE, CAST(NULL AS DATE) AS DISPATCH_DATE, CAST(NULL AS DATE) AS DELIVERY_DATE, \'delivered\' AS order_item_status, Final_Sale_Units AS QUANTITY, Final_Sale_Amount AS line_item_sales, CAST(0 AS FLOAT64) AS DISCOUNT, CAST(0 AS FLOAT64) AS TAX, CAST(0 AS FLOAT64) AS SHIPPING_PRICE, 0 AS IS_REFUND, 0 AS IS_CANCELLED, CAST(NULL AS STRING) AS payment_mode, CAST(NULL AS STRING) AS courier, Fulfillment_Type FROM base WHERE Final_Sale_Units > 0 UNION ALL SELECT ORDER_ID, CONCAT(ORDER_ID, \'_RETURN\') AS line_item_id, \'FLIPKART\', \'FLIPKART\', NULL, NULL, SKU, Product_Id, SKU, Category, NULL, NULL, NULL, Order_Date, NULL, NULL, \'returned\' AS order_item_status, Return_Units, Return_Amount, 0, 0, 0, 1 AS IS_REFUND, 0 AS IS_CANCELLED, NULL, NULL, Fulfillment_Type FROM base WHERE Return_Units > 0 UNION ALL SELECT ORDER_ID, CONCAT(ORDER_ID, \'_CANCEL\') AS line_item_id, \'FLIPKART\', \'FLIPKART\', NULL, NULL, SKU, Product_Id, SKU, Category, NULL, NULL, NULL, Order_Date, NULL, NULL, \'cancelled\' AS order_item_status, Cancellation_Units, Cancellation_Amount, 0, 0, 0, 0, 1 AS IS_CANCELLED, NULL, NULL, Fulfillment_Type FROM base WHERE Cancellation_Units > 0;",
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
            