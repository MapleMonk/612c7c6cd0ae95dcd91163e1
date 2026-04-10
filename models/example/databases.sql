{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.matrixstore_flipkart_fact_items_EM AS SELECT \'FLIPKART\' AS marketplace, \'FLIPKART EARN MORE\' AS CHANNEL, \'FLIPKART EARN MORE\' AS SOURCE, Fulfillment_Type AS shop_name, CONCAT(REPLACE(Order_Date, \'-\', \'\'), \'_\', SKU_ID, \'_\', Product_Id, \'_\', Location_Id, \'_\', Fulfillment_Type) AS ORDER_ID, CONCAT(REPLACE(Order_Date, \'-\', \'\'), \'_\', SKU_ID, \'_\', Product_Id, \'_\', Location_Id, \'_\', Fulfillment_Type) AS reference_code, CONCAT(REPLACE(Order_Date, \'-\', \'\'), \'_\', SKU_ID, \'_\', Product_Id, \'_\', Location_Id, \'_\', Fulfillment_Type, \'_LINE\') AS SALEORDERITEMCODE, CONCAT(REPLACE(Order_Date, \'-\', \'\'), \'_\', SKU_ID, \'_\', Product_Id, \'_\', Location_Id, \'_\', Fulfillment_Type, \'_LINE\') AS SALES_ORDER_ITEM_ID, SKU_ID AS SKU, CAST(Product_Id AS STRING) AS PRODUCT_ID, SKU_ID AS PRODUCT_NAME, Category, PARSE_DATE(\'%Y-%m-%d\', Order_Date) AS ORDER_DATE, CAST(Gross_Units AS INT64) AS Gross_Units, CAST(GMV AS FLOAT64) AS GMV, CAST(Final_Sale_Units AS INT64) AS Final_Sale_Units, CAST(Final_Sale_Amount AS FLOAT64) AS Final_Sale_Amount, CAST(Return_Units AS INT64) AS Return_Units, CAST(Return_Amount AS FLOAT64) AS Return_Amount, CAST(Cancellation_Units AS INT64) AS Cancellation_Units, CAST(Cancellation_Amount AS FLOAT64) AS Cancellation_Amount FROM maplemonk.matrixstore_flipkart_earn_more_report;",
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
            