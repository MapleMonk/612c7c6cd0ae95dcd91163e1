{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.asaya_flipkart_earn_more_fact_items AS SELECT \'FLIPKART\' AS marketplace, \'FLIPKART EARN MORE\' AS CHANNEL, \'FLIPKART EARN MORE\' AS SOURCE, Fulfillment_Type AS shop_name, SKU_ID AS SKU, CAST(Product_Id AS STRING) AS PRODUCT_ID, SKU_ID AS PRODUCT_NAME, Category, PARSE_DATE(\'%Y-%m-%d\', Order_Date) AS ORDER_DATE, CAST(Gross_Units AS INT64) AS Gross_Units, CAST(GMV AS FLOAT64) AS GMV, CAST(Final_Sale_Units AS INT64) AS Final_Sale_Units, CAST(Final_Sale_Amount AS FLOAT64) AS Final_Sale_Amount, CAST(Return_Units AS INT64) AS Return_Units, CAST(Return_Amount AS FLOAT64) AS Return_Amount, CAST(Cancellation_Units AS INT64) AS Cancellation_Units, CAST(Cancellation_Amount AS FLOAT64) AS Cancellation_Amount FROM maplemonk.asaya_flipkart_earn_more_report;",
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
            