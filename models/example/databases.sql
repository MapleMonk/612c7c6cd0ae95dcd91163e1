{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.greenrorbit_amazon_fba_fulfillment_customer_returns_fact_items AS WITH deduped AS ( SELECT UPPER(TRIM(sku)) AS sku, asin, fnsku, UPPER(TRIM(reason)) AS reason, UPPER(TRIM(status)) AS status, order_id, CAST(quantity AS INT64) AS quantity, DATETIME(TIMESTAMP(return_date), \'Asia/Kolkata\') AS return_date, product_name, customer_comments, UPPER(TRIM(detailed_disposition)) AS detailed_disposition, license_plate_number, fulfillment_center_id, ROW_NUMBER() OVER ( PARTITION BY order_id, UPPER(TRIM(sku)) ORDER BY return_date DESC ) AS rn FROM maplemonk.greenrorbit_amazon_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA ) SELECT sku, asin, fnsku, reason, status, order_id, quantity, return_date, product_name, customer_comments, detailed_disposition, license_plate_number, fulfillment_center_id, FROM deduped WHERE rn = 1;",
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
            