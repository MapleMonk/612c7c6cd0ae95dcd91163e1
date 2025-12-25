{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE TABLE IF NOT EXISTS misschase-maplemonk-wh.maplemonk.misschase_maplemonk_wh_UNICOMMERCE_FACT_ITEMS ( order_id STRING, order_Date DATE, reference_code STRING, name STRING, email STRING, city STRING, state STRING, phone STRING, saleorderitemcode STRING, sales_order_item_id STRING, shippingpackagecode STRING, SHIPPINGPACKAGESTATUS STRING, shipping_status STRING, order_status STRING, Courier STRING, Dispatch_Date DATE, Delivered_date DATE, Return_flag INT64, Return_quantity INT64, suborder_quantity INT64, cancelled_quantity INT64, selling_price FLOAT64, shipping_price FLOAT64, tax FLOAT64, discount FLOAT64, shipping_last_update_date DATE, days_in_shipment FLOAT64, awb STRING, marketplace STRING, payment_method STRING, PAYMENT_MODE STRING, PRODUCT_ID STRING, SKU STRING, SKU_CODE STRING, currency STRING, NEW_CUSTOMER_FLAG STRING, product_name STRING, mapped_product_name STRING, product_name_final STRING, mapped_category STRING, product_category STRING, mapped_sub_category STRING, product_sub_category STRING, warehouse_name STRING, pincode STRING, commonsku STRING );",
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
            