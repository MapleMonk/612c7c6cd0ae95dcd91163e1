{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE saadaa-wh.maplemonk.Varak_orders_fact_items AS SELECT cast(NULL AS string) customer_id, site_name AS Shop_name, order_source AS marketplace, order_source AS CHANNEL, order_source AS SOURCE, ORDER_ID, ORDER_ID AS reference_code, CAST(CUSTOMER_CONTACT_NUMBER AS STRING) AS PHONE, CUSTOMER_NAME as NAME, cast(CUSTOMER_EMAIL_ADDRESS AS string) AS EMAIL, null AS SHIPPING_LAST_UPDATE_DATE, null as SKU, null PRODUCT_ID, null as Product_Name, \'AED\' as CURRENCY, null AS CITY, null As STATE, null AS Customer_State, cast(ORDER_STATUS as string) as Order_Status, SAFE.PARSE_DATE(\'%d/%m/%Y\', REGEXP_REPLACE(TRIM(order_date), r\'[^0-9]\', \'/\')) AS Order_Date, CAST(SAFE.PARSE_DATE(\'%d/%m/%Y\', REGEXP_REPLACE(TRIM(Order_Time), r\'[^0-9]\', \'/\')) AS DATETIME) AS Order_Time, null as QUANTITY, (IFNULL(safe_cast(grand_total as float64), 0) - IFNULL(safe_cast(gst_amount as float64), 0) + ifnull(safe_cast(DISCOUNT__ as float64),0)) AS GROSS_SALES_BEFORE_TAX, safe_cast(discount__ as float64) AS DISCOUNT, safe_cast(ifnull(gst_amount,0) as float64) as TAX, null as SHIPPING_PRICE, safe_cast(ifnull(GRAND_TOTAL,0) as float64) AS SELLING_PRICE, cast(ORDER_STATUS as string) AS OMS_order_status, cast(ORDER_STATUS as string) AS SHIPPING_STATUS, cast(ORDER_STATUS as string) AS FINAL_SHIPPING_STATUS, order_id AS SALEORDERITEMCODE, order_id AS SALES_ORDER_ITEM_ID, cast(null as string) AS AWB, cast(null as array<string>) Payment_Gateway, NULL AS Payment_Mode, cast(null as string) AS COURIER, cast(null as date) AS DISPATCH_DATE, null AS DELIVERED_DATE, 1 AS DELIVERED_STATUS, 0 AS RETURN_FLAG, NULL AS returned_quantity, NULL AS returned_sales, NULL AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_PRODUCT, NULL AS Days_in_Shipment, cast(null as date) AS ACQUISITION_DATE, null AS SKU_CODE, null AS PRODUCT_NAME_FINAL, null AS PRODUCT_CATEGORY, null AS PRODUCT_SUB_CATEGORY, null as commonsku, cast(null as string) AS WAREHOUSE, null as pincode, cast(null as string) pickup_pincode, cast(null as string) Source_name, cast(null as string) Mapped_Source_Name, cast(null as string) Order_Type, null as Invoice_Date, null as sub_category, null as size, FROM `Maplemonk.S3_Order_Sales` ;",
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
            