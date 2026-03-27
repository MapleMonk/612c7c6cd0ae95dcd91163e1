{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE varak-wh.maplemonk.Varak_s3_item_sales_fact_items AS SELECT cast(NULL AS string) customer_id, site_name AS Shop_name, order_source AS marketplace, order_source AS CHANNEL, order_source AS SOURCE, order_id_2 as ORDER_ID, order_id_2 AS reference_code, CAST(CUSTOMER_CONTACT_NUMBER AS STRING) AS PHONE, CUSTOMER_NAME as NAME, cast(CUSTOMER_EMAIL_ADDRESS AS string) AS EMAIL, null AS SHIPPING_LAST_UPDATE_DATE, null as SKU, null PRODUCT_ID, Item_Name as Product_Name, \'AED\' as CURRENCY, null AS CITY, null As STATE, null AS Customer_State, cast(ORDER_STATUS as string) as Order_Status, SAFE.PARSE_DATE(\'%d/%m/%Y\', REGEXP_REPLACE(TRIM(Order_Date_and_Time), r\'[^0-9]\', \'/\')) AS date, CAST(SAFE.PARSE_DATE(\'%d/%m/%Y\', REGEXP_REPLACE(TRIM(Order_Date_and_Time), r\'[^0-9]\', \'/\')) AS DATETIME) AS Order_Time, qty as QUANTITY, IFNULL(safe_cast(amount_before_tax as float64), 0) + IFNULL(safe_cast(discount as float64), 0) AS GROSS_SALES_BEFORE_TAX, safe_cast(discount as float64) AS DISCOUNT, safe_cast(ifnull(Tax_Amount,0) as float64) as TAX, null as SHIPPING_PRICE, safe_cast(ifnull(Grand_Total,0) as float64) AS SELLING_PRICE, cast(ORDER_STATUS as string) AS OMS_order_status, cast(ORDER_STATUS as string) AS SHIPPING_STATUS, cast(ORDER_STATUS as string) AS FINAL_SHIPPING_STATUS, order_id_2 AS SALEORDERITEMCODE, order_id_2 AS SALES_ORDER_ITEM_ID, cast(null as string) AS AWB, cast(order_subtype as string) Payment_Gateway, NULL AS Payment_Mode, cast(null as string) AS COURIER, cast(null as date) AS DISPATCH_DATE, null AS DELIVERED_DATE, 1 AS DELIVERED_STATUS, 0 AS RETURN_FLAG, NULL AS returned_quantity, NULL AS returned_sales, NULL AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_PRODUCT, NULL AS Days_in_Shipment, cast(null as date) AS ACQUISITION_DATE, null AS SKU_CODE, item_name AS PRODUCT_NAME_FINAL, Category_Name AS PRODUCT_CATEGORY, null AS PRODUCT_SUB_CATEGORY, null as commonsku, cast(null as string) AS WAREHOUSE, null as pincode, cast(null as string) pickup_pincode, cast(null as string) Source_name, cast(null as string) Mapped_Source_Name, cast(null as string) Order_Type, null as Invoice_Date, null as sub_category, null as size, FROM `Maplemonk.S3_Item_Sales`;",
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
            