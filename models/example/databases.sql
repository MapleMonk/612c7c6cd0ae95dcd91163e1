{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE varak-wh.maplemonk.s3_sales_consolidated AS SELECT cast(NULL AS string) customer_id, coalesce(a.shop_name, b.shop_name) AS Shop_name, upper(coalesce(a.marketplace, b.marketplace)) AS marketplace, upper(coalesce(a.CHANNEL, b.CHANNEL)) AS CHANNEL, upper(a.SOURCE) AS SOURCE, coalesce(a.order_id, b.order_id) as ORDER_ID, coalesce(a.reference_code, b.reference_code) AS reference_code, coalesce(a.phone, b.phone) AS PHONE, coalesce(a.name, b.name) as NAME, COALESCE(a.email, b.email) AS EMAIL, null AS SHIPPING_LAST_UPDATE_DATE, null as SKU, null PRODUCT_ID, b.product_name as PRODUCT_NAME, \'AED\' as CURRENCY, null AS CITY, null AS State, UPPER(coalesce(a.Order_Status, b.Order_Status)) AS Order_Status, coalesce(a.Order_Date, b.date) AS Order_Date, coalesce(a.Order_Time, b.Order_Time) as Order_Time, b.QUANTITY as QUANTITY, coalesce(b.GROSS_SALES_BEFORE_TAX, a.GROSS_SALES_BEFORE_TAX) AS GROSS_SALES_BEFORE_TAX, coalesce(b.DISCOUNT, a.DISCOUNT) AS DISCOUNT, coalesce(b.TAX, a.TAX) as TAX, null as SHIPPING_PRICE, coalesce(b.SELLING_PRICE, a.SELLING_PRICE) AS SELLING_PRICE, coalesce(a.order_status, b.order_status) AS OMS_order_status, coalesce(a.order_status, b.order_status) AS SHIPPING_STATUS, coalesce(a.order_status, b.order_status) AS FINAL_SHIPPING_STATUS, coalesce(a.ORDER_ID, b.ORDER_ID) AS SALEORDERITEMCODE, coalesce(a.ORDER_ID, b.ORDER_ID) AS SALES_ORDER_ITEM_ID, null AS AWB, b.Payment_Gateway AS Payment_Gateway, null AS Payment_Mode, null AS COURIER, null AS DISPATCH_DATE, null AS DELIVERED_DATE, 1 AS DELIVERED_STATUS, 0 AS RETURN_FLAG, null AS returned_quantity, null AS returned_sales, null AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_PRODUCT, null AS Days_in_Shipment, NULL AS ACQUISITION_DATE, null AS SKU_CODE, b.PRODUCT_NAME_FINAL AS PRODUCT_NAME_FINAL, b.PRODUCT_CATEGORY AS PRODUCT_CATEGORY, b.PRODUCT_SUB_CATEGORY AS PRODUCT_SUB_CATEGORY, null AS commonsku, null AS WAREHOUSE, null AS pincode, null as source_pincode, null as discount_codes FROM `Maplemonk.Varak_s3_order_sales_fact_items` a full outer join `Maplemonk.Varak_s3_item_sales_fact_items` b on a.ORDER_ID = b.ORDER_ID;",
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
            