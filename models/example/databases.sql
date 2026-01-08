{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT NULL AS customer_id, shop_name, marketplace, CHANNEL, SOURCE, Order_id AS ORDER_ID, Order_id AS reference_code, NULL AS PHONE, NULL AS NAME, NULL AS EMAIL, CAST(shipped_date AS TIMESTAMP) AS SHIPPING_LAST_UPDATE_DATE, SKU AS SKU, CAST(style_id AS STRING) AS PRODUCT_ID, PRODUCT_NAME AS PRODUCT_NAME, NULL AS CURRENCY, UPPER(CITY) AS city, UPPER(STATE) AS State, UPPER(ORDER_STATUS) AS order_status, CAST(created_date AS DATE) AS Order_Date, CAST(created_date AS DATETIME) AS ORDER_TIME, QUANTITY, IFNULL(SELLING_PRICE, 0) - IFNULL(tax_recovery, 0) AS gross_sales_before_tax, DISCOUNT AS DISCOUNT, tax_recovery AS TAX, shipping_charge AS SHIPPING_PRICE, SELLING_PRICE AS SELLING_PRICE, UPPER(ORDER_STATUS) AS OMS_ORDER_STATUS, UPPER(order_status) AS SHIPPING_STATUS, UPPER(order_status) AS FINAL_SHIPPING_STATUS, NULL AS SALEORDERITEMCODE, NULL AS SALES_ORDER_ITEM_ID, NULL AWB, NULL AS payment_gateway, NULL AS payment_mode, courier_code AS COURIER, CAST(shipped_date AS DATE) AS DISPATCH_DATE, CAST(delivered_date AS DATE) AS delivered_date, CASE WHEN UPPER(order_status) IN (\'DELIVERED\') THEN 1 END AS DELIVERED_STATUS, NULL AS RETURN_FLAG, NULL AS returned_quantity, NULL AS returned_sales, NULL AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS ACQUISITION_PRODUCT, NULL AS days_in_shipment, NULL AS ACQUISITION_DATE, myntra_sku_code AS sku_code, PRODUCT_NAME AS PRODUCT_NAME_FINAL, PRODUCT_CATEGORY, NULL AS PRODUCT_SUB_CATEGORY, NULL AS commonsku, CAST(seller_warehouse_id AS STRING) AS warehouse, zipcode pincode, NULL AS source_pincode, NULL AS discount_codes FROM Maplemonk.TryBuy_Myntra_Orders_Fact_Items",
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
            