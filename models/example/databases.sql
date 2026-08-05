{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table medmongers_db.MAPLEMONK.medmongers_unicommerce_returns_intermediate as select CASE WHEN UPPER(shipping_status) LIKE \'%RETURN%\' THEN \'RETURN\' END AS return_status, CASE WHEN UPPER(shipping_status) LIKE \'%RETURN%\' THEN Courier END AS return_courier, CASE WHEN UPPER(shipping_status) LIKE \'%RETURN%\' THEN Shipping_Provider END AS return_shipping_provider, CASE WHEN UPPER(shipping_status) LIKE \'%RETURN%\' THEN awb END AS return_Tracking_Number, CASE WHEN UPPER(shipping_status) LIKE \'%RETURN%\' THEN reference_code END AS return_display_code ,FR.* from medmongers_db.MAPLEMONK.MEDMONGERS_UNICOMMERCE_FACT_ITEMS FR ; create or replace table medmongers_db.MAPLEMONK.medmongers_RETURNS_CONSOLIDATED as SELECT REFERENCE_CODE AS reference_code, SALEORDERITEMCODE AS saleorderitemcode, ORDER_ID AS ORDER_ID, order_Status AS order_status, date(ORDER_DATE) AS Order_Date, NULL AS Delivered_Date, MARKETPLACE AS marketplace, source AS MARKETING_CHANNEL, SOURCE AS SOURCE, PRODUCT_NAME AS PRODUCT_NAME, SKU AS SKU, COMMONSKU AS commonsku, MAPPED_CATEGORY AS product_category, MAPPED_SUB_CATEGORY AS product_sub_category, suborder_quantity AS order_quantity, BRAND AS brand, NULL AS image, City, State, shipping_pincode pincode, courier as Shipping_Courier, awb, NAME, EMAIL AS EMAIL, RETURN_DISPLAY_CODE AS return_request_number, NULL AS original_return_request_date, date(RETURN_DATE) AS return_date, RETURN_STATUS AS return_approval_status, NULL AS return_request_type, case WHEN UPPER(shipping_status) LIKE \'%COURIER_RETURN%\' THEN \'RTO\' else \'CUSTOMER RETURN\' end AS return_type, return_flag, RETURN_QUANTITY AS TOTAL_RETURNED_QUANTITY, NULL AS return_reason, NULL AS Payment_mode, COGS AS selling_price, PRODUCT_NAME AS returned_product_name, SKU AS returned_sku, RETURN_sales AS TOTAL_RETURN_AMOUNT, TAX AS TOTAL_RETURN_TAX, null AS TOTAL_RETURN_AMOUNT_EXCL_TAX, SALEORDERITEMCODE AS returned_item_saleorderitemcode, order_status AS oms_order_status, NULL AS region, NULL AS days_from_delivery, NULL AS flipkart_return_type FROM medmongers_db.MAPLEMONK.medmongers_unicommerce_returns_intermediate;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MEDMONGERS_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            