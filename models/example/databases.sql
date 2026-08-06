{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table daniel_klein_DB.MAPLEMONK.DK_S3_AmazonOR_Sales_Fact_Items as SELECT NULL AS customer_id, \'AMAZON OR\' AS SHOP_NAME, \'AMAZON OR\' AS marketplace, \'AMAZON OR\' AS CHANNEL, a.Remark AS SOURCE, NULL AS Final_UTM_Campaign, NULL AS ORDER_ID, NULL AS reference_code, NULL AS PHONE, NULL AS NAME, NULL AS EMAIL, NULL AS SHIPPING_LAST_UPDATE_DATE, a.sku AS SKU, NULL AS PRODUCT_ID, NULL AS PRODUCT_NAME, NULL AS CURRENCY, NULL AS CITY, NULL AS STATE, NULL AS ORDER_STATUS, COALESCE( TRY_TO_DATE(ORDER_DATE, \'DD-MON-YY\'), TRY_TO_DATE(ORDER_DATE, \'DD-MON\'), TRY_TO_DATE(ORDER_DATE, \'MM/DD/YYYY\') ) AS Order_Date, Cast(a.\"Sold Qty\" as INT) AS QUANTITY, TRY_TO_DOUBLE(REPLACE(a.gmv, \',\', \'\')) AS GROSS_SALES_BEFORE_TAX, NULL AS DISCOUNT, NULL AS TAX, NULL AS SHIPPING_PRICE, TRY_TO_DOUBLE(REPLACE(a.gmv, \',\', \'\')) AS SELLING_PRICE, NULL AS OMS_order_status, NULL AS SHIPPING_STATUS, NULL AS FINAL_SHIPPING_STATUS, NULL AS SALEORDERITEMCODE, NULL AS SALES_ORDER_ITEM_ID, NULL AS AWB, NULL AS PAYMENT_GATEWAY, NULL AS Payment_Mode, NULL AS COURIER, NULL AS DISPATCH_DATE, NULL AS DELIVERED_DATE, NULL AS DELIVERED_STATUS, NULL AS RETURN_FLAG, NULL AS returned_quantity, NULL AS returned_sales, NULL AS cancelled_quantity, NULL AS NEW_CUSTOMER_FLAG, NULL AS acquisition_product, NULL AS Days_in_Shipment, NULL AS ACQUISITION_DATE, a.sku AS SKU_CODE, NULL AS PRODUCT_NAME_FINAL, NULL AS PRODUCT_CATEGORY, NULL AS PRODUCT_SUB_CATEGORY, NULL AS WAREHOUSE FROM daniel_klein_DB.maplemonk.DK_S3_AmazonOR_Sales a;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DANIEL_KLEIN_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            