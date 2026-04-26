{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prd_db.justherbs.dwh_SHIPROCKET_LOGISTICS_CONSOLIDATED as WITH base AS ( SELECT id, CHANNEL_ORDER_ID, CHANNEL_NAME, STATUS, MASTER_STATUS, PAYMENT_METHOD, COD, TOTAL_ORDER_VALUE, CREATED_AT, DELIVERED_DATE, CUSTOMER_STATE, CUSTOMER_CITY, PRODUCTS FROM datalake_db.justherbs.jh_shiprocket_orders ), flattened AS ( SELECT b.*, f.value AS product_json FROM base b, LATERAL FLATTEN(input => PARSE_JSON(b.PRODUCTS)) f ), awb as ( SELECT channel_order_id, shipment.value:\"awb\"::STRING AS AWB_NUMBER FROM datalake_db.justherbs.jh_shiprocket_orders , LATERAL FLATTEN(input => TRY_PARSE_JSON(SHIPMENTS)) shipment qualify row_number() over(partition by channel_order_id order by length(ifnull(AWB_NUMBER,\'\')) desc) = 1 ) SELECT id, f.CHANNEL_ORDER_ID, AWB_NUMBER, CHANNEL_NAME, coalesce(product_json:\"channel_sku\"::STRING,product_json:\"sku\"::STRING) AS SKU, product_json:\"name\"::STRING AS PRODUCT_NAME, product_json:\"quantity\"::INT AS QUANTITY, coalesce(nullif(product_json:\"selling_price\"::FLOAT,0),ifnull(product_json:\"mrp\"::FLOAT,0)) * ifnull(product_json:\"quantity\"::INT,1) as selling_price, STATUS, MASTER_STATUS, PAYMENT_METHOD, COD, CUSTOMER_STATE, CUSTOMER_CITY, CREATED_AT, DELIVERED_DATE FROM flattened f left join awb on f.CHANNEL_ORDER_ID = awb.CHANNEL_ORDER_ID ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from DATALAKE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            