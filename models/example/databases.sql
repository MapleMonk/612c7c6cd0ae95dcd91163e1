{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MINDFUL_DB.MAPLEMONK.EASYECOM_PO_FACT_ITEMS AS SELECT PO_ID ,PO_NUMBER ,PO_REF_NUM ,VENDOR_C_ID ,VENDOR_LOCATION_KEY ,UPPER(VENDOR_NAME) VENDOR_NAME ,PO_CREATED_LOCATION_KEY ,UPPER(PO_CREATED_WAREHOUSE) PO_CREATED_WAREHOUSE ,PO_CREATED_WAREHOUSE_C_ID ,try_to_timestamp(PO_CREATED_DATE) PO_CREATED_DATE ,try_to_timestamp(PO_UPDATED_DATE) PO_UPDATED_DATE ,B.VALUE:\"cp_id\" CP_ID ,B.VALUE:\"ean\" EAN ,B.VALUE:\"hsn\" HSN ,B.VALUE:\"model_no\" MODEL_NO ,B.VALUE:\"product_id\" PRODUCT_ID ,B.VALUE:\"sku\" SKU ,B.VALUE:\"product_description\" PRODUCT_DESCRIPTION ,B.VALUE:\"purchase_order_detail_id\" PO_DETAIL_ID ,B.VALUE:\"original_quantity\"::float ORIGINAL_QUANTITY ,B.VALUE:\"pending_quantity\"::float PENDING_QUANTITY ,B.VALUE:\"item_price\"::float ITEM_PRICE ,po_status_id from MINDFUL_DB.MAPLEMONK.EASYECOM_1_PURCHASE_ORDERS A, LATERAL FLATTEN (INPUT => A.PO_ITEMS) B UNION SELECT PO_ID ,PO_NUMBER ,PO_REF_NUM ,VENDOR_C_ID ,VENDOR_LOCATION_KEY ,UPPER(VENDOR_NAME) VENDOR_NAME ,PO_CREATED_LOCATION_KEY ,UPPER(PO_CREATED_WAREHOUSE) PO_CREATED_WAREHOUSE ,PO_CREATED_WAREHOUSE_C_ID ,try_to_timestamp(PO_CREATED_DATE) PO_CREATED_DATE ,try_to_timestamp(PO_UPDATED_DATE) PO_UPDATED_DATE ,B.VALUE:\"cp_id\" CP_ID ,B.VALUE:\"ean\" EAN ,B.VALUE:\"hsn\" HSN ,B.VALUE:\"model_no\" MODEL_NO ,B.VALUE:\"product_id\" PRODUCT_ID ,B.VALUE:\"sku\" SKU ,B.VALUE:\"product_description\" PRODUCT_DESCRIPTION ,B.VALUE:\"purchase_order_detail_id\" PO_DETAIL_ID ,B.VALUE:\"original_quantity\"::float ORIGINAL_QUANTITY ,B.VALUE:\"pending_quantity\"::float PENDING_QUANTITY ,B.VALUE:\"item_price\"::float ITEM_PRICE ,po_status_id from MINDFUL_DB.MAPLEMONK.EASYECOM_2_PURCHASE_ORDERS A, LATERAL FLATTEN (INPUT => A.PO_ITEMS) B;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from MINDFUL_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            