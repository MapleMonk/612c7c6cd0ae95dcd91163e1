{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace TABLE HYPD_data as SELECT ORDER_ID, REFERENCE_CODE, ORDERITEM_CODE, EASY_ECOM_SKU, DISPATCH_STATUS, DELIVERY_STATUS, AWB, NAME, EMAIL, PHONE, ORDER_DATE, ORDER_MANIFEST_DATE, BATCH_ID, CITY, STATE, FINAL_STATUS, TARGET_DISPATCH_DATE, PAYMENT_MODE, WAREHOUSE_NAME, COURIER, QUANTITY, CASE WHEN DISPATCH_STATUS = \'DISPATCHED\' AND DELIVERY_STATUS = \'NOT DELIVERED\' THEN DATEDIFF(\'day\', ORDER_MANIFEST_DATE, CURRENT_DATE()) END AS In_Transit_time, CASE WHEN DISPATCH_STATUS = \'NOT DISPATCHED\' THEN DATEDIFF(\'day\', ORDER_DATE, CURRENT_DATE()) END AS O2S FROM SELECT_DB.maplemonk.\"SELECT_DB_ORDER_FULFILLMENT_REPORT\" where LENGTH(REFERENCE_CODE) = 14 AND LEFT(REFERENCE_CODE, 4) = \'HYPD\';",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SELECT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        