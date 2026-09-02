{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.zouk_trz_get_purchase_order_register_item_wise` AS SELECT * REPLACE( DATETIME(TIMESTAMP_MILLIS(SAFE_CAST(document_date AS INT64)), \"Asia/Kolkata\") AS document_date, DATETIME(TIMESTAMP_MILLIS(SAFE_CAST(creation_date AS INT64)), \"Asia/Kolkata\") AS creation_date, DATETIME(TIMESTAMP_MILLIS(SAFE_CAST(creation_date_ts AS INT64)), \"Asia/Kolkata\") AS creation_date_ts, DATETIME(TIMESTAMP_MILLIS(SAFE_CAST(doc_delivery_date AS INT64)), \"Asia/Kolkata\") AS doc_delivery_date, DATETIME(TIMESTAMP_MILLIS(SAFE_CAST(item_delivery_date AS INT64)), \"Asia/Kolkata\") AS item_delivery_date ) FROM `MapleMonk.zouk_trz_get_purchase_order_register_item_wise`;",
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
            