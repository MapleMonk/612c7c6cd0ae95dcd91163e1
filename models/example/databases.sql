{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.good_bad_check as with cp as ( SELECT order_id, TRIM(sku.value) AS unique_sku, journey, awb_number, assigned_date as assigned_time, assigned_date::date as date, return_reason, pickup_date, delivery_date, courier_partner, concat(order_id,unique_sku) as ident1 FROM snitch_db.maplemonk.cp_1, LATERAL FLATTEN(input => SPLIT(sku_list, \',\')) sku WHERE journey = \'Reverse\' AND assigned_date >= \'2024-10-01\' and status = \'DELIVERED\' AND ( LOWER(return_reason) LIKE \'%wrong%\' OR LOWER(return_reason) LIKE \'%defect%\' OR LOWER(return_reason) LIKE \'%different%\' ) ), uc as ( select \"Tracking No\", \"Item SkuCode\", \"Return Item Status\", \"Original Sale Order Code\", UPPER(\"QC Comment\") as WH_COMMENT, concat(TRIM(\"Original Sale Order Code\"),TRIM(\"Item SkuCode\")) as ident2 from snitch_db.maplemonk.snitch_get_copy_of_reverse_pickup where \"Reverse Pickup Last Updated\"::date >= \'2024-08-01\' and \"Channel Name\" = \'SHOPIFY\' and \"Return Item Status\" like \'%INV%\' ), final as ( select * from cp left join uc on cp.ident1 = uc.ident2 ) select * from final where \"Return Item Status\" is not null",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            