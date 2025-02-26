{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.good_bad_check AS WITH cp AS ( SELECT order_id, TRIM(sku.value) AS unique_sku, journey, awb_number, assigned_date AS assigned_time, assigned_date::DATE AS date, return_reason, pickup_date, delivery_date, courier_partner, CONCAT(order_id, unique_sku) AS ident1 FROM snitch_db.maplemonk.cp_1, LATERAL FLATTEN(input => SPLIT(sku_list, \',\')) sku WHERE journey = \'Reverse\' AND assigned_date >= \'2024-10-01\' AND status = \'DELIVERED\' AND ( LOWER(return_reason) LIKE \'%wrong%\' OR LOWER(return_reason) LIKE \'%defect%\' OR LOWER(return_reason) LIKE \'%different%\' ) ), uc AS ( SELECT \"Tracking No\", \"Item SkuCode\", \"Return Item Status\", \"Original Sale Order Code\", UPPER(\"QC Comment\") AS WH_COMMENT, CONCAT(TRIM(\"Original Sale Order Code\"), TRIM(\"Item SkuCode\")) AS ident2 FROM snitch_db.maplemonk.snitch_get_copy_of_reverse_pickup WHERE \"Reverse Pickup Last Updated\"::DATE >= \'2024-08-01\' AND \"Channel Name\" = \'SHOPIFY\' AND \"Return Item Status\" LIKE \'%INV%\' ), final AS ( SELECT cp.*, uc.\"Tracking No\", uc.\"Return Item Status\", uc.WH_COMMENT, ROW_NUMBER() OVER (PARTITION BY cp.ident1 ORDER BY cp.assigned_time DESC) AS rn FROM cp LEFT JOIN uc ON cp.ident1 = uc.ident2 ) SELECT * FROM final WHERE rn = 1 AND \"Return Item Status\" IS NOT NULL;",
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
            