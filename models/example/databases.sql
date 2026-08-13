{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `goodtribe-wh.maplemonk.gift_Card_consolidated` AS SELECT a.*, b.phone, b.email, b.first_name, b.last_name, CASE WHEN b.first_name IS NULL AND (b.last_name IS NULL OR TRIM(b.last_name) IN (\'\', \'.\')) THEN NULL WHEN b.last_name IS NULL OR TRIM(b.last_name) IN (\'\', \'.\') THEN b.first_name WHEN b.first_name IS NULL OR TRIM(b.first_name) = \'\' THEN b.last_name ELSE CONCAT(b.first_name, \' \', b.last_name) END AS full_name, CASE WHEN a.disabled_at IS NULL THEN \"FALSE\" ELSE \"TRUE\" END AS is_disable, CASE WHEN SAFE_CAST(a.expires_on AS DATE) < CURRENT_DATE() THEN \"TRUE\" ELSE \"FALSE\" END AS is_expire, CASE WHEN a.disabled_at IS NULL AND ( a.expires_on IS NULL OR SAFE_CAST(a.expires_on AS DATE) >= CURRENT_DATE() ) THEN \"ACTIVE\" ELSE \"INACTIVE\" END AS final_status, CASE WHEN a.expires_on IS NULL THEN NULL ELSE DATE_DIFF( SAFE_CAST(a.expires_on AS DATE), CURRENT_DATE(), DAY ) END AS days_till_expiry FROM `goodtribe-wh.maplemonk.shopify_gift_Cards` a LEFT JOIN ( SELECT id, phone, email, first_name, last_name FROM `goodtribe-wh.maplemonk.Shopify_All_customers` ) b ON CAST(a.customer_id AS STRING) = CAST(b.id AS STRING);",
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
            