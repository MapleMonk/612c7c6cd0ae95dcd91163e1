{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM `kerala-ayurveda-wh.MapleMonk.amazon_us_sp_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_fact_table` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY) OR `_airbyte_unique_key` IN ( SELECT NULLIF(TRIM(`_airbyte_unique_key`), \'\') FROM `kerala-ayurveda-wh.MapleMonk.amazon_us_sp_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY) ); INSERT INTO `kerala-ayurveda-wh.MapleMonk.amazon_us_sp_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_fact_table` SELECT NULLIF(TRIM(s.`_airbyte_unique_key`), \'\') AS `_airbyte_unique_key`, SAFE_CAST(NULLIF(TRIM(s.`return_date`), \'\') AS TIMESTAMP) AS `return_date`, NULLIF(TRIM(s.`order_id`), \'\') AS `order_id`, NULLIF(TRIM(s.`sku`), \'\') AS `sku`, NULLIF(TRIM(s.`asin`), \'\') AS `asin`, NULLIF(TRIM(s.`fnsku`), \'\') AS `fnsku`, NULLIF(TRIM(s.`product_name`), \'\') AS `product_name`, SAFE_CAST(NULLIF(TRIM(s.`quantity`), \'\') AS INT64) AS `quantity`, NULLIF(TRIM(s.`fulfillment_center_id`), \'\') AS `fulfillment_center_id`, NULLIF(TRIM(s.`detailed_disposition`), \'\') AS `detailed_disposition`, NULLIF(TRIM(s.`reason`), \'\') AS `reason`, NULLIF(TRIM(s.`status`), \'\') AS `status`, NULLIF(TRIM(s.`license_plate_number`), \'\') AS `license_plate_number`, NULLIF(TRIM(s.`customer_comments`), \'\') AS `customer_comments`, s.`_airbyte_ab_id`, s.`_airbyte_emitted_at`, s.`_airbyte_normalized_at`, s.`_airbyte_amazon_us_sp_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM `kerala-ayurveda-wh.MapleMonk.amazon_us_sp_GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` AS s WHERE s.`_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY);",
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
            