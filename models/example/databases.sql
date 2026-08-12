{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM `kerala-ayurveda-wh.MapleMonk.aa_ka_us_get_campaign_history_fact_table` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY); INSERT INTO `kerala-ayurveda-wh.MapleMonk.aa_ka_us_get_campaign_history_fact_table` SELECT NULLIF(TRIM(`entityId`), \'\') AS `entityId`, NULLIF(TRIM(`entityType`), \'\') AS `entityType`, NULLIF(TRIM(`changeType`), \'\') AS `changeType`, TIMESTAMP_MILLIS(SAFE_CAST(NULLIF(TRIM(`timestamp`), \'\') AS INT64)) AS `timestamp`, NULLIF(TRIM(`metadata`), \'\') AS `metadata`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.campaignId\')), \'\') AS `campaignId`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.adGroupId\')), \'\') AS `adGroupId`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.keywordType\')), \'\') AS `keywordType`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.keyword\')), \'\') AS `keyword`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.productTargetingType\')), \'\') AS `productTargetingType`, NULLIF(TRIM(JSON_EXTRACT_SCALAR(`metadata`, \'$.targetingExpression\')), \'\') AS `targetingExpression`, NULLIF(TRIM(`newValue`), \'\') AS `newValue`, SAFE_CAST(NULLIF(TRIM(`newValue`), \'\') AS NUMERIC) AS `newValueAmount`, SAFE_CAST(NULLIF(TRIM(`newValue`), \'\') AS BOOL) AS `newValueBool`, TIMESTAMP_MILLIS(SAFE_CAST(NULLIF(TRIM(`newValue`), \'\') AS INT64)) AS `newValueTimestamp`, NULLIF(TRIM(`previousValue`), \'\') AS `previousValue`, SAFE_CAST(NULLIF(TRIM(`previousValue`), \'\') AS NUMERIC) AS `previousValueAmount`, SAFE_CAST(NULLIF(TRIM(`previousValue`), \'\') AS BOOL) AS `previousValueBool`, TIMESTAMP_MILLIS(SAFE_CAST(NULLIF(TRIM(`previousValue`), \'\') AS INT64)) AS `previousValueTimestamp`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_aa_ka_us_get_campaign_history_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM `kerala-ayurveda-wh.MapleMonk.aa_ka_us_get_campaign_history` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY);",
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
            