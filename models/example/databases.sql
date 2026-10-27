{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointments_fact_table` WHERE `_airbyte_unique_key` IN ( SELECT `_airbyte_unique_key` FROM `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointments` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY) ); INSERT INTO `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointments_fact_table` WITH src AS ( SELECT *, SAFE.PARSE_JSON(NULLIF(TRIM(`selected_body_parts`), \'\'), wide_number_mode => \'round\') AS `selected_body_parts_j`, SAFE.PARSE_JSON(NULLIF(TRIM(`spine_levels`), \'\'), wide_number_mode => \'round\') AS `spine_levels_j`, SAFE.PARSE_JSON(NULLIF(TRIM(`selected_oils`), \'\'), wide_number_mode => \'round\') AS `selected_oils_j` FROM `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointments` ) SELECT `_airbyte_unique_key`, NULLIF(TRIM(`appointment_id`), \'\') AS `appointment_id`, NULLIF(TRIM(`treatment_name`), \'\') AS `treatment_name`, NULLIF(TRIM(`body_part`), \'\') AS `body_part`, JSON_VALUE_ARRAY(`selected_body_parts_j`, \'$\') AS `selected_body_parts`, JSON_VALUE_ARRAY(`spine_levels_j`, \'$\') AS `spine_levels`, JSON_VALUE_ARRAY(`selected_oils_j`, \'$\') AS `selected_oils`, NULLIF(TRIM(`oil_temperature`), \'\') AS `oil_temperature`, NULLIF(TRIM(`custom_oil_temperature`), \'\') AS `custom_oil_temperature`, NULLIF(TRIM(`pressure`), \'\') AS `pressure`, NULLIF(TRIM(`stroke_direction`), \'\') AS `stroke_direction`, NULLIF(TRIM(`concern`), \'\') AS `concern`, NULLIF(TRIM(`focus_notes`), \'\') AS `focus_notes`, NULLIF(TRIM(`notes`), \'\') AS `notes`, `created_at`, NULLIF(TRIM(`created_by`), \'\') AS `created_by`, `updated_at`, NULLIF(TRIM(`updated_by`), \'\') AS `updated_by`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_ekam_therapy_appointments_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM src WHERE src.`_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY);",
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
            