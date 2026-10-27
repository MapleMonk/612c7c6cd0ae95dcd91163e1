{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointment_therapists_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointment_therapists_fact_table` SELECT NULLIF(TRIM(`appointment_id`), \'\') AS `appointment_id`, NULLIF(TRIM(`therapist_staff_id`), \'\') AS `therapist_staff_id`, `created_at`, NULLIF(TRIM(`created_by`), \'\') AS `created_by`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_ekam_therapy_appointment_therapists_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM `kerala-ayurveda-wh.MapleMonk.ekam_therapy_appointment_therapists` AS src;",
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
            