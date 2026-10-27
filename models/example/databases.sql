{{ config(
            materialized='table',
                post_hook={
                    "sql": "DELETE FROM `kerala-ayurveda-wh.MapleMonk.ekam_enquiries_fact_table` WHERE `_airbyte_unique_key` IN ( SELECT `_airbyte_unique_key` FROM `kerala-ayurveda-wh.MapleMonk.ekam_enquiries` WHERE `_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY) ); INSERT INTO `kerala-ayurveda-wh.MapleMonk.ekam_enquiries_fact_table` WITH src AS ( SELECT *, SAFE.PARSE_JSON(NULLIF(TRIM(`metadata`), \'\'), wide_number_mode => \'round\') AS `metadata_j` FROM `kerala-ayurveda-wh.MapleMonk.ekam_enquiries` ) SELECT `_airbyte_unique_key`, NULLIF(TRIM(`id`), \'\') AS `id`, NULLIF(TRIM(`facility_id`), \'\') AS `facility_id`, NULLIF(TRIM(`patient_id`), \'\') AS `patient_id`, NULLIF(TRIM(`linked_appointment_id`), \'\') AS `linked_appointment_id`, NULLIF(TRIM(`linked_medicine_sale_id`), \'\') AS `linked_medicine_sale_id`, NULLIF(TRIM(`linked_package_id`), \'\') AS `linked_package_id`, NULLIF(TRIM(`upsold_consultation_id`), \'\') AS `upsold_consultation_id`, NULLIF(TRIM(`enquiry_type`), \'\') AS `enquiry_type`, NULLIF(TRIM(`channel`), \'\') AS `channel`, NULLIF(TRIM(`source`), \'\') AS `source`, NULLIF(TRIM(`status`), \'\') AS `status`, NULLIF(TRIM(`patient_type`), \'\') AS `patient_type`, NULLIF(TRIM(`current_complaint`), \'\') AS `current_complaint`, NULLIF(TRIM(`patient_query`), \'\') AS `patient_query`, NULLIF(TRIM(`notes`), \'\') AS `notes`, NULLIF(TRIM(`clarification_notes`), \'\') AS `clarification_notes`, NULLIF(TRIM(`resolution_notes`), \'\') AS `resolution_notes`, NULLIF(TRIM(`not_converted_reason`), \'\') AS `not_converted_reason`, NULLIF(TRIM(`upsell_outcome`), \'\') AS `upsell_outcome`, NULLIF(TRIM(`upsell_failed_reason`), \'\') AS `upsell_failed_reason`, NULLIF(TRIM(`patient_name`), \'\') AS `patient_name`, NULLIF(TRIM(`patient_phone`), \'\') AS `patient_phone`, NULLIF(TRIM(`patient_email`), \'\') AS `patient_email`, NULLIF(TRIM(`patient_gender`), \'\') AS `patient_gender`, `patient_age`, `patient_date_of_birth`, `callback_date`, `callback_time`, `follow_up_date`, `arrived`, `arrived_at`, `entered_at`, `first_conversion_at`, `reopened_at`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.legacyLinkedPatientId\')), \'\') AS `legacy_linked_patient_id`, `metadata_j` AS `metadata`, `created_at`, NULLIF(TRIM(`created_by`), \'\') AS `created_by`, `updated_at`, NULLIF(TRIM(`updated_by`), \'\') AS `updated_by`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_ekam_enquiries_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM src WHERE src.`_airbyte_emitted_at` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY);",
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
            