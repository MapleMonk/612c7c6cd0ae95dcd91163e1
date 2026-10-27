{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.ekam_appointments_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.ekam_appointments_fact_table` WITH src AS ( SELECT *, SAFE.PARSE_JSON(NULLIF(TRIM(`metadata`), \'\'), wide_number_mode => \'round\') AS `metadata_j`, COALESCE(SAFE.PARSE_TIME(\'%H:%M:%S\', TRIM(`appointment_time`)), SAFE.PARSE_TIME(\'%H:%M\', TRIM(`appointment_time`))) AS `appointment_time_t` FROM `kerala-ayurveda-wh.MapleMonk.ekam_appointments` ) SELECT NULLIF(TRIM(`id`), \'\') AS `id`, NULLIF(TRIM(`facility_id`), \'\') AS `facility_id`, NULLIF(TRIM(`patient_id`), \'\') AS `patient_id`, NULLIF(TRIM(`doctor_staff_id`), \'\') AS `doctor_staff_id`, NULLIF(TRIM(`therapist_staff_id`), \'\') AS `therapist_staff_id`, NULLIF(TRIM(`enquiry_id`), \'\') AS `enquiry_id`, NULLIF(TRIM(`therapy_group_id`), \'\') AS `therapy_group_id`, NULLIF(TRIM(`rescheduled_from_appointment_id`), \'\') AS `rescheduled_from_appointment_id`, NULLIF(TRIM(`tentative_source_prescription_id`), \'\') AS `tentative_source_prescription_id`, NULLIF(TRIM(`type`), \'\') AS `type`, NULLIF(TRIM(`consultation_subtype`), \'\') AS `consultation_subtype`, NULLIF(TRIM(`mode`), \'\') AS `mode`, NULLIF(TRIM(`status`), \'\') AS `status`, NULLIF(TRIM(`origin`), \'\') AS `origin`, NULLIF(TRIM(`source`), \'\') AS `source`, `is_paid`, NULLIF(TRIM(`room_label`), \'\') AS `room_label`, NULLIF(TRIM(`meeting_link`), \'\') AS `meeting_link`, NULLIF(TRIM(`notes`), \'\') AS `notes`, `version`, `appointment_date`, `appointment_time_t` AS `appointment_time`, DATETIME(`appointment_date`, `appointment_time_t`) AS `appointment_start_at`, `duration_minutes`, `completed_at`, `cancelled_at`, `no_show_marked_at`, `tentative_confirmed_at`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.holdId\')), \'\') AS `hold_id`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.followUpRecommendationId\')), \'\') AS `follow_up_recommendation_id`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.packageId\')), \'\') AS `package_id`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.prescriptionTreatmentId\')), \'\') AS `prescription_treatment_id`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.sourceConsultationId\')), \'\') AS `source_consultation_id`, NULLIF(TRIM(JSON_VALUE(`metadata_j`, \'$.sourcePrescriptionId\')), \'\') AS `source_prescription_id`, `metadata_j` AS `metadata`, `created_at`, NULLIF(TRIM(`created_by`), \'\') AS `created_by`, `updated_at`, NULLIF(TRIM(`updated_by`), \'\') AS `updated_by`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_ekam_appointments_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM src;",
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
            