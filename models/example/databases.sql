{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM maplemonk.test_v_get_staff_appointments; SELECT Id, Notes, -- Staff JSON fields JSON_EXTRACT_SCALAR(Staff, \'$.FirstName\') AS StaffFirstName, JSON_EXTRACT_SCALAR(Staff, \'$.LastName\') AS StaffLastName, JSON_EXTRACT_SCALAR(Staff, \'$.DisplayName\') AS StaffDisplayName, JSON_EXTRACT_SCALAR(Staff, \'$.Id\') AS StaffJsonId, -- Resources JSON fields (only extract if not null or empty) CASE WHEN Resources IS NOT NULL AND ARRAY_LENGTH(Resources) > 0 THEN JSON_EXTRACT_SCALAR(Resources[OFFSET(0)], \'$.Id\') ELSE NULL END AS ResourceId, CASE WHEN Resources IS NOT NULL AND ARRAY_LENGTH(Resources) > 0 THEN JSON_EXTRACT_SCALAR(Resources[OFFSET(0)], \'$.Name\') ELSE NULL END AS ResourceName, AddOns, Status, StaffId, ClientId, Duration, ProgramId, IsWaitlist, LocationId, ProviderId, EndDateTime, SessionTypeId, StartDateTime, StaffRequested, ClientServiceId, WaitlistEntryId, FirstAppointment, GenderPreference, OnlineDescription, LastModifiedDateTime, -- Airbyte metadata _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, _airbyte_test_v_get_staff_appointments_hashid FROM `maplemonk.test_v_get_staff_appointments`",
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
            