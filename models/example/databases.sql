{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM maplemonk.test_v_get_schedule_items; SELECT Id, REGEXP_REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( REGEXP_REPLACE(Bio, r\"<[^>]+>\", \"\"), \"&nbsp;\", \" \"), \"&rsquo;\", \"\'\"), \"&ldquo;\", \'\"\'), \"&rdquo;\", \'\"\'), r\"\s+\", \" \" ) AS Cleaned_Bio, Rep, City, Name, Rep2, Rep3, Rep4, Rep5, Rep6, Email, EmpID, State, IsMale, Address, Country, site_id, ImageUrl, LastName, FirstName, HomePhone, SortOrder, WorkPhone, PostalCode, DisplayName, MobilePhone, ProviderIDs, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.ProgramId\') AS Appointment_ProgramId, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.ClientId\') AS Appointment_ClientId, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.StartDateTime\') AS Appointment_StartDateTime, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.EndDateTime\') AS Appointment_EndDateTime, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.Status\') AS Appointment_Status, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.Duration\') AS Appointment_Duration, JSON_EXTRACT_SCALAR(JSON_VALUE(Appointments[SAFE_OFFSET(0)]), \'$.Notes\') AS Appointment_Notes, ClassTeacher, EmploymentEnd, JSON_EXTRACT_SCALAR(StaffSettings, \'$.ShowStaffLastNamesOnSchedules\') AS ShowStaffLastNamesOnSchedules, JSON_EXTRACT_SCALAR(StaffSettings, \'$.UseStaffNicknames\') AS UseStaffNicknames, JSON_EXTRACT_SCALAR(JSON_VALUE(Availabilities[SAFE_OFFSET(0)]), \'$.Programs[0].Id\') AS Availability_ProgramId, JSON_EXTRACT_SCALAR(JSON_VALUE(Availabilities[SAFE_OFFSET(0)]), \'$.Programs[0].Name\') AS Availability_ProgramName, JSON_EXTRACT_SCALAR(JSON_VALUE(Availabilities[SAFE_OFFSET(0)]), \'$.Programs[0].ScheduleType\') AS Availability_ScheduleType, JSON_EXTRACT_SCALAR(JSON_VALUE(Availabilities[SAFE_OFFSET(0)]), \'$.Programs[0].CancelOffset\') AS Availability_CancelOffset, ClassAssistant, ClassAssistant2, EmploymentStart, JSON_EXTRACT_SCALAR(JSON_VALUE(Unavailabilities[SAFE_OFFSET(0)]), \'$.StartDateTime\') AS Unavailable_StartDateTime, JSON_EXTRACT_SCALAR(JSON_VALUE(Unavailabilities[SAFE_OFFSET(0)]), \'$.EndDateTime\') AS Unavailable_EndDateTime, JSON_EXTRACT_SCALAR(JSON_VALUE(Unavailabilities[SAFE_OFFSET(0)]), \'$.Description\') AS Unavailable_Description, AppointmentInstructor, IndependentContractor, AlwaysAllowDoubleBooking, _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, _airbyte_test_v_get_schedule_items_hashid FROM maplemonk.test_v_get_schedule_items;",
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
            