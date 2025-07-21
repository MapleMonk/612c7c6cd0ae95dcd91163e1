{{ config(
            materialized='table',
                post_hook={
                    "sql": "SELECT * FROM maplemonk.test_v_get_classes; SELECT Id, Active, Visits, Clients, WebBooked, ClassNotes, HideCancel, IsCanceled, IsEnrolled, SemesterId, Substitute, EndDateTime, IsAvailable, MaxCapacity, TotalBooked, WebCapacity, WaitListSize, BookingStatus, StartDateTime, TotalSignedIn, ClassScheduleId, VirtualStreamLink, IsWaitlistAvailable, JSON_EXTRACT_SCALAR(Staff, \'$.Id\') AS StaffId, JSON_EXTRACT_SCALAR(Staff, \'$.FirstName\') AS StaffFirstName, JSON_EXTRACT_SCALAR(Staff, \'$.LastName\') AS StaffLastName, JSON_EXTRACT_SCALAR(Staff, \'$.DisplayName\') AS StaffDisplayName, JSON_EXTRACT_SCALAR(Staff, \'$.State\') AS StaffState, JSON_EXTRACT_SCALAR(Staff, \'$.Email\') AS StaffEmail, JSON_EXTRACT_SCALAR(Staff, \'$.MobilePhone\') AS StaffMobilePhone, JSON_EXTRACT_SCALAR(Location, \'$.Id\') AS LocationId, JSON_EXTRACT_SCALAR(Location, \'$.Name\') AS LocationName, JSON_EXTRACT_SCALAR(Location, \'$.Address\') AS LocationAddress, JSON_EXTRACT_SCALAR(Location, \'$.City\') AS LocationCity, JSON_EXTRACT_SCALAR(Location, \'$.StateProvCode\') AS LocationState, JSON_EXTRACT_SCALAR(Location, \'$.PostalCode\') AS LocationPostalCode, JSON_EXTRACT_SCALAR(Location, \'$.Phone\') AS LocationPhone, JSON_EXTRACT_SCALAR(Location, \'$.SiteID\') AS LocationSiteID, JSON_EXTRACT_SCALAR(BookingWindow, \'$.StartDateTime\') AS BookingStart, JSON_EXTRACT_SCALAR(BookingWindow, \'$.EndDateTime\') AS BookingEnd, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Id\') AS ClassId, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Name\') AS ClassName, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Description\') AS ClassDescription, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Category\') AS ClassCategory, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Subcategory\') AS ClassSubcategory, JSON_EXTRACT_SCALAR(ClassDescription, \'$.Program.Name\') AS ProgramName, JSON_EXTRACT_SCALAR(ClassDescription, \'$.SessionType.Name\') AS SessionTypeName, JSON_EXTRACT_SCALAR(ClassDescription, \'$.ScheduleType\') AS SessionScheduleType, JSON_EXTRACT_SCALAR(Resource, \'$.Id\') AS ResourceId, JSON_EXTRACT_SCALAR(Resource, \'$.Name\') AS ResourceName, TotalBookedWaitlist, LastModifiedDateTime, _airbyte_ab_id, _airbyte_emitted_at, _airbyte_normalized_at, _airbyte_test_v_get_classes_hashid FROM maplemonk.test_v_get_classes;",
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
            