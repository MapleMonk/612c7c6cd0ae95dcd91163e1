{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_events_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_events_fact_table` WITH src AS ( SELECT id, api_name, module_name, Created_Time, Modified_Time, SAFE.PARSE_JSON(data) AS j, data AS raw_data, _airbyte_emitted_at FROM `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_events` ) SELECT id, api_name, module_name, Created_Time, Modified_Time, raw_data, JSON_VALUE(j, \'$.id\') AS event_id, JSON_VALUE(j, \'$.Event_Title\') AS Event_Title, JSON_VALUE(j, \'$.Venue\') AS Venue, SAFE_CAST(JSON_VALUE(j, \'$.All_day\') AS BOOL) AS All_day, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Start_DateTime\')) AS Start_DateTime, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.End_DateTime\')) AS End_DateTime, JSON_QUERY(j, \'$.Owner\') AS Owner, JSON_QUERY(j, \'$.Who_Id\') AS Who_Id, JSON_QUERY(j, \'$.What_Id\') AS What_Id, JSON_QUERY(j, \'$.Recurring_Activity\') AS Recurring_Activity, JSON_VALUE(j, \'$.Remind_At\') AS Remind_At, JSON_QUERY(j, \'$.Created_By\') AS Created_By, JSON_QUERY(j, \'$.Modified_By\') AS Modified_By, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Created_Time\')) AS Payload_Created_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Modified_Time\')) AS Payload_Modified_Time, IFNULL(JSON_QUERY_ARRAY(j, \'$.Participants\'), CAST([] AS ARRAY<JSON>)) AS Participants, JSON_VALUE(j, \'$.Description\') AS Description, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Check_In_Time\')) AS Check_In_Time, JSON_QUERY(j, \'$.Check_In_By\') AS Check_In_By, JSON_VALUE(j, \'$.Check_In_Comment\') AS Check_In_Comment, JSON_VALUE(j, \'$.Check_In_Sub_Locality\') AS Check_In_Sub_Locality, JSON_VALUE(j, \'$.Check_In_City\') AS Check_In_City, JSON_VALUE(j, \'$.Check_In_State\') AS Check_In_State, JSON_VALUE(j, \'$.Check_In_Country\') AS Check_In_Country, SAFE_CAST(JSON_VALUE(j, \'$.Latitude\') AS FLOAT64) AS Latitude, SAFE_CAST(JSON_VALUE(j, \'$.Longitude\') AS FLOAT64) AS Longitude, JSON_VALUE(j, \'$.ZIP_Code\') AS ZIP_Code, JSON_VALUE(j, \'$.Check_In_Address\') AS Check_In_Address, JSON_VALUE(j, \'$.Check_In_Status\') AS Check_In_Status, JSON_VALUE_ARRAY(j, \'$.Tag\') AS Tag, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Last_Activity_Time\')) AS Last_Activity_Time, JSON_VALUE(j, \'$.Event\') AS Event, JSON_VALUE(j, \'$.Single_Line_1\') AS Single_Line_1, JSON_VALUE(j, \'$.zohobookingstest__BookingId\') AS zohobookingstest__BookingId, JSON_VALUE(j, \'$.zohobookingstest__Booking_Summary\') AS zohobookingstest__Booking_Summary, SAFE_CAST(JSON_VALUE(j, \'$.Sync\') AS BOOL) AS Sync, _airbyte_emitted_at, CURRENT_TIMESTAMP() AS bq_load_ts FROM src;",
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
            