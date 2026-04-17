{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinars_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinars_fact_table` WITH src AS ( SELECT id, api_name, module_name, Created_Time, Modified_Time, SAFE.PARSE_JSON(data) AS j, data AS raw_data, _airbyte_emitted_at FROM `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinars` ) SELECT id, api_name, module_name, Created_Time, Modified_Time, raw_data, JSON_VALUE(j, \'$.id\') AS webinar_id, JSON_QUERY(j, \'$.Owner\') AS Owner, JSON_VALUE(j, \'$.Name\') AS Name, JSON_VALUE(j, \'$.Secondary_Email\') AS Secondary_Email, JSON_QUERY(j, \'$.Created_By\') AS Created_By, JSON_QUERY(j, \'$.Modified_By\') AS Modified_By, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Created_Time\')) AS Payload_Created_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Modified_Time\')) AS Payload_Modified_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Last_Activity_Time\')) AS Last_Activity_Time, SAFE_CAST(JSON_VALUE(j, \'$.Email_Opt_Out\') AS BOOL) AS Email_Opt_Out, JSON_QUERY(j, \'$.Tag\') AS Tag, JSON_VALUE(j, \'$.Unsubscribed_Mode\') AS Unsubscribed_Mode, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Unsubscribed_Time\')) AS Unsubscribed_Time, JSON_VALUE(j, \'$.Record_Image\') AS Record_Image, JSON_VALUE(j, \'$.Host_email\') AS Host_email, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Start_Time\')) AS Start_Time, SAFE_CAST(JSON_VALUE(j, \'$.Duration\') AS INT64) AS Duration, JSON_VALUE(j, \'$.Status\') AS Status, SAFE_CAST(JSON_VALUE(j, \'$.Locked__s\') AS BOOL) AS Locked__s, JSON_VALUE(j, \'$.Zoom_Webinar_ID\') AS Zoom_Webinar_ID, JSON_VALUE(j, \'$.UUID\') AS UUID, JSON_VALUE(j, \'$.Join_URL\') AS Join_URL, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Actual_Start_Time\')) AS Actual_Start_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Actual_end_time\')) AS Actual_end_time, SAFE_CAST(JSON_VALUE(j, \'$.Actual_Duration\') AS INT64) AS Actual_Duration, _airbyte_emitted_at, CURRENT_TIMESTAMP() AS bq_load_ts FROM src;",
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
            