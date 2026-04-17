{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinar_registrants_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinar_registrants_fact_table` WITH src AS ( SELECT id, api_name, module_name, Created_Time, Modified_Time, SAFE.PARSE_JSON(data) AS j, data AS raw_data, _airbyte_emitted_at FROM `kerala-ayurveda-wh.MapleMonk.us_academy_zoho_webinar_registrants` ) SELECT id, api_name, module_name, Created_Time, Modified_Time, raw_data, JSON_VALUE(j, \'$.id\') AS webinar_participation_id, JSON_VALUE(j, \'$.Name\') AS Name, JSON_QUERY(j, \'$.Owner\') AS Owner, JSON_VALUE(j, \'$.Email\') AS Email, JSON_VALUE(j, \'$.Secondary_Email\') AS Secondary_Email, JSON_QUERY(j, \'$.Created_By\') AS Created_By, JSON_QUERY(j, \'$.Modified_By\') AS Modified_By, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Created_Time\')) AS Payload_Created_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Modified_Time\')) AS Payload_Modified_Time, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Last_Activity_Time\')) AS Last_Activity_Time, SAFE_CAST(JSON_VALUE(j, \'$.Email_Opt_Out\') AS BOOL) AS Email_Opt_Out, JSON_VALUE_ARRAY(j, \'$.Tag\') AS Tag, JSON_VALUE(j, \'$.Unsubscribed_Mode\') AS Unsubscribed_Mode, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Unsubscribed_Time\')) AS Unsubscribed_Time, JSON_VALUE(j, \'$.Record_Image\') AS Record_Image, JSON_QUERY(j, \'$.Webinar\') AS Webinar, SAFE.TIMESTAMP(JSON_VALUE(j, \'$.Registration_Time\')) AS Registration_Time, JSON_VALUE(j, \'$.Status\') AS Status, SAFE_CAST(JSON_VALUE(j, \'$.Locked__s\') AS BOOL) AS Locked__s, JSON_QUERY(j, \'$.Contact\') AS Contact, JSON_VALUE(j, \'$.Join_URL\') AS Join_URL, JSON_QUERY(j, \'$.Lead\') AS Lead, JSON_VALUE(j, \'$.Registration_ID\') AS Registration_ID, SAFE_CAST(JSON_VALUE(j, \'$.Sync\') AS BOOL) AS Sync, _airbyte_emitted_at, CURRENT_TIMESTAMP() AS bq_load_ts FROM src;",
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
            