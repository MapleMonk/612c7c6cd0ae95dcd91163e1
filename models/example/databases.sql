{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_In_App_Notifications_Upload` AS SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', Start_Date) AS Start_Date, SAFE.PARSE_DATE(\'%m/%d/%Y\', End_Date) AS End_Date, CAST(REPLACE(ATC , \',\',\'\') AS FLOAT64) AS ATC, CAST(REPLACE(Open, \',\', \'\') AS FLOAT64) AS Open, CAST(REPLACE(Send, \',\', \'\') AS FLOAT64) AS Send, CAST(REPLACE(Received, \',\',\'\')AS FLOAT64) AS Received, CAST(REPLACE(Total_Nudge_Sent, \',\', \'\') AS FLOAT64) AS Total_Nudge_Sent, FROM `MapleMonk.Zouk_In_App_Notifications` ;",
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
            