{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_App_Performance_Update AS SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\',Start_Date) AS Start_Date, SAFE.PARSE_DATE(\'%Y-%m-%d\',End_Date) AS End_Date, CAST(Total_Orders AS INT64) AS Total_Orders, CAST(REPLACE(AOV, \',\', \'\') AS FLOAT64) AS AOV, CAST(REPLACE(Total_Revenue, \',\', \'\') AS FLOAT64) AS Total_Revenue, CAST(REPLACE(Conversion_Rate, \'%\',\'\')AS FLOAT64) AS Conversion_Rate, CAST(REPLACE(Total_Downloads, \',\', \'\') AS FLOAT64) AS Total_Downloads, CAST(REPLACE(Total_New_Download, \',\', \'\') AS FLOAT64) AS Total_New_Download, CAST(REPLACE(Total_Spends_meta, \',\', \'\') AS FLOAT64) AS Total_Spends_meta, CAST(REPLACE(Cost_per_New_Download, \',\', \'\') AS FLOAT64) AS Cost_per_New_Download FROM MapleMonk.Zouk_App_Performance ;",
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
            