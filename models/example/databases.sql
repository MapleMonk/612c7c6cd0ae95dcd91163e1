{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Retention_Update AS SELECT SAFE.PARSE_DATE(\'%Y-%m-%d\', Start_Date) AS Start_Date, SAFE.PARSE_DATE(\'%Y-%m-%d\', End_Date) AS End_Date, SAFE_CAST(REPLACE(AOV, \',\', \'\') AS FLOAT64) AS AOV, CAST(Type AS STRING) AS Type, SAFE_CAST(REPLACE(Spends, \',\', \'\') AS FLOAT64) AS Spends, SAFE_CAST(REPLACE(Revenue, \',\', \'\') AS FLOAT64) AS Revenue, CAST(Channel AS STRING) AS Channel, SAFE_CAST(REPLACE(Delivered, \',\', \'\') AS FLOAT64) AS Delivered, SAFE_CAST(REPLACE(Open_Count, \',\', \'\') AS FLOAT64) AS Open_Count, SAFE_CAST(REPLACE(Click_Count, \',\', \'\') AS FLOAT64) AS Click_Count, SAFE_CAST(REPLACE(Sends_Count, \',\', \'\') AS FLOAT64) AS Sends_Count, SAFE_CAST(REPLACE(Conversion_Count, \',\', \'\') AS FLOAT64) AS Conversion_Count FROM MapleMonk.Zouk_Retention ;",
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
            