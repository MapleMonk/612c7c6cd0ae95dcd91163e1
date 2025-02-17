{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MapleMonk.Zouk_EBO_Calling_Data` AS SELECT FORMAT_DATE(\'%Y-%M-%D\', DATE(date)) AS Date, CAST(Store AS STRING) AS Store, CAST(Employee AS STRING) AS Employee, CAST(Calls_Made AS INT64) AS Calls_Made, CAST(Calls_Spoken AS INT64) as Calls_Spoken FROM `MapleMonk.Zouk_Daily_Calling_Data`;",
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
            