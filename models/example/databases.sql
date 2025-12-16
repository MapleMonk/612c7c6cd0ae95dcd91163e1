{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Geek_AMAZON_VENDOR_PARTNER AS SELECT CAST(asin AS STRING) AS Asin, DATETIME(startTime, \'Asia/Kolkata\') AS StartTime, datetime(endtime,\"Asia/Kolkata\") AS EndTime, CAST(orderedUnits AS INT64) AS OrderedUnits, CAST(orderedRevenue AS float64) AS OrderedRevenue FROM `MapleMonk.Amazon_VP_Geek_GET_VENDOR_REAL_TIME_SALES_REPORT`;",
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
            