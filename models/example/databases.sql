{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_RM_QC_Report AS WITH ConvertedData AS ( SELECT Brand, SAFE_CAST(`Vendor_Code` AS STRING) AS Vendor_Code, SAFE.PARSE_DATE(\'%Y-%m-%d\', `Start_Date`) AS Start_Date, SAFE.PARSE_DATE(\'%Y-%m-%d\', `End_Date`) AS End_Date, SAFE_CAST(`QC_Accepted` AS INT64) AS QC_Accepted, SAFE_CAST(`QC_Rejected` AS INT64) AS QC_Rejected FROM `MapleMonk.Zouk_wh_RM_QC` ), DateExpansion AS ( SELECT Brand, Vendor_Code, Start_Date, End_Date, GENERATE_DATE_ARRAY(Start_Date, End_Date, INTERVAL 1 DAY) AS date_array, (DATE_DIFF(End_Date, Start_Date, DAY) + 1) AS day_count, QC_Accepted, QC_Rejected FROM ConvertedData WHERE Start_Date IS NOT NULL AND End_Date IS NOT NULL ), FinalDailyData AS ( SELECT Brand, Vendor_Code, single_day AS `Date`, CASE WHEN single_day = End_Date THEN DIV(QC_Accepted, day_count) + MOD(QC_Accepted, day_count) ELSE DIV(QC_Accepted, day_count) END AS `Daily_QC_Accepted`, CASE WHEN single_day = End_Date THEN DIV(QC_Rejected, day_count) + MOD(QC_Rejected, day_count) ELSE DIV(QC_Rejected, day_count) END AS `Daily_QC_Rejected` FROM DateExpansion, UNNEST(date_array) AS single_day ) SELECT Date, Brand, Vendor_Code, Daily_QC_Accepted, Daily_QC_Rejected, FROM FinalDailyData ORDER BY Date DESC, Brand ASC;",
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
            