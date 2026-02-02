{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_RM_QC_Fabric_Report AS WITH ConvertedData AS ( SELECT SAFE_CAST(`Month` AS STRING) AS Month, SAFE.PARSE_DATE(\'%d-%b-%Y\', `Date`) AS Start_Date, SAFE_CAST(`Person_Name` AS STRING) AS Person_Name, SAFE_CAST(`Category_Name` AS STRING) AS Category_Name, SAFE_CAST(`RM_Name` AS STRING) AS RM_Name, SAFE_CAST(`Print_Name` AS STRING) AS Print, SAFE_CAST(`Unit` AS STRING) AS Unit, SAFE_CAST(`Qc_Passed` AS INT64) AS QC_Accepted, SAFE_CAST(`Qc_Rejected` AS INT64) AS QC_Rejected, SAFE_CAST(`Parameters` AS STRING) AS Parameters FROM `MapleMonk.Zouk_wh_RM_QC_Fabric` ), FinalData AS ( SELECT Month, FORMAT_DATE(\'%Y-%m-%d\', Start_Date) AS Date, Person_Name, Category_Name, RM_Name, Print, Unit, QC_Accepted, QC_Rejected, Parameters FROM ConvertedData WHERE Start_Date IS NOT NULL ) SELECT Month, Date, Person_Name, Category_Name, RM_Name, Print, Unit, QC_Accepted, QC_Rejected, FROM FinalData ORDER BY Date DESC;",
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
            