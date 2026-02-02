{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_RM_QC_Fabric_Report AS WITH FinalData AS ( SELECT SAFE_CAST(`Month` AS STRING) AS Month, SAFE.PARSE_DATE(\'%d-%b-%y\', `Date`) AS Date, SAFE_CAST(`Person_Name` AS STRING) AS QC_Person, SAFE_CAST(`Category_Name` AS STRING) AS Category_Name, SAFE_CAST(`RM_Name` AS STRING) AS RM_Name, SAFE_CAST(`Print_Name` AS STRING) AS Print, SAFE_CAST(`Unit` AS STRING) AS Unit, SAFE_CAST(`Qc_Passed` AS INT64) AS QC_Accepted, SAFE_CAST(`Qc_Rejected` AS INT64) AS QC_Rejected, SAFE_CAST(`Parameters` AS STRING) AS QC_Parameters FROM `MapleMonk.Zouk_wh_RM_QC_Fabric` WHERE Date IS NOT NULL ) SELECT Month, Date, QC_Person, Category_Name, RM_Name, Print, Unit, QC_Accepted, QC_Rejected, QC_Parameters FROM FinalData ORDER BY Date DESC;",
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
            