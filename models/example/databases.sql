{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Fabric_QC_Update AS SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', CAST(Date AS STRING)) AS Date, CAST(Unit AS STRING) AS Unit, SAFE_CAST(Passed AS INT64) AS Passed, CAST(RM_Name AS STRING) AS RM_Name, SAFE_CAST(Rejected AS INT64) AS Rejected, CAST(QC_Person AS STRING) AS QC_Person, CAST(RM_Description AS STRING) AS RM_Description, CAST(FG_Description AS STRING) AS FG_Description, SAFE_CAST(Total_QC_Checked AS INT64) AS Total_QC_Checked FROM `MapleMonk.Zouk_Fabric_QC`;",
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
            