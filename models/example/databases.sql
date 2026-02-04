{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_FG_QC_Report AS SELECT SAFE_CAST(`Month` AS STRING) AS Month, SAFE.PARSE_DATE(\'%d-%b-%y\', `Date`) AS Date, SAFE_CAST(`Vendor_Name` AS STRING) AS Vendor_Name, SAFE_CAST(`Category` AS STRING) AS Category_Name, SAFE_CAST(`Qc_Pass` AS INT64) AS QC_Accepted, SAFE_CAST(`Qc_Damaged` AS INT64) AS QC_Rejected, FROM `MapleMonk.Zouk_wh_FG_QC` WHERE Date IS NOT NULL",
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
            