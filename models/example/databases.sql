{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Zouk_TranZact_WIP_Report AS WITH WIP AS ( SELECT CAST(Item_id AS STRING) AS SKU_ID, CAST(Sub_con_Vendor_Name AS STRING) AS Vendor_Name, SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date) AS Data_Updated_Date, SAFE.PARSE_DATE(\'%d-%m-%Y\', Execution_Date) AS Execution_Date, CAST(Reference_Number AS STRING) AS Reference_Number, SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS Quantity_Produced, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) AS Estimated_Quantity, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) - SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS WIP FROM `MapleMonk.Zouk_Zouk_S3_TranZact_WIP` WHERE NOT(LOWER(Status) LIKE \'%cancel%\') GROUP BY 1,2,3,4,5 ) SELECT w.*, DATE_DIFF(Data_Updated_Date, Execution_Date, DAY) AS Age, fsm.Name, fsm.Product_Type, fsm.Collection, fsm.Category, fsm.Category_Code FROM WIP w LEFT JOIN ( SELECT * FROM `MapleMonk.Final_SKU_Master` QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(COMMONSKU) ORDER BY LENGTH(IFNULL(CATEGORY, \'\'))) = 1 ) fsm ON LOWER(fsm.COMMONSKU) = LOWER(w.SKU_ID)",
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
            