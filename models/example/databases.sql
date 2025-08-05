{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Zouk_SKUWise_RM_WIP_Report1 AS WITH Inward_Data AS ( SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', Date) AS Date, CAST(SKU_ID AS STRING) AS SKU_ID, CAST(Vendor AS STRING) AS Vendor, COALESCE(vwm.Vendor_Name, Vendor) AS Vendor_Name, SAFE_CAST(Inward_Quantity AS INT64) AS Inward_Quantity FROM `MapleMonk.Inward_Data` LEFT JOIN `MapleMonk.Zouk_Vendor_Code_Mapping` vwm ON LOWER(Vendor) = LOWER(vwm.Vendor_Code) WHERE SAFE.PARSE_DATE(\'%m/%d/%Y\', Date) IS NOT NULL ), Inward_30_Days AS ( SELECT SKU_ID, Vendor_Name, SUM(IFNULL(Inward_Quantity, 0)) AS Inward_30_Days FROM Inward_Data WHERE Date >= DATE_SUB(CURRENT_DATE() -1, INTERVAL 30 DAY) GROUP BY SKU_ID, Vendor_Name ), Inward_90_Days AS ( SELECT SKU_ID, Vendor_Name, SUM(IFNULL(Inward_Quantity, 0)) AS Inward_90_Days FROM Inward_Data WHERE Date >= DATE_SUB(CURRENT_DATE() -1, INTERVAL 90 DAY) GROUP BY SKU_ID, Vendor_Name ), Transact_WIP AS ( SELECT CAST(Item_id AS STRING) AS SKU_ID, CAST(Sub_con_Vendor_Name AS STRING) AS Vendor_Name, SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date) AS Data_Updated_Date, SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS Quantity_Produced, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) AS Estimated_Quantity, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) - SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS WIP FROM `MapleMonk.Zouk_Zouk_S3_TranZact_WIP` WHERE NOT(LOWER(Status) LIKE \'%cancel%\') GROUP BY SKU_ID, Vendor_Name, Data_Updated_Date ) SELECT COALESCE(twip.SKU_ID, i30.SKU_ID, i90.SKU_ID) AS SKU_ID, COALESCE(twip.Vendor_Name, i30.Vendor_Name, i90.Vendor_Name) AS Vendor_Name, twip.Data_Updated_Date, IFNULL(twip.WIP, 0) AS WIP, IFNULL(i30.Inward_30_Days, 0) AS Inward_30_Days, IFNULL(i90.Inward_90_Days, 0) AS Inward_90_Days, fsm.Name, fsm.Product_Type, fsm.Collection, fsm.Category, fsm.Category_Code FROM Transact_WIP twip FULL OUTER JOIN Inward_30_Days i30 ON LOWER(twip.SKU_ID) = LOWER(i30.SKU_ID) AND LOWER(twip.Vendor_Name) = LOWER(i30.Vendor_Name) FULL OUTER JOIN Inward_90_Days i90 ON LOWER(COALESCE(twip.SKU_ID, i30.SKU_ID)) = LOWER(i90.SKU_ID) AND LOWER(COALESCE(twip.Vendor_Name, i30.Vendor_Name)) = LOWER(i90.Vendor_Name) LEFT JOIN ( SELECT * FROM `MapleMonk.Final_SKU_Master` QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(COMMONSKU) ORDER BY LENGTH(IFNULL(CATEGORY, \'\'))) = 1 ) fsm ON LOWER(fsm.COMMONSKU) = LOWER(COALESCE(twip.SKU_ID, i30.SKU_ID, i90.SKU_ID))",
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
            