{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MAPLEMONK.Zouk_SKUWise_RM_WIP_Report1 AS WITH Inward_Data AS ( SELECT SAFE.PARSE_DATE(\'%m/%d/%Y\', Date) AS Date, CAST(SKU_ID AS STRING) AS SKU_ID, CAST(Vendor AS STRING) AS Vendor, vwm.Vendor_Name, SUM(IFNULL(SAFE_CAST(Inward_Quantity AS INT64), 0)) AS Inward_Quantity FROM `MapleMonk.Inward_Data` LEFT JOIN `MapleMonk.Zouk_Vendor_Code_Mapping` vwm ON LOWER(Vendor) = LOWER(vwm.Vendor_Code) GROUP BY 1,2,3,4 ), inwards_monthly AS ( SELECT LAST_DAY(Date) AS month, SKU_ID, Vendor, Vendor_Name, SUM(IFNULL(Inward_Quantity, 0)) AS monthly_Inward_Quantity FROM Inward_Data GROUP BY 1,2,3,4 ), Transact_WIP AS ( SELECT CAST(Item_id AS STRING) AS Item_Id, CAST(Sub_con_Vendor_Name AS STRING) AS Vendor_Name, SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date) AS Data_Updated_Date, SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS Quantity_Produced, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) AS Estimated_Quantity, SUM((SAFE_CAST(Estimated_Quantity AS INT64) - SAFE_CAST(Quantity_Produced AS INT64))) AS WIP FROM `MapleMonk.Zouk_S3_TranZact_WIP` WHERE NOT(LOWER(Status) LIKE \'%cancel%\') GROUP BY 1,2,3 ) SELECT COALESCE(ID.SKU_ID, IM.SKU_ID, TWIP.Item_Id) AS SKU_ID, COALESCE(ID.Vendor_Name, IM.Vendor_Name, TWIP.Vendor_Name) AS Vendor, COALESCE(ID.date, TWIP.Data_Updated_Date) AS date, COALESCE(ID.Inward_Quantity, 0) AS Inward_Quantity, COALESCE(IM.monthly_Inward_Quantity, 0) AS Last_month_Inward_Quantity, IFNULL(WIP, 0) AS WIP, fsm.Name, fsm.Product_Type, fsm.Collection, fsm.Category, fsm.Category_Code FROM Inward_Data ID LEFT JOIN inwards_monthly IM ON LOWER(ID.SKU_ID) = LOWER(IM.SKU_ID) AND LOWER(ID.Vendor_Name) = LOWER(IM.Vendor_Name) AND DATE_DIFF(ID.date, IM.month, MONTH) = 1 FULL OUTER JOIN Transact_WIP TWIP ON LOWER(ID.SKU_ID) = LOWER(TWIP.Item_Id) AND LOWER(ID.Vendor_Name) = LOWER(TWIP.Vendor_Name) AND ID.date = TWIP.Data_Updated_Date left join ( SELECT * FROM `MapleMonk.Final_SKU_Master` qualify ROW_NUMBER() OVER (PARTITION BY upper(COMMONSKU) ORDER BY length(ifnull(CATEGORY,\'\'))) = 1 )fsm on lower(fsm.COMMONSKU) = lower(COALESCE(ID.SKU_ID, IM.SKU_ID, TWIP.Item_Id)) ;",
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
            