{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `MAPLEMONK.Freakins_GRN_Factitems` AS SELECT PO_Code, SAFE_CAST(PO_Date AS DATETIME) AS PO_Date, SAFE_CAST(Updated AS DATETIME) AS Updated_Timestamp, Category, Facility, GRN_Code, SAFE_CAST(GRN_Date AS DATETIME) AS GRN_Date, Batch_Code, SAFE_CAST(Expiry_Date AS DATE) AS Expiry_Date, Vendor_Code, Vendor_Name, Item_SkuCode, GRN_Created_By, GRN_Invoice_No, Item_Type_Name, Item_Type_Size, Vendor_SkuCode, Warehouse_Name, SAFE_CAST(Additional_Cost AS FLOAT64) AS Additional_Cost, Grn_item_Status, Item_Type_Brand, Item_Type_Color, SAFE_CAST(QC_Completed_On AS DATETIME) AS QC_Completed_On, SAFE_CAST(GRN_Invoice_Date AS DATE) AS GRN_Invoice_Date, Rejection_Reason, SAFE_CAST(Quantity_Received AS INT64) AS Quantity_Received, SAFE_CAST(Quantity_Rejected AS INT64) AS Quantity_Rejected, SAFE_CAST(Percentage_Rejection AS FLOAT64) AS Percentage_Rejection, SAFE_CAST(GRN_Received_Timestamp AS DATETIME) AS GRN_Received_Timestamp, SAFE_CAST(Values_of_Goods_Received_with_taxes AS FLOAT64) AS Value_Received_With_Tax, SAFE_CAST(Values_of_Goods_Rejected_with_taxes AS FLOAT64) AS Value_Rejected_With_Tax, SAFE_CAST(Values_of_Goods_Received_without_taxes AS FLOAT64) AS Value_Received_Without_Tax, SAFE_CAST(Values_of_Goods_Rejected_without_taxes AS FLOAT64) AS Value_Rejected_Without_Tax, CASE WHEN SAFE_CAST(GRN_Date AS DATE) IS NULL THEN \'Unknown\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN \'0-30\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) THEN \'30-60\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN \'60-90\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY) THEN \'90-120\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY) THEN \'120-180\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 270 DAY) THEN \'180-270\' WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 360 DAY) THEN \'270-360\' ELSE \'360+\' END AS Bucket, CASE WHEN DATE(GRN_Date) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN \'0-30\' ELSE \'30+\' END AS Extra_Bucket FROM `MAPLEMONK.Unicommerence_Freakins_get_grn_report_new`;",
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
            