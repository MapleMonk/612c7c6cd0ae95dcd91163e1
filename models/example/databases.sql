{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Blinkit_FillRate_Report AS WITH Blinkit AS ( SELECT upc, City, grn_id AS GRN_ID, SAFE_CAST(po_mrp AS FLOAT64) AS PO_MRP, SAFE_CAST(po_qty AS INT64) AS PO_QTY, SAFE_CAST(grn_mrp AS FLOAT64) AS GRN_MRP, SAFE_CAST(grn_qty AS INT64) AS GRN_QTY, b.Item_ID, PO_Type, DATE(SAFE_CAST(grn_date AS Datetime)) AS GRN_Date, SAFE_CAST(po_value AS FLOAT64) AS PO_Value, SAFE_CAST(grn_value AS FLOAT64) AS GRN_Value, PO_Number, PO_Status, Vendor_Id, Brand_Name, SAFE_CAST(cost_price AS FLOAT64) AS Cost_Price, Facility_Id, Item_Status, l0_category, l1_category, l2_category, Vendor_Name, Product_Name, Facility_Name, DATE(SAFE_CAST(insert_dt_ist AS Datetime)) AS Insert_Date, EXTRACT(ISOWEEK FROM DATE(SAFE_CAST(insert_dt_ist AS DATETIME))) AS Week_Of_Year, SAFE_CAST(landing_price AS FLOAT64) AS Landing_Price, New_PO_Status, DATE(SAFE_CAST(po_issue_date AS Datetime)) AS PO_Issue_Date, DATE(SAFE_CAST(po_expiry_date AS Datetime)) AS PO_Expiry_Date, SAFE_CAST(grn_landing_price AS FLOAT64) AS GRN_Landing_Price, Manufacturer_name, m.Zouk_Sku AS SKU FROM `MapleMonk.Zouk_Blinkit_Fill_Rate_S3_Upload` b LEFT JOIN `MapleMonk.Zouk_Blinkit_Mapping` m ON UPPER(b.item_id) = UPPER(m.Item_id) ) SELECT b.*, fsm.COMMONSKU, fsm.category AS PRODUCT_CATEGORY, fsm.Category_Code, fsm.Collection, fsm.Print, fsm.Name AS Final_Product_Name FROM Blinkit b LEFT JOIN ( SELECT * FROM maplemonk.final_sku_master QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku) ORDER BY IFNULL(commonsku, \'\') DESC) = 1 ) fsm ON LOWER(fsm.Commonsku) = LOWER(b.SKU)",
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
            