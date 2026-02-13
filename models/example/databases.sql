{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.zouk_Swiggy_FillRate_Report AS WITH Swiggy AS ( SELECT City, Entity, Status AS PO_Status, p.SkuCode, SAFE_CAST(PoAmount AS FLOAT64) AS PO_MRP, PoNumber AS PO_Number, SAFE_CAST(OrderedQty AS INT64) AS PO_QTY, FacilityId AS Facility_Id, FacilityName AS Facility_Name, VendorName AS Vendor_Name, DATE(SAFE_CAST(PoCreatedAt AS Datetime)) AS PO_Date, DATE(SAFE_CAST(PoModifiedAt AS Datetime)) AS PO_Modify_Date, DATE(SAFE_CAST(PoExpiryDate AS Datetime)) AS PO_Expiry_Date, g.GRN_Date, g.GRN_QTY, g.GrnNumber, m.Sku_Code FROM `MapleMonk.Zouk_Swiggy_purchase_orders` p LEFT JOIN ( SELECT SkuCode, GrnNumber, PurchaseOrderNumber, SUM(SAFE_CAST(ReceivedQty AS INT64)) AS GRN_QTY, DATE(SAFE_CAST(CreatedAtDate AS Datetime)) AS GRN_Date FROM `MapleMonk.Zouk_Swiggy_goods_received` GROUP BY 1,2,3,5 ) g ON p.PoNumber = g.purchaseOrderNumber AND lower(p.SkuCode) = lower(g.SkuCode) LEFT JOIN `MapleMonk.Zouk_Swiggy_Mapping` m ON UPPER(p.SkuCode) = UPPER(m.Item_Code) ) SELECT s.*, fsm.COMMONSKU, fsm.category AS PRODUCT_CATEGORY, fsm.Category_Code, fsm.Collection, fsm.Print, fsm.Name AS Final_Product_Name FROM Swiggy s LEFT JOIN ( SELECT * FROM maplemonk.final_sku_master QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku) ORDER BY IFNULL(commonsku, \'\') DESC) = 1 ) fsm ON LOWER(fsm.Commonsku) = LOWER(s.SKU_Code) ;",
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
            