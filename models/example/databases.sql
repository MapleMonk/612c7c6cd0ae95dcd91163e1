{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Unicommerce_B2B_Orders AS WITH Unicommerce AS ( SELECT * FROM ( SELECT *, ROW_NUMBER() OVER ( PARTITION BY saleOrderitemCode ) AS rn FROM `MapleMonk.zouk_UNICOMMERCE_fact_items_intermediate_final` WHERE LOWER(marketplace) LIKE ANY ( \'%myntra_sor%\', \'%cocoblu_or%\', \'%flipkart_sor%\', \'%blinkit%\', \'%instamart%\', \'%zepto%\' ) ) WHERE rn = 1 ), Combined AS ( SELECT u.Initial_Order_Date, u.Order_Date, u.Dispatch_Date, u.invoiceDate, u.Shipping_Last_Update_Date, u.Order_Id, u.reference_code, u.invoiceCode, u.shippingPackageCode, u.saleOrderItemCode, u.order_status, u.SKU, fsm.commonsku, fsm.collection AS Collection, fsm.category AS Product_Category, fsm.print AS Print, fsm.Product_Type, u.Product_Name, u.Marketplace, u.Source, u.FINAL_MARKETPLACE, u.MARKETPLACE_SEGMENT, u.TYPE_OF_SALE, u.Selling_Price, u.Payment_Mode, SUM(IFNULL(WIP.WIP,0)) AS WIP FROM Unicommerce u LEFT JOIN ( SELECT * FROM `MapleMonk.FINAL_SKU_MASTER` QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku_master) ORDER BY 1) = 1 ) fsm ON LOWER(u.sku) = LOWER(fsm.commonsku) LEFT JOIN ( SELECT LOWER(CAST(Item_id AS STRING)) AS Item_Id, SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date) AS Data_Updated_Date, SUM(SAFE_CAST(Estimated_Quantity AS INT64)) AS Estimated_Quantity, SUM(SAFE_CAST(Quantity_Produced AS INT64)) AS Quantity_Produced, SUM(SAFE_CAST(Estimated_Quantity AS INT64) - SAFE_CAST(Quantity_Produced AS INT64)) AS WIP FROM MapleMonk.Zouk_Zouk_S3_TranZact_WIP WHERE Item_id IS NOT NULL GROUP BY LOWER(CAST(Item_id AS STRING)), SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date) ) wip ON LOWER(u.SKU) = wip.Item_Id AND DATE(u.initial_order_date) = wip.Data_Updated_Date GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25 ) SELECT c.*, CASE WHEN ROW_NUMBER() OVER (PARTITION BY sku, DATE(initial_order_date) ORDER BY saleOrderItemCode) = 1 THEN WIP ELSE 0 END AS wip_inventory FROM Combined c ;",
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
            