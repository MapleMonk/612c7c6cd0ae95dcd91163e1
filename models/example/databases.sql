{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE MapleMonk.Zouk_Unicommerce_B2B_Orders AS WITH Unicommerce AS ( SELECT * FROM `MapleMonk.zouk_UNICOMMERCE_fact_items_intermediate_final` WHERE LOWER(marketplace) LIKE ANY ( \'%myntra_sor%\', \'%cocoblu_or%\', \'%flipkart_sor%\', \'%blinkit%\', \'%instamart%\', \'%zepto%\' ) ) , WIP_Latest AS ( SELECT LOWER(CAST(Item_id AS STRING)) AS SKU, MAX(SAFE.PARSE_DATE(\'%Y-%m-%d\', Data_Updated_Date)) AS Latest_Date FROM MapleMonk.Zouk_Zouk_S3_TranZact_WIP WHERE Item_id IS NOT NULL GROUP BY SKU ), WIP_Final AS ( SELECT LOWER(CAST(w.Item_id AS STRING)) AS SKU, SUM(SAFE_CAST(w.Estimated_Quantity AS INT64) - SAFE_CAST(w.Quantity_Produced AS INT64)) AS WIP FROM MapleMonk.Zouk_Zouk_S3_TranZact_WIP w JOIN WIP_Latest l ON LOWER(CAST(w.Item_id AS STRING)) = l.SKU AND SAFE.PARSE_DATE(\'%Y-%m-%d\', w.Data_Updated_Date) = l.Latest_Date GROUP BY SKU ) , Combined AS ( SELECT u.Initial_Order_Date, u.Order_Date, u.Dispatch_Date, u.invoiceDate, u.Shipping_Last_Update_Date, u.Order_Id, u.reference_code, u.invoiceCode, u.shippingPackageCode, u.saleOrderItemCode, u.order_status, u.Marketplace_SKU, u.SKU, fsm.commonsku, fsm.collection AS Collection, fsm.category AS Product_Category, fsm.print AS Print, fsm.Product_Type, u.Product_Name, u.Marketplace, u.Source, u.FINAL_MARKETPLACE, u.MARKETPLACE_SEGMENT, u.TYPE_OF_SALE, u.Selling_Price, u.Payment_Mode, SAFE_CAST(w.WIP AS INT64) AS WIP FROM Unicommerce u LEFT JOIN ( SELECT * FROM `MapleMonk.FINAL_SKU_MASTER` QUALIFY ROW_NUMBER() OVER (PARTITION BY LOWER(commonsku_master) ORDER BY 1) = 1 ) fsm ON LOWER(u.marketplace_sku) = LOWER(fsm.commonsku) LEFT JOIN WIP_Final w ON LOWER(u.SKU) = w.SKU ) , Ranked_Unfulfillable AS ( SELECT SKU, saleOrderItemCode, MIN(index_in_sequence) AS cum_index FROM ( SELECT LOWER(SKU) AS SKU, LOWER(saleOrderItemCode) AS saleOrderItemCode, ROW_NUMBER() OVER ( PARTITION BY LOWER(SKU) ORDER BY Initial_Order_Date, saleOrderItemCode ) - 1 AS index_in_sequence FROM Combined WHERE LOWER(order_status) = \'unfulfillable\' ) GROUP BY SKU, saleOrderItemCode ) SELECT c.*, r.cum_index, CASE WHEN LOWER(c.order_status) = \'unfulfillable\' AND c.WIP > r.cum_index THEN 1 ELSE 0 END AS wip_inventory_flag FROM Combined c LEFT JOIN Ranked_Unfulfillable r ON lower(c.sku) = lower(r.sku) AND lower(c.order_status) = \'unfulfillable\' AND lower(c.saleOrderItemCode) = lower(r.saleOrderItemCode)",
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
            