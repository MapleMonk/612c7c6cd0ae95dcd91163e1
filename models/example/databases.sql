{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.KA_AVP_INVENTORY_FACT_ITEMS AS SELECT a.ASIN, b.COMMONSKU, b.Name, CAST(a.startDate AS DATE) AS inventory_date, \'AMAZON VENDOR CENTRAL INDIA\' AS Marketplace, IFNULL(CAST(a.receiveFillRate AS FLOAT64), 0) AS receiveFillRate, IFNULL(CAST(a.openPurchaseOrderUnits AS FLOAT64), 0) AS openPO_Units, IFNULL(CAST(a.unhealthyInventoryCost AS FLOAT64), 0) AS unhealthy_Inventory_Cost, IFNULL(CAST(a.unhealthyInventoryUnits AS FLOAT64), 0) AS unhealthy_Inventory_Units, IFNULL(CAST(a.vendorConfirmationRate AS FLOAT64), 0) AS vendorConfirmationRate, IFNULL(CAST(JSON_EXTRACT_SCALAR(a.netReceivedInventoryCost, \'$.amount\') AS FLOAT64), 0) AS netReceivedInventoryCost, IFNULL(CAST(JSON_EXTRACT_SCALAR(a.sellableOnHandInventoryCost, \'$.amount\') AS FLOAT64), 0) AS sellableOnHand_Inventory_Cost, IFNULL(CAST(a.sellableOnHandInventoryUnits AS FLOAT64), 0) AS sellableOnHand_Inventory_Units, IFNULL(CAST(a.procurableProductOutOfStockRate AS FLOAT64), 0) AS procurableProductOutOfStockRate, IFNULL(CAST(a.sourceableProductOutOfStockRate AS FLOAT64), 0) AS sourceableProductOutOfStockRate FROM `MapleMonk.KA_AVP_GET_VENDOR_INVENTORY_REPORT` a LEFT JOIN `kerala-ayurveda-wh.MapleMonk.FINAL_SKU_MASTER` b ON a.ASIN = b.Marketplace_SKU AND b.MARKETPLACE = \'AMAZON VENDOR CENTRAL INDIA\';",
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
            