{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.KA_AVP_INVENTORY_FACT_ITEMS as SELECT ASIN, CAST(startDate AS DATE) inventory_date, ifnull(cast(receiveFillRate as Float64),0) receiveFillRate, ifnull(cast(openPurchaseOrderUnits as float64),0) openPO_Units, ifnull(cast(unhealthyInventoryCost as float64),0) unhealthy_Inventory_Cost, ifnull(cast(unhealthyInventoryUnits as float64),0) unhealthy_Inventory_Units, ifnull(cast(vendorConfirmationRate as float64),0) vendorConfirmationRate, ifnull(cast(JSON_EXTRACT_SCALAR(netReceivedInventoryCost,\'$.amount\') as float64),0) netReceivedInventoryCost FROM `MapleMonk.KA_AVP_GET_VENDOR_INVENTORY_REPORT`;",
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
            