{{ config(
            materialized='table',
                post_hook={
                    "sql": "ALTER SESSION SET TIMEZONE = \'Asia/Kolkata\'; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.LOGIC_BIN_STOCK_ANALYSIS as with total_stock as ( select date,branch_name,addlitemcode,sum(stock_qty) as qty from snitch_db.maplemonk.LOgicerp23_24_get_stock_in_hand where date = current_date and branch_name like \'%JAYA%\' group by 1,2,3 ), bin_stock as ( select date,branch_name,addlitemcode ,sum(stock_qty) as bin_qty from snitch_db.maplemonk.logic_bin_stock_get_stock_in_hand where date = current_date group by 1,2,3 ), uc_stock as ( select current_date as DATE, facility,\"Item SkuCode\" as uc_sku, sum(INVENTORY::INTEGER) as uc_qty from snitch_db.maplemonk.unicommerce_new_get_inventory_snapshot_export where facility like \'%EBO%\' group by 1,2,3 ), compute1 as ( select t.*, IFNULL(b.bin_qty,0) as bin_qty , IFNULL(t.qty,0) - IFNULL(b.bin_qty,0) as floor_qty, u.uc_qty, bin_qty - uc_qty as UC_DIFF from total_stock t left join bin_stock b on CONCAT(t.branch_name,t.addlitemcode) = CONCAT(b.branch_name,b.addlitemcode) left join uc_stock u on CONCAT(\'JAYANAGAR_EBO\',t.addlitemcode) = CONCAT(u.facility,u.uc_sku) ) select * from compute1 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from snitch_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            