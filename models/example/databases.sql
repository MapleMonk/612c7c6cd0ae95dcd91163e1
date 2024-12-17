{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.outward_tracker_stores as select Type, Uniquecode, SO_number, Date, Quantity, Store, EDispatch_date, BoxCount, Dispatch_Quantity, Invoice_value, approval, \"STO/PR_No\", EWaybill_Generated, PUR_NO, Pickup_Status, Pickup_date, \"AWB/LR_NO\", DELIVERY_STATUS, DELIVERY_DATE, REMARKS, Warehouse_receipt_date, BoxReceived, DC_quantity, qc_date, Quantity_received, \"Short/Excess\", WBCQty, WashingQTY, BadQty, FinalQTY, PO_DATE, Sti_NUMBER, PO_RECEIVED_DATE, PO_CODE, PUTAWAY_DATE, WH_REMARKS, Other from snitch_db.maplemonk.gs_store_outward_tracker_store",
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
            