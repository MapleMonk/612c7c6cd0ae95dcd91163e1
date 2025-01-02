{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.outward_tracker_stores AS SELECT CASE WHEN dispatch_quantity IS NULL THEN \'STORE_PROCESSING_PENDING\' WHEN pickup_date IS NULL THEN \'PICKUPPENDING\' WHEN delivery_date IS NULL THEN \'DELIVERYPENDING\' WHEN warehouse_receipt_date IS NULL THEN \'WH_RECEIPT_PENDING\' WHEN qc_date IS NULL THEN \'QCPENDING\' WHEN po_date IS NULL THEN \'PO_NOT_RAISED\' WHEN po_received_date IS NULL THEN \'PO_PENDING\' WHEN putaway_date IS NULL THEN \'PUTAWAY_PENDING\' ELSE \'COMPLETE\' END AS current_status, CASE WHEN dispatch_quantity IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN pickup_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN delivery_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(pickup_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN warehouse_receipt_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(delivery_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN qc_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(warehouse_receipt_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN po_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(qc_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN po_received_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(po_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) WHEN putaway_date IS NULL THEN TIMESTAMPDIFF(day, TO_DATE(po_received_date, \'DD/MM/YYYY\'), CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) END AS AGEING, Type, Uniquecode, SO_number, TO_DATE(date, \'DD/MM/YYYY\') as DATE, Quantity, Store, TO_DATE(EDispatch_date, \'DD/MM/YYYY\') as EDispatch_date, BoxCount, Dispatch_Quantity, Invoice_value, approval, \"STO/PR_No\", EWaybill_Generated, PUR_NO, Pickup_Status, TO_DATE(Pickup_date, \'DD/MM/YYYY\') as Pickup_date, \"AWB/LR_NO\", DELIVERY_STATUS, TO_DATE(DELIVERY_DATE, \'DD/MM/YYYY\') as DELIVERY_DATE, REMARKS, TO_DATE(Warehouse_receipt_date, \'DD/MM/YYYY\') as Warehouse_receipt_date, BoxReceived, DC_quantity, TO_DATE(qc_date, \'DD/MM/YYYY\') as qc_date, Quantity_received, \"Short/Excess\", WBCQty, WashingQTY, BadQty, FinalQTY, TO_DATE(PO_DATE, \'DD/MM/YYYY\') as PO_DATE, Sti_NUMBER, TO_DATE(PO_RECEIVED_DATE, \'DD/MM/YYYY\') as PO_RECEIVED_DATE, PO_CODE, TO_DATE(PUTAWAY_DATE, \'DD/MM/YYYY\') as PUTAWAY_DATE, WH_REMARKS, Other FROM snitch_db.maplemonk.gs_store_outward_tracker_store;",
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
            