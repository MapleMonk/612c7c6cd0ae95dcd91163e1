{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_mp_performance AS WITH uc AS ( SELECT * FROM snitch_db.maplemonk.warehouse_sla_performance WHERE marketplace_mapped NOT IN ( \'STORE\',\'MYNTRA\',\'AJIO\',\'FLIPKART\',\'SHOPIFY\',\'\' ) ), mp AS ( SELECT UPPER(TRIM(SO_NUMBER)) AS mp_so_key, SO_NUMBER AS mp_so_number, PO_NUMBER AS mp_po_number, DC_NUMBER AS mp_dc_number, LR_NO AS mp_lr_no, INVOICE_NUMBER AS mp_invoice_number, CHANNEL AS mp_channel, MARKETPLACE_NAME AS mp_marketplace_name, FACILITY AS mp_facility, CITY AS mp_city, \"Address \" AS mp_address, STATUS AS mp_status, DELIVERY_STATUS AS mp_delivery_status, CURRENT_REMARKS AS mp_current_remarks, REMARKS AS mp_remarks, LOGISTICS_PARTNER AS mp_logistics_partner, TRY_TO_NUMBER(QTY) AS mp_qty, TRY_TO_NUMBER(QTY_DISPATCHED) AS mp_qty_dispatched, TRY_TO_NUMBER(NF_UF_QTY) AS mp_nf_uf_qty, TRY_TO_NUMBER(BOX_COUNT) AS mp_box_count, TRY_TO_NUMBER(ACTUALWEIGHT) AS mp_actual_weight, NEW_SLOT AS mp_new_slot, TRY_TO_DATE(DATE, \'DD/MM/YYYY\') AS mp_date, TRY_TO_DATE(RTS_DATE, \'YYYY-MM-DD\') AS mp_rts_date, TRY_TO_DATE(SLOT_DATE, \'YYYY-MM-DD\') AS mp_slot_date, TRY_TO_DATE(PICKUP_DATE, \'YYYY-MM-DD\') AS mp_pickup_date, TRY_TO_DATE(DISPATCH_DATE, \'YYYY-MM-DD\') AS mp_dispatch_date, TRY_TO_DATE(DELIVERED_DATE, \'YYYY-MM-DD\') AS mp_delivered_date, PROCESSING_TAT AS mp_processing_tat, ROW_NUMBER() OVER ( PARTITION BY UPPER(TRIM(SO_NUMBER)) ORDER BY TRY_TO_DATE(DISPATCH_DATE, \'YYYY-MM-DD\') DESC NULLS LAST ) AS rn FROM snitch_db.maplemonk.gs_marketplace_wh_tracker WHERE SO_NUMBER IS NOT NULL AND TRIM(SO_NUMBER) <> \'\' ) SELECT uc.*, mp.mp_so_number, mp.mp_po_number, mp.mp_dc_number, mp.mp_lr_no, mp.mp_invoice_number, mp.mp_channel, mp.mp_marketplace_name, mp.mp_facility, mp.mp_city, mp.mp_address, mp.mp_status, mp.mp_delivery_status, mp.mp_current_remarks, mp.mp_remarks, mp.mp_logistics_partner, mp.mp_qty, mp.mp_qty_dispatched, mp.mp_nf_uf_qty, mp.mp_box_count, mp.mp_actual_weight, mp.mp_new_slot, mp.mp_date, mp.mp_rts_date, mp.mp_slot_date, mp.mp_pickup_date, mp.mp_dispatch_date, mp.mp_delivered_date, mp.mp_processing_tat, CASE WHEN mp.mp_so_key IS NOT NULL THEN 1 ELSE NULL END AS mp_tracker_match_flag FROM uc LEFT JOIN mp ON UPPER(TRIM(uc.order_name)) = mp.mp_so_key WHERE mp.rn = 1 OR mp.rn IS NULL ;",
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
            