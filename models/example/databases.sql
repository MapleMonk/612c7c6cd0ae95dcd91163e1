{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.gs_wh_master_data as select \"From Store\" as from_store, TRY_TO_DATE(\"Putaway Date\",\'YYYY-MM-DD\') as putaway_date, \"STORE REMARKS\" as store_remarks, \"Inward remarks\" as inward_remarks, \"Putaway number\" as putaway_number, \"Ageing(in Days)\" as ageing, \"COUNTING AGEING\" as count_ageing, \"Logic difference\"::integer as logic_diff, TRY_TO_DATE(\"PO Requested date\", \'YYYY-MM-DD\') as pt_requested_date, \"Sla breach status\" as sla_breach_status, \"Sla breach remarks\" as sla_breach_remarks, \"processing helper\" as processing_helper, \"Without Barcode qty\"::integer as qty_without_barcode, \"Washing pcs quantity\"::integer as qty_washing, \"Current status remarks\" as current_status, \"Final qty after discrepancy\"::integer as final_qty, \"Shortages / Excess Quantity\" as shortage_excess, \"Bad Inventory quantity - Damaged\"::integer as bad_inv, from snitch_db.maplemonk.gs_wh_master_data ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.gs_ratio_data as select \"BOX NO\" AS box_no, \"DONE BY\" as done_by, \"FINAL SKU\" as final_sku, \"Qc status\" as qc_status, \"SCANNED QTY\" ::integer as scanned_qty, \"Vendor type\" AS vendor_type, \"vendor name\" as vendor_name, \"VARIANCE QTY\"::integer as variance_qty, try_to_date(\"Received date\", \'YYYY-MM-DD\') as received_date, \"erp po number\" as erp_po_number, \"invoice number\" as invoice_number, try_to_date(\"Ratio done date\", \'YYYY-MM-DD\') as ratio_done_date, \"VARIANCE STATUS\" as variance_status, \"PACKING LIST QTY\"::integer as packing_list_qty from snitch_db.maplemonk.gs_ratio_data ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.gs_putaway_done as select \"Done by\" as done_by, \"Done on\" as done_on, \"Pending QTy\"::integer as pending_qty, PUTAWAY_QTY::integer as putaway_qty, \"Completed qty\"::integer as completed_qty, \"Putaway created by\" as putaway_created_by, from snitch_db.maplemonk.gs_putaway_done ;",
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
            