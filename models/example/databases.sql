{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.gs_ratio_data as select \"BOX NO\" AS box_no, \"DONE BY\" as done_by, \"FINAL SKU\" as final_sku, \"SKU2\" as SKU2, \"Qc status\" as qc_status, \"SCANNED QTY\" ::integer as scanned_qty, \"Vendor type\" AS vendor_type, \"vendor name\" as vendor_name, \"VARIANCE QTY\"::integer as variance_qty, try_to_date(\"Received date\", \'YYYY-MM-DD\') as received_date, \"erp po number\" as erp_po_number, \"invoice number\" as invoice_number, try_to_date(\"Ratio done date\", \'YYYY-MM-DD\') as ratio_done_date, \"VARIANCE STATUS\" as variance_status, \"PACKING LIST QTY\"::integer as packing_list_qty, \"SKU\" as sku, \"SIZE\" AS SIZE, \"COLOR\" AS COLOR, \"REMARKS\" AS REMARKS, \"EAN_NUMBER\" AS EAN_NUMBER from snitch_db.maplemonk.gs1_ratio_data ; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.gs_putaway_done as select \"Done by\" as done_by, \"Done on\" as done_on, \"Pending QTy\"::integer as pending_qty, PUTAWAY_QTY::integer as putaway_qty, \"Completed qty\"::integer as completed_qty, \"Putaway created by\" as putaway_created_by from snitch_db.maplemonk.gs1_putaway_done ;",
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
            