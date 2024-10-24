{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.floor_pendency_fresh_inwards_blr AS WITH data AS ( SELECT serial, sku_no, repeat_style, new_style, colour_count, cut_quantity, factory, expected_delivery_date, del_location, top_priority, delivered, delivery_date, invoice, sku_name, dc_qty, ratio_date, po_req_date, po_received_date, po_code, grn_date, putaway_date, remarks FROM snitch_db.maplemonk.gs_floor_pendency LIMIT 10 ), out_data AS ( SELECT serial, sku_no, repeat_style, new_style, colour_count, cut_quantity, factory, expected_delivery_date, del_location, top_priority, delivered, delivery_date, invoice, sku_name, dc_qty, ratio_date, po_req_date, po_received_date, po_code, grn_date, putaway_date, remarks FROM snitch_db.maplemonk.gs_floor_out ) SELECT * FROM data UNION SELECT * FROM out_data;",
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
            