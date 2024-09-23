{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.logistics_convertor as SELECT TRIM(f.value) AS ind_so, t.\"DATE\", t.remark, t.no_boxes, t.dc_number, t.lr_number, t.adj_logic, t.d_quantity, t.excess_qty, t.store_name, t.inward_date, t.qty_received, t.shortage_qty, t.logic_ref_num, t.store_remarks, t.boxes_received, t.order_received_date, t.so_number FROM snitch_db.maplemonk.gs_outstation_dispatch t, LATERAL FLATTEN(input => SPLIT(t.so_number, \'\n\')) f;",
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
            