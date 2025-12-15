{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.logistics_convertor as ( select date, remark,no_boxes,dc_number,lr_number,so_number,d_quantity,store_name from snitch_db.maplemonk.gs_outstation_dispatch , UNION ALL select date, remark, no_boxes,dc_number,lr_number,so_number,d_quantity,store_name from snitch_db.maplemonk.gs2_logistics_tauru ) ; create or replace table snitch_db.maplemonk.logistics_convertor as SELECT trim(f.value) as ind_so, t.\"DATE\", t.remark, t.no_boxes, t.dc_number, t.lr_number, t.d_quantity, t.store_name, t.so_number FROM snitch_db.maplemonk.logistics_convertor t, LATERAL FLATTEN(input => SPLIT( REGEXP_REPLACE(t.so_number, \'\\r\\n|\\n\', \',\'), \',\' )) f ; select * FROM snitch_db.maplemonk.logistics_convertor where so_number like \'%MAGNET6860%\' ; select distinct \"Section Code\" from snitch_db.maplemonk.uc_new_get_shelf_report where \"Warehouse Name\" like \'%TAURU%\'",
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
            