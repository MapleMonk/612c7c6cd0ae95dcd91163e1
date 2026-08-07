{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.mp_fk as with base as ( select sku_group, expected_delivery_date from snitch_db.maplemonk.FK_SMU ), PS_Status as ( select sku_group, sample_received_date, shoot_date from snitch_db.maplemonk.sku_group_sample_to_live ), RTS as ( select sku_group, warehouse, total_qty, inspection_date, planned_date, estimated_delivery_date from snitch_db.maplemonk.rts_tat ), Putaway as ( select skugroup, sum(putaway_completed_quantity) as total_putaway_qty, max(putaway_created) as putaway_created from snitch_db.maplemonk.putaway_tracking group by 1 ) select base.sku_group, expected_delivery_date, sample_received_date, shoot_date, warehouse, total_qty, inspection_date, planned_date, estimated_delivery_date, case when shoot_date is null then \'Not_Shot\' else \'Shot\' end as shoot_status, total_putaway_qty, putaway_created from base left join PS_Status on base.sku_group = PS_Status.sku_group left join RTS on base.sku_group = RTS.sku_group left join Putaway on base.sku_group = Putaway.skugroup order by base.sku_group",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from SNITCH_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            