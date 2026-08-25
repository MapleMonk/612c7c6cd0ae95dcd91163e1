{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.mp_fk as with base as ( select sku_group, revised_delivered_date, qty, warehouse from SNITCH_DB.MAPLEMONK.FK_MP_10_08_2026 ), PS_Status as ( select sku_group, min(sample_received_date) as sample_received_date, min(shoot_date) as shoot_date from snitch_db.maplemonk.sku_group_sample_to_live group by 1 ), RTS as ( select sku_group, warehouse, total_qty, planned_date, estimated_delivery_date, max(inspection_date) as inspection_date from snitch_db.maplemonk.rts_tat where not ( sku_group in (\'FK-4PL0002-01\', \'FK-4PL0002-02\', \'FK-4PL0002-03\', \'FK-4PL0002-04\') and warehouse = \'SOUTH\' ) group by sku_group, warehouse, total_qty, planned_date, estimated_delivery_date ), Putaway as ( select regexp_replace(\"Item Type skuCode\", \'-[^-]+$\', \'\') as skugroup, sum(putaway_completed_quantity) as total_putaway_qty, max(putaway_created) as putaway_created, \"Warehouse Name\" as warehouse from snitch_db.maplemonk.putaway_tracking group by 1,4 ) select base.sku_group, revised_delivered_date as expected_delivery_date, sample_received_date, shoot_date, base.warehouse as source_warehouse, rts.warehouse, putaway.warehouse as destination_warehouse, total_qty, inspection_date, planned_date, estimated_delivery_date, base.qty as ordered_qty, case when shoot_date is null then \'Not_Shot\' else \'Shot\' end as shoot_status, total_putaway_qty, putaway_created from base left join PS_Status on base.sku_group = PS_Status.sku_group left join RTS on base.sku_group = RTS.sku_group left join Putaway on base.sku_group = Putaway.skugroup order by base.sku_group",
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
            