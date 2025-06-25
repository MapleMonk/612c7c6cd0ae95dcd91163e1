{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.dod_all_channels_sku_inventory as WITH inv as ( select date, sku_group, sum(total_inventory)::INT as inv from snitch_db.maplemonk.cut_size_analysis where date >= \'2024-05-30\' group by 1,2 UNION ALL select date, SKU_GROUP, sum(inventory+jit_qty)::int AS INV from snitch_db.maplemonk.offline_master_daily_report_1 group by 1,2 ) SELECT DATE, SKU_GROUP, SUM(INV) as inv from inv group by 1, ;",
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
            