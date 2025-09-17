{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.distinct_sku_count as ( with total_inv as ( select date, upper(trim(sku_group)) as sku_group, \'online\' as type, sum(total_inventory)::INT as inv from snitch_db.maplemonk.cut_size_analysis group by 1,2,3 having sum(total_inventory)::INT >0 UNION ALL select date, upper(trim(SKU_GROUP)) as sku_group, \'offline\' as type, sum(inventory+jit_qty)::int AS INV from snitch_db.maplemonk.offline_master_daily_report_1 group by 1,2,3 having sum(inventory+jit_qty)::INT >0 union all select date, upper(trim(sku_group)) as sku_group, \'marketplace\' as type, sum(opening_inventory)::int as INV from ( select date, sku_group, case when opening_inventory < 0 then 0 else opening_inventory end as opening_inventory from snitch_db.maplemonk.slikk_inventory ) group by 1,2,3 having sum(opening_inventory)::int >0 union all select date, upper(trim(sku_group)) as sku_group, \'marketplace\' as type, sum(opening_inventory)::int as inv from ( select date, sku_group, case when opening_inventory < 0 then 0 else opening_inventory end as opening_inventory from snitch_db.maplemonk.knot_inventory_report ) group by 1,2,3 having sum(opening_inventory)::int >0 union all select date, upper(trim(sku_group)) as sku_group, \'marketplace\' as type, sum(opening_inventory)::int as inv from ( select date, sku_group, case when opening_inventory < 0 then 0 else opening_inventory end as opening_inventory from snitch_db.maplemonk.Mnow_inventory_report ) group by 1,2,3 having sum(opening_inventory)::int >0 union all select date, upper(trim(sku_group)) as sku_group, \'marketplace\' as type, sum(opening_inventory)::int as inv from ( select date, sku_group, case when opening_inventory < 0 then 0 else opening_inventory end as opening_inventory from snitch_db.maplemonk.Myntra_SJIT_INV_report ) group by 1,2,3 having sum(opening_inventory)::int >0 ), channel_wise_sku as ( select date, count(distinct case when type = \'online\' then sku_group end) as online_sku, count(distinct case when type = \'offline\' then sku_group end) as offline_sku, count(distinct case when type = \'marketplace\' then sku_group end) as marketplace_sku from total_inv where inv > 0 group by 1 ), dod_inv as ( select date, sku_group, sum(inv) as inv from total_inv group by 1,2 ), overall_sku as ( select date, count(distinct case when inv>0 then sku_group end) as total_sku_group from dod_inv group by 1 ) select a.*, b.total_sku_group from channel_wise_sku a left join overall_sku b on a.date = b.date );",
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
            