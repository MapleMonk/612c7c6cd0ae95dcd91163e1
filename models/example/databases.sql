{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.sku_count_channel as with online_inv as ( select date, trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))))) AS sku_group, sum(inventory) as inventory from snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE facility in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL_EMIZA\',\'SAPL-NORTH-TAURU\') GROUP BY 1,2 ), offline_inv as ( select date, upper(trim(SKU_GROUP)) as sku_group, sum(inventory+jit_qty)::int AS INV from snitch_db.maplemonk.offline_master_daily_report_1 group by 1,2 ), main_data as ( select a.date, coalesce(a.sku_group,b.sku_group) as sku_group, ifnull(inventory,0) as online_inv, ifnull(inv,0) as offline_inv from online_inv a full outer join offline_inv b on a.date = b.date and a.sku_group = b.sku_group where a.date >= \'2025-01-01\' ) select date, count(distinct case when online_inv > 0 then sku_group end) as online_sku, count(distinct case when offline_inv > 0 then sku_group end) as offline_sku, count(distinct case when online_inv > 0 and offline_inv > 0 then sku_group end) common_sku, count(distinct case when online_inv > 0 or offline_inv > 0 then sku_group end) as ecosystem_sku, count(distinct case when online_inv > 0 and offline_inv = 0 then sku_group end) as only_online_sku, count(distinct case when online_inv = 0 and offline_inv > 0 then sku_group end) as only_offline_sku from main_data group by 1",
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
            