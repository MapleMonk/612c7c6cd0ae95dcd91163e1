{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.dod_sku_inventory as WITH inv as ( select DATE, trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))))) AS sku_group, sum(inventory) as INV from snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE facility in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL_EMIZA\',\'SAPL-NORTH-TAURU\') GROUP BY 1,2 ) select a.date, b.category, sum(a.inv) as inventory from inv a left join base_product b on a.sku_group = b.sku_group where a.date >= \'2025-05-01\' and category is not null group by 1,2",
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
            