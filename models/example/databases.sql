{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.inventory_delta as ( with base as ( select trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item SkuCode\"), CHARINDEX(\'-\', REVERSE(\"Item SkuCode\")) + 1, LEN(\"Item SkuCode\"))))) AS sku_group, ifnull(sum(case when date = current_date then inventory end),0) as current_date_inv, ifnull(sum(case when date = current_date-1 then inventory end),0) as last_date_inv, ifnull(sum(case when date = current_date then inventory end),0) - ifnull(sum(case when date = current_date-1 then inventory end),0) as inv_delta from snitch_db.maplemonk.snitch_final_inventory_wh2 WHERE facility in (\'SAPL-WH2\',\'SAPL-WH1\',\'SAPL-NORTH-TAURU\') group by 1 ) select base.*, p.current_slashed_price, p.price from base left join snitch_db.maplemonk.availability_master_v2 as p on base.sku_group=p.sku_group )",
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
            