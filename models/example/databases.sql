{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.inwards_l1_mapped as with inwards_data as ( select trim(upper(REVERSE(SUBSTRING(REVERSE(\"Item Type skuCode\"), CHARINDEX(\'-\', REVERSE(\"Item Type skuCode\")) + 1, LEN(\"Item Type skuCode\"))))) as sku_group, putaway_updated::date as inward_date, SUM(PUTAWAY_COMPLETED_QUANTITY::int)::int as inward_quant from snitch_db.maplemonk.putaway_tracking where lower(FINAL_TYPE) LIKE \'%inward%\' and putaway_updated::date = current_date -1 group by 1,2 ), metafields_data as ( select sku_group, case when lower(sku_group) like \'4mbg%\' then \'plus_size\' when lower(style) like \'%luxe%\' then \'luxe\' when lower(style) like \'%core%\' then \'core\' when lower(category) in (\'perfumes\',\'accessories\',\'bags\',\'belts\',\'sunglasses\',\'shoes\',\'slip-ons\') then \'long_tail\' else \'Snitch\' end as l1_category, mapping, manual_mapping, mapping_final from snitch_db.maplemonk.metafields_data ) select a.*, l1_category, mapping, manual_mapping, mapping_final from inwards_data a left join metafields_data b on a.sku_group = b.sku_group",
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
            