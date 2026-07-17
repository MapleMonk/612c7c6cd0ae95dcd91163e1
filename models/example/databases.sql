{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fresh_allocation_tagged AS SELECT a.order_date, a.branch_code, a.sku, a.sku_group, a.size_mapped, a.actual_qty, ms.category, ms.style, ms.meta1, ms.meta2, ms.meta3 FROM snitch_db.maplemonk.fresh_actual_allocation a LEFT JOIN snitch_db.maplemonk.metafields_std ms ON UPPER(a.sku_group) = UPPER(ms.sku_group);",
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
            