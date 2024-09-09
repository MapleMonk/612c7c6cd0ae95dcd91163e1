{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.unicommerce_availability_merge as with metafields as ( select designs, collar_new, material_new, occassion_new, print_design, sleeve_type, fit, style, color, new_category, sku_group, from snitch_db.maplemonk.product_info where date_wise = \'2024-09-09\' order by sku_group ) SELECT fa.*, metafields.designs, metafields.collar_new, metafields.material_new, metafields.occassion_new, metafields.print_design, metafields.sleeve_type, metafields.fit, metafields.style, metafields.new_category, metafields.color FROM snitch_db.maplemonk.unicommerce_fact_items_snitch fa LEFT JOIN metafields ON fa.sku_group = metafields.sku_group",
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
            