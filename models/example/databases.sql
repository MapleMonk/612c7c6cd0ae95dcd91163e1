{{ config(
            materialized='table',
                post_hook={
                    "sql": "Create or replace table prolicious-wh.maplemonk.prolicious_parent_child_sku_mapping_fact_items as SELECT PARENT_SKU as Parent_SKU, TRIM(SPLIT(component, \'&\')[OFFSET(0)]) AS Child_sku, CAST(TRIM(SPLIT(component, \'&\')[OFFSET(1)]) AS INT64) AS Child_quantity FROM maplemonk.Prolicious_SKU_MAPPING_PARENT_CHILD, UNNEST([ Component1_SKU, Component2_SKU, Component3_SKU, Component4_SKU, Component5_SKU, Component6_SKU, Component7_SKU, Component8_SKU, Component9_SKU, Component10_SKU, Component11_SKU ]) AS component WHERE component IS NOT NULL ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            