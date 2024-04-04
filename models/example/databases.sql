{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.unicommerce_availability_merge as SELECT fa.*, sgt.sleeve_type, sgt.collar_type, sgt.fabric, sgt.design, sgt.hem, sgt.closure, sgt.fit, sgt.occassion, sgt.product_type, sgt.color FROM snitch_db.maplemonk.unicommerce_fact_items_snitch fa LEFT JOIN snitch_db.maplemonk.sku_group_tags sgt ON fa.sku_group = sgt.sku_group",
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
                        