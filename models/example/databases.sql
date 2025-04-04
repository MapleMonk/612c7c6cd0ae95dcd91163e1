{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table kerala-ayurveda-wh.maplemonk.packiyo_inventory_fact_items as select * from ( select JSON_VALUE(attributes, \'$.sku\') AS sku, JSON_VALUE(attributes, \'$.name\') AS name, JSON_VALUE(attributes, \'$.created_at\') AS created_at, JSON_VALUE(attributes, \'$.updated_at\') AS updated_at, JSON_VALUE(attributes, \'$.quantity_available\') AS quantity_available, JSON_VALUE(attributes, \'$.quantity_on_hand\') AS quantity_on_hand, JSON_VALUE(attributes, \'$.quantity_reorder\') AS quantity_reorder, JSON_VALUE(attributes, \'$.quantity_backordered\') AS quantity_backordered, JSON_VALUE(attributes, \'$.quantity_reserved\') AS quantity_reserved, JSON_VALUE(attributes, \'$.quantity_inbound\') AS quantity_inbound, cast(_airbyte_emitted_at as date) inventory_snapshot_date, row_number() over (partition by JSON_VALUE(attributes, \'$.sku\'), cast(_airbyte_emitted_at as date) order by cast(_airbyte_emitted_at as date) desc ) rw from kerala-ayurveda-wh.maplemonk.packiyo_get_inventory ) where rw = 1 ;",
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
            