{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE `kerala-ayurveda-wh.maplemonk.packiyo_inventory_fact_items` AS SELECT sku, name, created_at, updated_at, quantity_available, quantity_on_hand, quantity_reorder, quantity_backordered, quantity_reserved, quantity_inbound, inventory_snapshot_date FROM ( SELECT JSON_VALUE(attributes, \'$.sku\') AS sku, JSON_VALUE(attributes, \'$.name\') AS name, JSON_VALUE(attributes, \'$.created_at\') AS created_at, JSON_VALUE(attributes, \'$.updated_at\') AS updated_at, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_available\') AS INT64) AS quantity_available, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_on_hand\') AS INT64) AS quantity_on_hand, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_reorder\') AS INT64) AS quantity_reorder, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_backordered\') AS INT64) AS quantity_backordered, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_reserved\') AS INT64) AS quantity_reserved, SAFE_CAST(JSON_VALUE(attributes, \'$.quantity_inbound\') AS INT64) AS quantity_inbound, CAST(_airbyte_emitted_at AS DATE) AS inventory_snapshot_date, ROW_NUMBER() OVER ( PARTITION BY JSON_VALUE(attributes, \'$.sku\'), CAST(_airbyte_emitted_at AS DATE) ORDER BY CAST(_airbyte_emitted_at AS DATE) DESC ) AS rw FROM `kerala-ayurveda-wh.maplemonk.packiyo_get_inventory` ) WHERE rw = 1;",
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
            