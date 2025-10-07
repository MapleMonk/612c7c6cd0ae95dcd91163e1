{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE Maplemonk.wondercare_AMAZON_IN_INVENTARY_FACT_ITEMS AS SELECT CAST(sku AS STRING) AS sku, CAST(asin AS STRING) AS asin, CAST(fnsku AS STRING) AS fnsku, CAST(condition AS STRING) AS condition, CAST(your_price AS FLOAT64) AS your_price, CAST(dataEndTime AS TIMESTAMP) AS data_end_time, CAST(product_name AS STRING) AS product_name, SAFE_CAST(per_unit_volume AS FLOAT64) AS per_unit_volume, CAST(afn_listing_exists AS STRING) AS afn_listing_exists, CAST(afn_total_quantity AS INT64) AS afn_total_quantity, CAST(mfn_listing_exists AS STRING) AS mfn_listing_exists, CAST(afn_reserved_quantity AS INT64) AS afn_reserved_quantity, CAST(afn_warehouse_quantity AS INT64) AS afn_warehouse_quantity, CAST(afn_unsellable_quantity AS INT64) AS afn_unsellable_quantity, CAST(afn_fulfillable_quantity AS INT64) AS afn_fulfillable_quantity, CAST(afn_researching_quantity AS INT64) AS afn_researching_quantity, SAFE_CAST(mfn_fulfillable_quantity AS INT64) AS mfn_fulfillable_quantity, CAST(afn_future_supply_buyable AS INT64) AS afn_future_supply_buyable, CAST(afn_reserved_future_supply AS INT64) AS afn_reserved_future_supply, CAST(afn_inbound_shipped_quantity AS INT64) AS afn_inbound_shipped_quantity, CAST(afn_inbound_working_quantity AS INT64) AS afn_inbound_working_quantity, CAST(afn_inbound_receiving_quantity AS INT64) AS afn_inbound_receiving_quantity FROM Maplemonk.Wondercare_Amazon_IN_GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA;",
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
            