{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE maplemonk.P_TAL_DB_SHIPSTATION_FACT_ITEMS AS WITH shipment_base AS ( SELECT external_order_id AS order_id, shipment_number AS awb_number, service_code AS courier_partner, shipment_status AS current_status, datetime(CAST(created_at AS TIMESTAMP)) AS ordered_date, datetime(CAST(ship_date AS TIMESTAMP)) AS dispatch_date, CAST(ship_by_date AS TIMESTAMP) AS min_sla, CAST(deliver_by_date AS TIMESTAMP) AS max_sla, CONCAT( COALESCE(JSON_EXTRACT_SCALAR(ship_to, \'$.city_locality\'), \'\'), \', \', COALESCE(JSON_EXTRACT_SCALAR(ship_to, \'$.state_province\'), \'\'), \', \', COALESCE(JSON_EXTRACT_SCALAR(ship_to, \'$.country_code\'), \'\') ) AS order_location, CONCAT( COALESCE(JSON_EXTRACT_SCALAR(ship_from, \'$.city_locality\'), \'\'), \', \', COALESCE(JSON_EXTRACT_SCALAR(ship_from, \'$.state_province\'), \'\'), \', \', COALESCE(JSON_EXTRACT_SCALAR(ship_from, \'$.country_code\'), \'\') ) AS pickup_location, items FROM `MapleMonk.Shipstation_P_TaL_get_shipments` QUALIFY ROW_NUMBER() OVER (PARTITION BY shipment_number ORDER BY CAST(modified_at AS TIMESTAMP) DESC) = 1 ) SELECT s.order_id, JSON_EXTRACT_SCALAR(item, \'$.external_order_item_id\') AS external_order_item_id, s.awb_number, s.courier_partner, s.current_status, s.ordered_date, s.dispatch_date, s.min_sla, s.max_sla, s.order_location, s.pickup_location, JSON_EXTRACT_SCALAR(item, \'$.sku\') AS item_sku, JSON_EXTRACT_SCALAR(item, \'$.name\') AS item_name, CAST(JSON_EXTRACT_SCALAR(item, \'$.quantity\') AS INT64) AS item_quantity, CAST(JSON_EXTRACT_SCALAR(item, \'$.unit_price\') AS FLOAT64) AS item_unit_price, CAST(JSON_EXTRACT_SCALAR(item, \'$.weight.value\') AS FLOAT64) AS item_weight_oz, JSON_EXTRACT_SCALAR(item, \'$.product_id\') AS product_id FROM shipment_base s CROSS JOIN UNNEST(s.items) AS item",
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
            