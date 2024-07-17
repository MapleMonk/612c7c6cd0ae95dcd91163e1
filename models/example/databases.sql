{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_sla_performance AS WITH unicom AS ( SELECT awb, MARKETPLACE, marketplace_mapped, shippingpackagecode, order_id, order_name, saleorderitemcode, sku, order_status, order_date, order_timestamp, SHIPPING_LAST_UPDATE_TIMESTAMP, shipping_courier, warehouse_name, shipping_status, CASE WHEN SALE_ORDER_ITEM_STATUS = \'PICKING_FOR_INVOICING\' THEN shippingpackagestatus ELSE SALE_ORDER_ITEM_STATUS END AS UC_STATUS, dispatched_timestamp AS \"UC_Dispatched\", dispatch_date, delivered_date, days_to_dispatch, days_in_shipment, dispatch_to_delivery_days, item_details, address_line_1, CASE WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 0 THEN \'D0\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 1 THEN \'D1\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 2 THEN \'D2\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 3 THEN \'D3\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 3 AND TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 5 THEN \'D>3\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 5 AND TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 7 THEN \'D>5\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 7 AND TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 10 THEN \'D>7\' WHEN TIMESTAMPDIFF(DAY, order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 10 THEN \'D>10\' ELSE NULL END AS Ageing, CASE WHEN upper(MARKETPLACE) = \'SHOPIFY\' AND (EXTRACT(HOUR FROM order_timestamp)) < 16 THEN DATE_TRUNC(\'day\', order_timestamp::timestamp) + INTERVAL \'18 hours\' WHEN upper(MARKETPLACE) = \'SHOPIFY\' AND (EXTRACT(HOUR FROM order_timestamp)) >= 16 THEN DATE_TRUNC(\'day\', DATEADD(DAY, 1, order_timestamp::timestamp)) + INTERVAL \'18 hours\' ELSE fulfillment_tat END AS TAT, category FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate ), forward AS ( SELECT \"Shipping Package Code\", created_timestamp, picking_timestamp, picked_timestamp, packed_timestamp, rts_timestamp, manifested_timestamp, dispatched_timestamp as dispatched_timestamp1, shipped_timestamp, delivered_timestamp, cancelled_timestamp, \"created&dispatch\", \"pick&dispatch\", \"pack&dispatch\", LATEST_STATUS, LAST_UPDATED FROM snitch_db.maplemonk.forward_timestamps ), clickpost AS ( SELECT awb, \"Order Date\" AS CP_orderplaced, \"Pickup Date\", \"Delivery Date\" AS CP_Delivery FROM snitch_db.maplemonk.snitch_clickpost_track_order_dashboard_report ), sr AS ( SELECT \"Order ID\", warehouse FROM snitch_db.maplemonk.GS_SAPL_SR ), wh AS ( SELECT \"Order ID\", warehouse FROM snitch_db.maplemonk.GS_SAPL_WH ), wh2 AS ( SELECT \"Order ID\", warehouse FROM snitch_db.maplemonk.GS_SAPL_WH2 ), emiza AS ( SELECT \"Order ID\", warehouse FROM snitch_db.maplemonk.GS_SAPL_EMIZA ), marketplace AS ( SELECT \"Order ID\", warehouse FROM snitch_db.maplemonk.GS_MARKET_PLACE_ERROR_ORDERS ), unicom_error AS ( SELECT u.*, CASE WHEN u.marketplace_mapped = \'SHOPIFY\' THEN CASE WHEN u.warehouse_name = \'SAPL-EMIZA\' AND EXISTS (SELECT 1 FROM emiza WHERE emiza.\"Order ID\" = u.order_name AND emiza.warehouse = \'SAPL-EMIZA\') THEN 1 WHEN u.warehouse_name = \'SAPL-WH\' AND EXISTS (SELECT 1 FROM wh WHERE wh.\"Order ID\" = u.order_name AND wh.warehouse = \'SAPL-WH\') THEN 1 WHEN u.warehouse_name = \'SAPL-SR\' AND EXISTS (SELECT 1 FROM sr WHERE sr.\"Order ID\" = u.order_name AND sr.warehouse = \'SAPL-SR\') THEN 1 WHEN u.warehouse_name = \'SAPL-WH2\' AND EXISTS (SELECT 1 FROM wh2 WHERE wh2.\"Order ID\" = u.order_name AND wh2.warehouse = \'SAPL-WH2\') THEN 1 ELSE NULL END WHEN u.marketplace_mapped NOT IN (\'SHOPIFY\', \'OWN_STORE\', \'FRANCHISE_STORE\') THEN CASE WHEN EXISTS (SELECT 1 FROM marketplace WHERE marketplace.\"Order ID\" = u.order_name AND marketplace.warehouse = u.warehouse_name) THEN 1 ELSE NULL END ELSE NULL END AS ERROR FROM unicom u ), main_data AS ( SELECT ue.*, f.created_timestamp, f.picking_timestamp, f.picked_timestamp, f.packed_timestamp, f.rts_timestamp, f.manifested_timestamp, f.dispatched_timestamp1, f.shipped_timestamp, f.delivered_timestamp, f.cancelled_timestamp, f.\"created&dispatch\", f.\"pick&dispatch\", f.\"pack&dispatch\", f.LATEST_STATUS, f.LAST_UPDATED, cp.CP_orderplaced, cp.\"Pickup Date\", cp.CP_Delivery FROM unicom_error ue LEFT JOIN forward f ON ue.shippingpackagecode = f.\"Shipping Package Code\" LEFT JOIN clickpost cp ON ue.awb = cp.awb ), new_status AS ( SELECT md.*, CASE WHEN UC_STATUS in (\'UNFULFILLABLE\',\'DISPATCHED\',\'DELIVERED\',\'SHIPPED\',\'RETURN_EXPECTED\',\'CANCELLED\',\'REPLACED\',\'RETURNED\') THEN UC_STATUS ELSE coalesce(LATEST_STATUS,UC_STATUS) END AS ITEM_STATUS, COALESCE(dispatched_timestamp1, \"UC_Dispatched\") AS dispatched_timestamp, CASE WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', GETDATE()) AND ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-COMPLETED\' WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', GETDATE()) AND ITEM_STATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-PENDING\' ELSE CASE WHEN ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN \"DISPATCHED_TIMESTAMP\" < TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END WHEN ITEM_STATUS = \'CANCELLED\' THEN CASE WHEN \"DISPATCHED_TIMESTAMP\" IS NULL THEN \'CANCELLED\' ELSE CASE WHEN \"DISPATCHED_TIMESTAMP\" < TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END WHEN ITEM_STATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\', \'PICKING_FOR_INVOICING\', \'FULFILLABLE\') THEN \'BREACHED - PENDING FOR PROCESS\' WHEN ITEM_STATUS IN (\'UNFULFILLABLE\') THEN \'UNFULFILLABLE\' ELSE \'BREACHED\' END END AS SLA_Status, CASE WHEN marketplace_mapped = \'MYNTRA\' THEN CASE WHEN lower(SLA_Status) = \'cancelled\' THEN \'CANCELLED\' WHEN lower(SLA_STATUS) NOT IN (\'cancelled\',\'within sla\', \'breached\',\'future date-completed\') THEN \'PENDING\' ELSE CASE WHEN category IN (\'Accessories\', \'Sunglass\', \'Shorts\', \'Pyjama\', \'Boxer\', \'Co-ords\', \'Jogsuit\', \'Underpants\') AND item_details IS NULL THEN \'CORRECT\' WHEN lower(item_details) LIKE \'%mp%\' THEN \'CORRECT\' ELSE \'INCORRECT\' END END ELSE \'OTHER CHANNEL\' END AS Myntra_Tag_Loops, FROM main_data md ) SELECT * FROM new_status WHERE (warehouse_name LIKE \'%SAPL%\' OR warehouse_name IS NULL) AND order_date::date >= DATEADD(DAY, -60, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP));",
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
                        