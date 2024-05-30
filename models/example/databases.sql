{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.forward_timestamps AS SELECT \"Sale Order Code\", \"Display Order Code\", \"Shipping Package Code\", MAX(CASE WHEN \"Old Value\" = \'NEW\' AND \"New Value\" = \'CREATED\' THEN to_date(\"Time Stamp\")::date END) AS created_timestamp, MAX(CASE WHEN \"Old Value\" = \'CREATED\' AND \"New Value\" = \'PICKING\' THEN to_date(\"Time Stamp\")::date END) AS picking_timestamp, MAX(CASE WHEN \"Old Value\" = \'PICKING\' AND \"New Value\" = \'PICKED\' THEN to_date(\"Time Stamp\")::date END) AS picked_timestamp, MAX(CASE WHEN \"Old Value\" = \'PICKED\' AND \"New Value\" = \'PACKED\' THEN to_date(\"Time Stamp\")::date END) AS packed_timestamp, MAX(CASE WHEN \"Old Value\" = \'PACKED\' AND \"New Value\" = \'READY_TO_SHIP\' THEN to_date(\"Time Stamp\")::date END) AS rts_timestamp, MAX(CASE WHEN \"Old Value\" = \'READY_TO_SHIP\' AND \"New Value\" = \'MANIFESTED\' THEN to_date(\"Time Stamp\")::date END) AS manifested_timestamp, MAX(CASE WHEN (\"Old Value\" = \'MANIFESTED\' OR \"Old Value\" = \'READY_TO_SHIP\') AND \"New Value\" = \'DISPATCHED\' THEN to_date(\"Time Stamp\")::date END) AS dispatched_timestamp, MAX(CASE WHEN \"Old Value\" = \'DISPATCHED\' AND \"New Value\" = \'SHIPPED\' THEN to_date(\"Time Stamp\")::date END) AS shipped_timestamp, MAX(CASE WHEN (\"Old Value\" = \'SHIPPED\' OR \"Old Value\" = \'DISPATCHED\') AND \"New Value\" = \'DELIVERED\' THEN to_date(\"Time Stamp\")::date END) AS delivered_timestamp, \"No. of Items\", CASE WHEN DATE(\"CREATED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") THEN 1 ELSE 0 END AS \"created&dispatch\", CASE WHEN DATE(\"PICKED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") THEN 1 ELSE 0 END AS \"pick&dispatch\", CASE WHEN (DATE(\"PACKED_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\") ) OR (DATE(\"RTS_TIMESTAMP\") = DATE(\"DISPATCHED_TIMESTAMP\")) THEN 1 ELSE 0 END AS \"pack&dispatch\" FROM snitch_db.maplemonk.snitch_get_shipping_package_timeline GROUP BY \"Sale Order Code\", \"Display Order Code\", \"Shipping Package Code\", \"No. of Items\"; CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_sla_performance AS SELECT uc.awb, uc.MARKETPLACE, uc.marketplace_mapped, uc.shippingpackagecode, uc.order_id, uc.order_name, uc.sku, uc.order_status, uc.order_date, uc.order_timestamp, uc.SHIPPING_LAST_UPDATE_TIMESTAMP, uc.shippingpackagestatus, uc.shipping_courier, uc.warehouse_name, uc.shipping_status, sc.created_timestamp, sc.picking_timestamp, sc.picked_timestamp, sc.packed_timestamp, sc.manifested_timestamp, sc.dispatched_timestamp, uc.dispatched_timestamp as \"UC_Dispatched\", uc.dispatch_date, uc.delivered_date, cp.orderplaced_date as \"CP_orderplaced\", cp.pickup_date as \"CP_Pickup\", cp.delivery_date as \"CP_Delivery\", sc.\"created&dispatch\", sc.\"pick&dispatch\", sc.\"pack&dispatch\", uc.days_to_dispatch, uc.days_in_shipment, uc.dispatch_to_delivery_days, sps.fulfillment_tat, case WHEN upper(uc.MARKETPLACE) = \'SHOPIFY\' and (EXTRACT(HOUR FROM order_timestamp)) < 16 then DATE_TRUNC(\'day\', order_timestamp::timestamp) + INTERVAL \'18 hours\' WHEN upper(uc.MARKETPLACE) = \'SHOPIFY\' and (EXTRACT(HOUR FROM order_timestamp)) >= 16 then DATE_TRUNC(\'day\', DATEADD(DAY, 1, order_timestamp)::timestamp) + INTERVAL \'18 hours\' else sps.fulfillment_tat end AS TAT, CASE WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\',\'Asia/Kolkata\',getdate()) AND uc.SHIPPINGPACKAGESTATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\') THEN \'Future Date-Completed\' WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\',\'Asia/Kolkata\',getdate()) AND uc.SHIPPINGPACKAGESTATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\') THEN \'Future Date-Pending\' ELSE CASE WHEN uc.order_status = \'CANCELLED\' THEN \'Cancelled\' WHEN uc.shippingpackagestatus IS NULL AND ORDER_STATUS = \'PROCESSING\' THEN \'Unfulfillable\' ELSE CASE WHEN uc.SHIPPINGPACKAGESTATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN uc.dispatched_timestamp < TAT THEN \'WITHIN SLA\' ELSE \'Breached\' END WHEN uc.SHIPPINGPACKAGESTATUS = \'CANCELLED\' THEN CASE WHEN uc.dispatched_timestamp IS NULL THEN \'Cancelled\' ELSE CASE WHEN uc.dispatched_timestamp < TAT THEN \'WITHIN SLA\' ELSE \'Breached\' END END WHEN uc.SHIPPINGPACKAGESTATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\') THEN \'Breached - Pending for Process\' ELSE \'Breached\' END END END AS SLA_Status, CASE WHEN SLA_Status IN (\'Breached - Pending for Process\',\'Unfulfillable\') THEN TIMESTAMPDIFF(DAY, uc.order_date, CURRENT_TIMESTAMP) ELSE NULL END AS Ageing FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate uc LEFT JOIN ( Select ORDER_ID,FULFILLMENT_TAT from ( SELECT REPLACE(saleorderdto:code,\'\"\',\'\') AS order_id, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:fulfillmentTat,\'1970-01-01\')) AS fulfillment_tat, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY 1) RN FROM snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages) sps ) where RN =1) sps ON uc.order_id = sps.order_id LEFT JOIN snitch_db.maplemonk.forward_timestamps sc ON uc.shippingpackagecode = sc.\"Shipping Package Code\" left join snitch_db.snitch.sleepycat_db_clickpost_fact_items cp on uc.awb = cp.awb_number where warehouse in (\'SAPL-WH\',\'SAPL-WH2\',\'SAPL-EMIZA\',\'SAPL-SR\')",
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
                        