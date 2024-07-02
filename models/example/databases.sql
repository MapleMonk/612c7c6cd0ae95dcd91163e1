{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_sla_performance AS SELECT uc.awb, uc.MARKETPLACE, uc.marketplace_mapped, uc.shippingpackagecode, uc.order_id, uc.order_name, uc.sku, uc.order_status, uc.order_date, uc.order_timestamp, uc.SHIPPING_LAST_UPDATE_TIMESTAMP, uc.shipping_courier, uc.warehouse_name, uc.shipping_status, CASE WHEN uc.SALE_ORDER_ITEM_STATUS = \'PICKING_FOR_INVOICING\' THEN uc.shippingpackagestatus ELSE uc.SALE_ORDER_ITEM_STATUS END AS Item_Status, sc.created_timestamp, sc.picking_timestamp, sc.picked_timestamp, sc.packed_timestamp, sc.manifested_timestamp, sc.dispatched_timestamp, uc.dispatched_timestamp as \"UC_Dispatched\", uc.dispatch_date, uc.delivered_date, cp.\"Order Date\" as \"CP_orderplaced\", cp.\"Pickup Date\" as \"CP_Pickup\", cp.\"Delivery Date\" as \"CP_Delivery\", sc.\"created&dispatch\", sc.\"pick&dispatch\", sc.\"pack&dispatch\", uc.days_to_dispatch, uc.days_in_shipment, uc.dispatch_to_delivery_days, sps.fulfillment_tat, uc.item_details, case WHEN upper(uc.MARKETPLACE) = \'SHOPIFY\' and (EXTRACT(HOUR FROM order_timestamp)) < 16 then DATE_TRUNC(\'day\', order_timestamp::timestamp) + INTERVAL \'18 hours\' WHEN upper(uc.MARKETPLACE) = \'SHOPIFY\' and (EXTRACT(HOUR FROM order_timestamp)) >= 16 then DATE_TRUNC(\'day\', DATEADD(DAY, 1, order_timestamp)::timestamp) + INTERVAL \'18 hours\' else sps.fulfillment_tat end AS TAT, CASE WHEN ITEM_STATUS = \'UNFULFILLABLE\' THEN \'UNFULFILLABLE\' ELSE CASE WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\',\'Asia/Kolkata\',getdate()) AND ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\',\'RETURNED\') THEN \'FUTURE DATE-COMPLETED\' WHEN TAT > CONVERT_TIMEZONE(\'Canada/Pacific\',\'Asia/Kolkata\',getdate()) AND ITEM_STATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\',\'RETURNED\') THEN \'FUTURE DATE-PENDING\' ELSE CASE WHEN ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN \"UC_Dispatched\" < TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END WHEN ITEM_STATUS = \'CANCELLED\' THEN CASE WHEN \"UC_Dispatched\" IS NULL THEN \'CANCELLED\' ELSE CASE WHEN \"UC_Dispatched\" < TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END WHEN ITEM_STATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\',\'PICKING_FOR_INVOICING\',\'FULFILLABLE\',\'UNFULFILLABLE\') THEN \'BREACHED - PENDING FOR PROCESS\' ELSE \'BREACHED\' END END END AS SLA_Status, CASE WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 0 THEN \'D0\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 1 THEN \'D1\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 2 THEN \'D2\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) = 3 THEN \'D3\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 3 AND TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 5 THEN \'D>3\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 5 AND TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 7 THEN \'D>5\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 7 AND TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) <= 10 THEN \'D>7\' WHEN TIMESTAMPDIFF(DAY, uc.order_date, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) > 10 THEN \'D>10\' ELSE null END AS Ageing, uc.category, CASE WHEN marketplace_mapped = \'MYNTRA\' THEN CASE WHEN lower(SLA_Status) = \'cancelled\' THEN \'CANCELLED\' WHEN lower(SLA_STATUS) NOT IN (\'cancelled\',\'within sla\', \'breached\',\'future date-completed\') THEN \'PENDING\' ELSE CASE WHEN uc.category IN (\'Accessories\', \'Sunglass\', \'Shorts\', \'Pyjama\', \'Boxer\', \'Co-ords\', \'Jogsuit\', \'Underpants\') AND uc.item_details IS NULL THEN \'CORRECT\' WHEN lower(uc.item_details) LIKE \'%mp%\' THEN \'CORRECT\' ELSE \'INCORRECT\' END END ELSE \'OTHER CHANNEL\' END AS Myntra_Tag_Loops, CASE WHEN uc.order_name = sr.\"Order ID\" AND uc.warehouse_name = sr.Warehouse THEN 1 WHEN uc.order_name = wh.\"Order ID\" AND uc.warehouse_name = wh.Warehouse THEN 1 WHEN uc.order_name = wh2.\"Order ID\" AND uc.warehouse_name = wh2.Warehouse THEN 1 WHEN uc.order_name = em.\"Order ID\" AND uc.warehouse_name = em.Warehouse THEN 1 WHEN uc.order_name = mp.\"Order ID\" AND uc.warehouse_name = mp.Warehouse THEN 1 ELSE NULL END AS Error FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate uc LEFT JOIN ( Select ORDER_ID,FULFILLMENT_TAT from ( SELECT REPLACE(saleorderdto:code,\'\"\',\'\') AS order_id, CONVERT_TIMEZONE(\'UTC\',\'Asia/Kolkata\',dateadd(\'ms\',saleorderdto:fulfillmentTat,\'1970-01-01\')) AS fulfillment_tat, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY 1) RN FROM snitch_db.maplemonk.SNITCH_UNICOMMERCE_GET_ORDERS_BY_IDS_TEST, LATERAL FLATTEN (INPUT => saleorderdto:shippingPackages) sps ) where RN =1) sps ON uc.order_id = sps.order_id LEFT JOIN snitch_db.maplemonk.forward_timestamps sc ON uc.shippingpackagecode = sc.\"Shipping Package Code\" left join snitch_db.maplemonk.snitch_clickpost_track_order_dashboard_report cp on uc.awb = cp.\"AWB\" LEFT JOIN snitch_db.maplemonk.GS_SAPL_SR sr ON uc.order_name = sr.\"Order ID\" LEFT JOIN snitch_db.maplemonk.GS_SAPL_WH wh ON uc.order_name = wh.\"Order ID\" LEFT JOIN snitch_db.maplemonk.GS_SAPL_WH2 wh2 ON uc.order_name = wh2.\"Order ID\" LEFT JOIN snitch_db.maplemonk.GS_SAPL_EMIZA em ON uc.order_name = em.\"Order ID\" LEFT JOIN snitch_db.maplemonk.GS_MARKET_PLACE_ERROR_ORDERS mp ON uc.order_name = mp.\"Order ID\" WHERE uc.ORDER_DATE >= DATEADD(DAY, -30, CONVERT_TIMEZONE(\'Canada/Pacific\', \'Asia/Kolkata\', CURRENT_TIMESTAMP)) AND (uc.warehouse_name like \'%SAPL%\' OR uc.warehouse_name is null)",
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
                        