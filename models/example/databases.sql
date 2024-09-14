{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_b2b_performance AS WITH unicom AS ( SELECT awb, MARKETPLACE, marketplace_mapped, shippingpackagecode, order_id, order_name, saleorderitemcode, sku, order_status, order_date, order_timestamp, SHIPPING_LAST_UPDATE_TIMESTAMP, shipping_courier, warehouse_name, shipping_status, CASE WHEN SALE_ORDER_ITEM_STATUS = \'PICKING_FOR_INVOICING\' THEN shippingpackagestatus ELSE SALE_ORDER_ITEM_STATUS END AS UC_STATUS, dispatched_timestamp AS UC_Dispatched, dispatch_date, delivered_date, days_to_dispatch, days_in_shipment, dispatch_to_delivery_days, item_details, address_line_1, uc_created, CASE WHEN UPPER(MARKETPLACE) = \'SHOPIFY\' AND EXTRACT(HOUR FROM uc_created) < 16 THEN DATE_TRUNC(\'day\', uc_created::timestamp) + INTERVAL \'18 hours\' WHEN UPPER(MARKETPLACE) = \'SHOPIFY\' AND EXTRACT(HOUR FROM uc_created) >= 16 THEN DATE_TRUNC(\'day\', DATEADD(day, 1, uc_created::timestamp)) + INTERVAL \'18 hours\' ELSE fulfillment_tat END AS TAT, category FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate ), forward AS ( SELECT \"Shipping Package Code\", created_timestamp, picking_timestamp, picked_timestamp, packed_timestamp, rts_timestamp, manifested_timestamp, dispatched_timestamp AS dispatched_timestamp1, shipped_timestamp, delivered_timestamp, cancelled_timestamp, \"created&dispatch\", \"pick&dispatch\", \"pack&dispatch\", LATEST_STATUS, LAST_UPDATED FROM snitch_db.maplemonk.forward_timestamps ), inward AS ( SELECT \"DATE\", TYPE, ORDER_NAME AS orderid, ORDERED_ITEMS, UNFULFILLABLE, \"Fulfilled Quantity\", STATE, STORE, BOXES, STATUS, REMARKS, FACILITY, PRIORITY, EWAYBILL, DISPATCH_TO_STORE, DISPATCH_TO_YLK, LOGISTICS_PARTNER, TAT AS manual_tat FROM snitch_db.maplemonk.gs_b2b_uc ), main_data AS ( SELECT ue.*, f.*, iw.* FROM unicom ue LEFT JOIN forward f ON ue.shippingpackagecode = f.\"Shipping Package Code\" LEFT JOIN inward iw ON ue.order_name = iw.orderid ), new_status AS ( SELECT md.*, CASE WHEN UC_STATUS IN (\'UNFULFILLABLE\', \'DISPATCHED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\', \'CANCELLED\', \'REPLACED\', \'RETURNED\') THEN UC_STATUS ELSE COALESCE(LATEST_STATUS, UC_STATUS) END AS ITEM_STATUS, COALESCE(dispatched_timestamp1, UC_Dispatched) AS dispatched_timestamp, CASE WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-COMPLETED\' WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-PENDING\' ELSE CASE WHEN ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN dispatched_timestamp <= manual_tat THEN \'WITHIN SLA\' ELSE \'BREACHED\' END WHEN ITEM_STATUS = \'CANCELLED\' THEN CASE WHEN dispatched_timestamp IS NULL THEN \'CANCELLED\' ELSE CASE WHEN dispatched_timestamp <= manual_tat THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END WHEN ITEM_STATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\', \'PICKING_FOR_INVOICING\', \'FULFILLABLE\') THEN \'BREACHED - PENDING FOR PROCESS\' WHEN ITEM_STATUS = \'UNFULFILLABLE\' THEN \'UNFULFILLABLE\' ELSE \'BREACHED\' END END AS SLA_Status, CASE WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 0 THEN \'D0\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 1 THEN \'D1\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 2 THEN \'D2\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 3 THEN \'D3\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 3 AND TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 5 THEN \'D>3\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 5 AND TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 7 THEN \'D>5\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 7 AND TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 9 THEN \'D>7\' WHEN TIMESTAMPDIFF(day, manual_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 9 THEN \'D>9\' END AS Ageing, CASE WHEN order_id LIKE \'TRION%\' THEN \'TRION\' WHEN order_id LIKE \'EVAMAL%\' THEN \'EVAMALL\' WHEN order_id LIKE \'LANDMA%\' THEN \'LANDMARK\' WHEN order_id LIKE \'VRMALL%\' THEN \'VR MALL\' WHEN order_id LIKE \'MBHSUR%\' THEN \'MBH-SURAT\' WHEN order_id LIKE \'SHYAMA%\' THEN \'SHYAMAL\' WHEN order_id LIKE \'ANDHER%\' THEN \'INFINITI-ANDHERI\' WHEN order_id LIKE \'MALADI%\' THEN \'INFINITI-MALAD\' WHEN order_id LIKE \'VASHII%\' THEN \'INORBIT-VASHI\' WHEN order_id LIKE \'EARTH%\' THEN \'EARTH-EUPHORIA\' WHEN order_id LIKE \'SARATH%\' THEN \'SARATH-CITY\' WHEN order_id LIKE \'AMANO%\' THEN \'AMANORA\' WHEN order_id LIKE \'LINKI%\' THEN \'LINKING ROAD\' WHEN order_id LIKE \'HSRLA%\' THEN \'HSR LAYOUT\' WHEN order_id LIKE \'BRIGA%\' THEN \'BRIGADE ROAD\' WHEN order_id LIKE \'SARJA%\' THEN \'SARJAPUR ROAD\' WHEN order_id LIKE \'BELRO%\' THEN \'BEL ROAD\' WHEN order_id LIKE \'JAYAN%\' THEN \'JAYANAGAR\' WHEN order_id LIKE \'KORMAN%\' THEN \'KORMANGLA\' WHEN order_id LIKE \'KUKAT%\' THEN \'KUKATPALLY\' WHEN order_id LIKE \'VIPVI%\' THEN \'VIP-VIZAG\' WHEN order_id LIKE \'VIPSUR%\' THEN \'VIP-SURAT\' WHEN order_id LIKE \'YAGNI%\' THEN \'YAGNIK\' WHEN order_id LIKE \'NIBMT%\' THEN \'TRIBECA\' WHEN order_id LIKE \'HILIT%\' THEN \'HILITE\' when order_id LIKE \'CITYCE%\' THEN \'CITY-CENTRE\' when order_id LIKE \'RAJPUR%\' THEN \'RAJPUR-ROAD\' when order_id LIKE \'BORIVA%\' THEN \'BORIVALI\' ELSE \'Unknown Store\' END AS Store_Name FROM main_data md ) SELECT * FROM new_status WHERE (warehouse_name LIKE \'%SAPL%\' OR warehouse_name IS NULL) AND order_date::date >= \'2024-09-01\' AND marketplace_mapped IN (\'OWN_STORE\', \'FRANCHISE_STORE\');",
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
            