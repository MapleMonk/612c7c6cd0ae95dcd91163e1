{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_b2b_performance AS WITH unicom AS ( SELECT awb, MARKETPLACE, marketplace_mapped, shippingpackagecode, order_id, order_name, saleorderitemcode, sku, order_status, order_date, order_timestamp, SHIPPING_LAST_UPDATE_TIMESTAMP, shipping_courier, warehouse_name, shipping_status, CASE WHEN SALE_ORDER_ITEM_STATUS = \'PICKING_FOR_INVOICING\' THEN shippingpackagestatus ELSE SALE_ORDER_ITEM_STATUS END AS UC_STATUS, dispatched_timestamp AS UC_Dispatched, dispatch_date, delivered_date, address_line_1, uc_created, CASE WHEN UPPER(MARKETPLACE) IN (\'FRANCHISE_STORE\', \'OWN_STORE\') AND EXTRACT(HOUR FROM uc_created) < 16 THEN DATE_TRUNC(\'day\', DATEADD(day, 1, COALESCE(uc_created::timestamp, CURRENT_TIMESTAMP))) + INTERVAL \'18 hours\' WHEN UPPER(MARKETPLACE) IN (\'FRANCHISE_STORE\', \'OWN_STORE\') AND EXTRACT(HOUR FROM uc_created) >= 16 THEN DATE_TRUNC(\'day\', DATEADD(day, 2, COALESCE(uc_created::timestamp, CURRENT_TIMESTAMP))) + INTERVAL \'12 hours\' ELSE COALESCE(fulfillment_tat, CURRENT_TIMESTAMP) END AS TAT, category FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate ), forward AS ( SELECT \"Shipping Package Code\", created_timestamp, picking_timestamp, picked_timestamp, packed_timestamp, rts_timestamp, manifested_timestamp, dispatched_timestamp AS dispatched_timestamp1, shipped_timestamp, delivered_timestamp, cancelled_timestamp, \"created&dispatch\", \"pick&dispatch\", \"pack&dispatch\", LATEST_STATUS, LAST_UPDATED FROM snitch_db.maplemonk.forward_timestamps ), inward AS ( SELECT TO_DATE(\"DATE\", \'DD/MM/YYYY\') AS inward_date, TYPE AS inward_type, ORDER_NAME AS orderid, ORDERED_ITEMS, UNFULFILLABLE, \"Fulfilled Quantity\", STATE, STORE, BOXES, STATUS, REMARKS, FACILITY, PRIORITY, EWAYBILL, TO_DATE(DISPATCH_TO_STORE, \'DD/MM/YYYY\') AS dispatch_to_store_date, TO_DATE(DISPATCH_TO_YLK, \'DD/MM/YYYY\') AS dispatch_to_ylk_date, LOGISTICS_PARTNER, tat AS manual_tat FROM snitch_db.maplemonk.gs_b2b_uc ), lr_data AS ( SELECT logs_tat, l_dc_number, l_lr_number, dispatch_type, TO_DATE(expected_date, \'DD-MM-YYYY\') AS expected_dispatch_date, TO_DATE(delivered_date, \'DD/MM/YYYY\') AS l_delivered_date, TO_DATE(l_dispatch_date, \'DD/MM/YYYY\') AS lr_dispatch_date, shipment_status, store_name_logs FROM snitch_db.maplemonk.gs_b2b_logistics ), os_data AS ( SELECT ind_so, TO_DATE(date, \'DD/MM/YYYY\') AS sheet_date, dc_number, lr_number, d_quantity, no_boxes, so_number, remark AS sheet_remark FROM snitch_db.maplemonk.logistics_convertor ) , main_data AS ( SELECT ue.*, f.*, iw.*, os.* FROM unicom ue LEFT JOIN forward f ON ue.shippingpackagecode = f.\"Shipping Package Code\" LEFT JOIN inward iw ON ue.order_name = iw.orderid LEFT JOIN os_data os ON ue.order_name = os.ind_so ), new_status AS ( SELECT md.*, ld.*, CASE WHEN UC_STATUS IN (\'UNFULFILLABLE\', \'DISPATCHED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\', \'CANCELLED\', \'REPLACED\', \'RETURNED\') THEN UC_STATUS ELSE COALESCE(LATEST_STATUS, UC_STATUS) END AS ITEM_STATUS, COALESCE(dispatched_timestamp1, UC_Dispatched) AS dispatched_timestamp, CASE WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-COMPLETED\' WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-PENDING\' ELSE CASE WHEN ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN rts_timestamp <= TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END WHEN ITEM_STATUS = \'CANCELLED\' THEN CASE WHEN rts_timestamp IS NULL THEN \'CANCELLED\' ELSE CASE WHEN rts_timestamp <= TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END WHEN ITEM_STATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\', \'PICKING_FOR_INVOICING\', \'FULFILLABLE\') THEN \'BREACHED - PENDING FOR PROCESS\' WHEN ITEM_STATUS = \'UNFULFILLABLE\' THEN \'UNFULFILLABLE\' ELSE \'BREACHED\' END END AS SLA_Status, CASE WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 0 THEN \'D0\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 1 THEN \'D1\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 2 THEN \'D2\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 3 THEN \'D3\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 3 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 5 THEN \'D>3\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 5 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 7 THEN \'D>5\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 7 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 9 THEN \'D>7\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 9 THEN \'D>9\' ELSE \'Unknown Ageing\' END AS Ageing, CASE WHEN order_id LIKE \'TRION%\' THEN \'TRION\' WHEN order_id LIKE \'EVAMAL%\' THEN \'EVAMALL\' WHEN order_id LIKE \'LANDMA%\' THEN \'LANDMARK\' WHEN order_id LIKE \'VRMALL%\' THEN \'VR MALL\' WHEN order_id LIKE \'MBHSUR%\' THEN \'MBH-SURAT\' WHEN order_id LIKE \'SHYAMA%\' THEN \'SHYAMAL\' WHEN order_id LIKE \'ANDHER%\' THEN \'INFINITI-ANDHERI\' WHEN order_id LIKE \'MALADI%\' THEN \'INFINITI-MALAD\' WHEN order_id LIKE \'VASHII%\' THEN \'INORBIT-VASHI\' WHEN order_id LIKE \'EARTH%\' THEN \'EARTH-EUPHORIA\' WHEN order_id LIKE \'SARATH%\' THEN \'SARATH-CITY\' WHEN order_id LIKE \'AMANO%\' THEN \'AMANORA\' WHEN order_id LIKE \'LINKI%\' THEN \'LINKING ROAD\' WHEN order_id LIKE \'HSRLA%\' THEN \'HSR LAYOUT\' WHEN order_id LIKE \'BRIGA%\' THEN \'BRIGADE ROAD\' WHEN order_id LIKE \'SARJA%\' THEN \'SARJAPUR ROAD\' WHEN order_id LIKE \'BELRO%\' THEN \'BEL ROAD\' WHEN order_id LIKE \'JAYAN%\' THEN \'JAYANAGAR\' WHEN order_id LIKE \'KORMAN%\' THEN \'KORMANGLA\' WHEN order_id LIKE \'KUKAT%\' THEN \'KUKATPALLY\' WHEN order_id LIKE \'VIPVI%\' THEN \'VIP-VIZAG\' WHEN order_id LIKE \'VIPSU%\' THEN \'VIP-SURAT\' ELSE \'UNKNOWN\' END AS STORE_unique FROM main_data md LEFT JOIN lr_data ld ON md.lr_number = ld.l_lr_number ) SELECT * FROM new_status WHERE marketplace IN (\'OWN_STORE\',\'FRANCHISE_STORE\') AND order_date > \'2024-09-12\' AND ITEM_STATUS NOT IN (\'CANCELLED\');",
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
            