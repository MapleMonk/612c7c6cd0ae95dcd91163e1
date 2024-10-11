{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.warehouse_b2b_performance AS WITH unicom AS ( SELECT awb, MARKETPLACE, marketplace_mapped, shippingpackagecode, order_id, order_name, saleorderitemcode, sku, order_status, order_date, DAYNAME(order_date) AS created_day, order_timestamp, SHIPPING_LAST_UPDATE_TIMESTAMP, shipping_courier, warehouse_name, shipping_status, CASE WHEN SALE_ORDER_ITEM_STATUS = \'PICKING_FOR_INVOICING\' THEN shippingpackagestatus ELSE SALE_ORDER_ITEM_STATUS END AS UC_STATUS, dispatched_timestamp AS UC_Dispatched, dispatch_date, delivered_date, address_line_1, uc_created, CASE WHEN UPPER(MARKETPLACE) IN (\'FRANCHISE_STORE\', \'OWN_STORE\') AND EXTRACT(HOUR FROM uc_created) < 16 THEN (DATE_TRUNC(\'day\', DATEADD(day, 1, COALESCE(uc_created::timestamp, CURRENT_TIMESTAMP))) + INTERVAL \'18 hours\')::timestamp WHEN UPPER(MARKETPLACE) IN (\'FRANCHISE_STORE\', \'OWN_STORE\') AND EXTRACT(HOUR FROM uc_created) >= 16 THEN (DATE_TRUNC(\'day\', DATEADD(day, 2, COALESCE(uc_created::timestamp, CURRENT_TIMESTAMP))) + INTERVAL \'12 hours\')::timestamp ELSE COALESCE(fulfillment_tat, CURRENT_TIMESTAMP)::timestamp END AS TAT , category FROM snitch_db.maplemonk.unicommerce_fact_items_intermediate ), forward AS ( SELECT \"Shipping Package Code\", created_timestamp, picking_timestamp, picked_timestamp, packed_timestamp, rts_timestamp, manifested_timestamp, dispatched_timestamp AS dispatched_timestamp1, shipped_timestamp, delivered_timestamp, cancelled_timestamp, \"created&dispatch\", \"pick&dispatch\", \"pack&dispatch\", LATEST_STATUS, LAST_UPDATED FROM snitch_db.maplemonk.forward_timestamps ), inward AS ( SELECT TO_DATE(\"DATE\", \'DD/MM/YYYY\') AS order_added_date, TYPE AS inward_type, ORDER_NAME AS orderid, ORDERED_ITEMS, UNFULFILLABLE, \"Fulfilled Quantity\", STATE, STORE, BOXES, STATUS, REMARKS, FACILITY, PRIORITY, EWAYBILL, TO_DATE(DISPATCH_TO_STORE, \'DD/MM/YYYY\') AS dispatch_to_store_date, TO_DATE(DISPATCH_TO_YLK, \'DD/MM/YYYY\') AS dispatch_to_ylk_date, LOGISTICS_PARTNER, tat AS manual_tat FROM snitch_db.maplemonk.gs_b2b_uc ), lr_data AS ( SELECT logs_tat as out_logs_tat, l_dc_number as out_dc_number, l_lr_number as out_lr_number, dispatch_type as out_dispatch_type, TO_DATE(expected_date, \'DD/MM/YYYY\') AS out_expected_delivery_date, TO_DATE(delivered_date, \'DD/MM/YYYY\') AS out_delivered_date, TO_DATE(l_dispatch_date, \'DD/MM/YYYY\') AS out_dispatch_date, shipment_status as out_shipment_status, store_name_logs FROM snitch_db.maplemonk.gs_b2b_logistics ), logsin AS ( SELECT in_dc_no, in_store, in_so_no, in_logs_partner, TO_DATE(in_dispatch_date,\'DD/MM/YYYY\') as in_dispatch_date, TO_DATE(in_expected_date,\'DD/MM/YYYY\') as in_expected_date, TO_DATE(in_delivered_date,\'DD/MM/YYYY\') as in_delivered_date, in_shipment_status FROM snitch_db.maplemonk.gs_b2b_instation ), os_data AS ( SELECT replace(ind_so,\' \',\'\') as ind_so, TO_DATE(date,\'DD/MM/YYYY\') AS sheet_date, dc_number, lr_number FROM snitch_db.maplemonk.logistics_convertor ), main_data AS ( SELECT ue.*, f.*, iw.*, os.* FROM unicom ue LEFT JOIN forward f ON ue.shippingpackagecode = f.\"Shipping Package Code\" LEFT JOIN inward iw ON ue.order_name = iw.orderid LEFT JOIN os_data os ON ue.order_name::string = os.ind_so::string ), sd_data as ( select fresh_day, rpl_day, branch_code, store_name_format, so_code, channel, \"Shipping Address Line 1\" as FINAL_STORE from snitch_db.maplemonk.gs_store_details ), new_status AS ( SELECT md.*, ld.*, li.*, COALESCE(dc_number, li.in_dc_no) AS l_dc_number, COALESCE(md.lr_number, li.in_so_no) AS l_lr_number, COALESCE(ind_so, li.in_store) AS store_name_input_logs, COALESCE(ld.out_dispatch_date, li.in_dispatch_date) AS lr_dispatch_date, COALESCE(ld.out_delivered_date, li.in_delivered_date) AS l_delivered_date, COALESCE(ld.out_shipment_status, li.in_shipment_status) AS shipment_status, COALESCE(ld.out_expected_delivery_date,li.in_expected_date) as expected_date, ld.out_dispatch_type as dispatch_type, sd.*, CASE WHEN UC_STATUS IN (\'UNFULFILLABLE\', \'DISPATCHED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\', \'CANCELLED\', \'REPLACED\', \'RETURNED\') THEN UC_STATUS ELSE COALESCE(LATEST_STATUS, UC_STATUS) END AS ITEM_STATUS, COALESCE(dispatched_timestamp1, UC_Dispatched) AS dispatched_timestamp, CASE WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-COMPLETED\' WHEN TAT > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) AND ITEM_STATUS NOT IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURNED\') THEN \'FUTURE DATE-PENDING\' ELSE CASE WHEN ITEM_STATUS IN (\'DISPATCHED\', \'REPLACED\', \'DELIVERED\', \'SHIPPED\', \'RETURN_EXPECTED\') THEN CASE WHEN rts_timestamp <= TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END WHEN ITEM_STATUS = \'CANCELLED\' THEN CASE WHEN rts_timestamp IS NULL THEN \'CANCELLED\' ELSE CASE WHEN rts_timestamp <= TAT THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END WHEN ITEM_STATUS IN (\'CREATED\', \'MANIFESTED\', \'PICKING\', \'PICKED\', \'PACKED\', \'READY_TO_SHIP\', \'PICKING_FOR_INVOICING\', \'FULFILLABLE\') THEN \'BREACHED - PENDING FOR PROCESS\' WHEN ITEM_STATUS = \'UNFULFILLABLE\' THEN \'UNFULFILLABLE\' ELSE \'BREACHED\' END END AS SLA_Status, CASE WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 0 THEN \'D0\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 1 THEN \'D1\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 2 THEN \'D2\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) = 3 THEN \'D3\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 3 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 5 THEN \'D>3\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 5 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 7 THEN \'D>5\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 7 AND TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) <= 9 THEN \'D>7\' WHEN TIMESTAMPDIFF(day, TAT, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) > 9 THEN \'D>9\' ELSE \'Unknown Ageing\' END AS Ageing, CASE WHEN inward_type = \'FRESH ALLOCATION\' THEN CASE WHEN UPPER(created_day) = UPPER(fresh_day) AND EXTRACT(HOUR FROM order_timestamp) < 16 THEN \'WITHIN SLA\' ELSE \'BREACHED\' END ELSE CASE WHEN EXTRACT(HOUR FROM order_timestamp) < 13 THEN \'WITHIN SLA\' ELSE \'BREACHED\' END END AS creation_sla, CASE WHEN sheet_date IS NULL THEN \'BREACHED-PENDING FOR PROCESS\' WHEN sheet_date > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) THEN \'FUTURE DATE-PENDING\' WHEN sheet_date <= date_trunc(\'day\',dateadd(day,0,TAT)) + interval \'23 hours\' THEN \'WITHIN SLA\' ELSE \'BREACHED\' END AS logistics_sla, CASE WHEN state = \'Gujarat\' THEN date_trunc(\'day\',dateadd(day,6,TAT)) + interval \'23 hours\' WHEN state = \'Andhra Pradesh\' THEN date_trunc(\'day\',dateadd(day,5,TAT)) + interval \'23 hours\' WHEN state = \'Maharashtra\' THEN date_trunc(\'day\',dateadd(day,6,TAT)) + interval \'23 hours\' WHEN state = \'Kerala\' THEN date_trunc(\'day\',dateadd(day,4,TAT)) + interval \'23 hours\' WHEN state = \'Madhya Pradesh\' THEN date_trunc(\'day\',dateadd(day,6,TAT)) + interval \'23 hours\' WHEN state = \'Uttarakhand\' THEN date_trunc(\'day\',dateadd(day,8,TAT)) + interval \'23 hours\' WHEN state = \'Karnataka\' THEN date_trunc(\'day\',dateadd(day,1,TAT)) + interval \'23 hours\' ELSE TAT END AS delivery_tat, CASE WHEN delivery_tat > CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP()) THEN \'FUTURE DATE-PENDING\' when l_delivered_date IS NULL THEN \'BREACHED-PENDING FOR PROCESS\' WHEN l_delivered_date > delivery_tat then \'BREACHED\' WHEN l_delivered_date <= delivery_tat THEN \'WITHIN SLA\' ELSE \'BREACHED\' END AS delivery_sla, TIMESTAMPDIFF(day, sheet_date, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) as Logistics_ageing, TIMESTAMPDIFF(day, delivery_tat, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())) as delivery_ageing, CASE when status = \'Cancelled\' OR (item_status not IN (\'DISPATCHED\',\'MANIFESTED\',\'READY_TO_SHIP\',\'PACKED\',\'SHIPPED\') AND (l_lr_number is not null OR lower(status) like \'%dispatch%\')) then \'CANCELLATION-PENDING\' when dispatch_to_ylk_date is not null and lower(status) = \'dispatched to wh\' and shipment_status is null then \'WH-YLK-PENDING\' WHEN l_lr_number IS NOT NULL AND shipment_status IS NULL THEN \'MISSING_LOGS_TRACKER\' when dispatch_to_ylk_date is not null and lower(status) = \'dispatched to store\' and shipment_status is null then \'MISSING/INCORRECT-DISPATCH\' WHEN LOWER(sla_status) LIKE \'%future%\' AND LOWER(sla_status) LIKE \'%pending%\' AND item_status NOT IN (\'DISPATCHED\',\'MANIFESTED\',\'READY_TO_SHIP\',\'SHIPPED\') THEN \'WH-FUTURETAT\' WHEN item_status NOT IN (\'DISPATCHED\',\'MANIFESTED\',\'READY_TO_SHIP\',\'SHIPPED\') THEN \'WH-PENDENCY\' WHEN item_status IN (\'DISPATCHED\',\'MANIFESTED\',\'READY_TO_SHIP\',\'SHIPPED\') AND l_lr_number IS NULL THEN \'PICKUPPENDING\' WHEN LOWER(shipment_status) IN (\'failed delivered\',\'in-transit\',\'Out for Delivery\') THEN \'INTRANSIT\' WHEN LOWER(shipment_status) = \'delivered\' THEN \'DELIVERED\' END AS final_status, CASE WHEN final_status like \'%WH%\' THEN item_status WHEN LOWER(shipment_status) = \'DELIVERED\' THEN logistics_sla when final_status in (\'INTRANSIT\') then shipment_status when final_status = \'PICKUPPENDING\' AND lower(status) in (\'on hold\',\'duplicate\',\'ready to dispatch\',\'rts-logic\') then status END AS detailed_status FROM main_data md LEFT JOIN lr_data ld ON md.lr_number = ld.out_lr_number LEFT JOIN sd_data sd ON md.store = sd.store_name_format LEFT JOIN logsin li on md.order_name = li.in_so_no ) SELECT * FROM new_status WHERE marketplace IN (\'OWN_STORE\',\'FRANCHISE_STORE\') AND order_date > \'2024-08-31\' AND ITEM_STATUS NOT IN (\'CANCELLED\') ;",
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
            