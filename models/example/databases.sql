{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.beastlife_shiprocket_Query as SELECT f.channel_order_id,f.awb,f.courier,zone,d.Order_Date,EXTRACT(HOUR FROM d.order_time) AS hour,f.order_status, DATE(SAFE.PARSE_TIMESTAMP(\'%Y-%m-%d %H:%M:%S\',COALESCE(NULLIF(f.picked_up_date, \'\'), p.picked_up_date),\'Asia/Kolkata\' )) AS picked_up_date, awb_assign_date, f.delivered_date, pickup_scheduled_date, rto_delivered_date, rto_initiated_date, EXTRACT(HOUR FROM SAFE.PARSE_TIMESTAMP( \'%Y-%m-%d %H:%M:%S\', COALESCE(NULLIF(f.picked_up_date, \'\'), p.picked_up_date), \'Asia/Kolkata\' ) ) AS PickedUp_hour, DATE_DIFF(CURRENT_DATE(\'Asia/Kolkata\'),DATE(SAFE.PARSE_TIMESTAMP( \'%Y-%m-%d %H:%M:%S\',COALESCE(NULLIF(f.picked_up_date, \'\'), p.picked_up_date),\'Asia/Kolkata\')), DAY) AS TAT_Days, CASE WHEN zone = \'z_a\' AND UPPER(f.courier) LIKE \'%NDD%\' THEN 1 WHEN zone = \'z_a\' AND UPPER(f.courier) LIKE \'%SURFACE%\' THEN 2 WHEN zone = \'z_a\' THEN 1 WHEN zone = \'z_b\' THEN 3 WHEN zone = \'z_c\' THEN 5 WHEN zone = \'z_d\' THEN 7 WHEN zone = \'z_e\' THEN 9 ELSE NULl END AS Zone_Expected_TAT, CASE WHEN DATE_DIFF(CURRENT_DATE(\'Asia/Kolkata\'),DATE(SAFE.PARSE_TIMESTAMP( \'%Y-%m-%d %H:%M:%S\', COALESCE(NULLIF(f.picked_up_date, \'\'), p.picked_up_date),\'Asia/Kolkata\' )),DAY) > CASE WHEN zone = \'z_a\' AND UPPER(f.courier) LIKE \'%NDD%\' THEN 1 WHEN zone = \'z_a\' AND UPPER(f.courier) LIKE \'%SURFACE%\' THEN 2 WHEN zone = \'z_b\' THEN 3 WHEN zone = \'z_c\' THEN 5 WHEN zone = \'z_d\' THEN 7 WHEN zone = \'z_e\' THEN 9 ELSE NULL END THEN \'TAT Breached\' ELSE \'Not Breached\' END AS TAT_Breach_Status, CASE COALESCE(shippingPackageStatus, u.ORDER_STATUS) WHEN \'DELIVERED\' THEN \'DISPATCHED\' WHEN \'RETURN_EXPECTED\' THEN \'DISPATCHED\' WHEN \'SHIPPED\' THEN \'DISPATCHED\' WHEN \'RETURN_ACKNOWLEDGED\' THEN \'DISPATCHED\' WHEN \'MANIFESTED\' THEN \'DISPATCHED\' WHEN \'DISPATCHED\' THEN \'DISPATCHED\' WHEN \'RETURNED\' THEN \'RETURNED\' WHEN \'CANCELLED\' THEN \'CANCELLED\' WHEN \'ALTERNATE_ACCEPTED\' THEN \'ALTERNATE_ACCEPTED\' WHEN \'PACKED\' THEN \'PACKED\' WHEN \'READY_TO_SHIP\' THEN \'READY_TO_SHIP\' WHEN \'PICKED\' THEN \'PICKED\' WHEN \'PICKING\' THEN \'PICKING\' WHEN \'CREATED\' THEN \'CREATED\' WHEN \'FULFILLABLE\' THEN \'UNFULFILLABLE\' WHEN \'UNFULFILLABLE\' THEN \'UNFULFILLABLE\' ELSE \'UNKNOWN\' END AS Consolidated_Status FROM `maplemonk.beastlife_shiprocket_fact_items_current` f LEFT JOIN maplemonk.Beastlife_db_Shiprocket_pickedup_ p ON f.awb = p.awb LEFT JOIN `maplemonk.beastlife_unicommerce_fact_items` u ON f.channel_order_id = u.order_id LEFT JOIN `maplemonk.D2C_master` d ON f.channel_order_id = d.ordeR_name",
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
            