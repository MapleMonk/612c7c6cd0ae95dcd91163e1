{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.S2_VMS_SCAN_STATUS AS WITH sla_dedup AS ( SELECT * FROM ( SELECT s.*, ROW_NUMBER() OVER ( PARTITION BY s.AWB ORDER BY COALESCE( s.LAST_UPDATED, s.DELIVERED_TIMESTAMP, s.DISPATCHED_TIMESTAMP, s.ORDER_DATE ) DESC ) AS rn FROM SNITCH_DB.MAPLEMONK.warehouse_sla_performance s WHERE s.MARKETPLACE_MAPPED = \'MYNTRA\' AND CAST(s.ORDER_DATE AS DATE) >= \'2025-11-30\' ) WHERE rn = 1 ) SELECT v.seller_id, v.warehouse_id, v.order_id, v.packed_on, v.awb, v.tracking_id_2_3pl, v.video_size_sec, v.remarks, s.ORDER_DATE, s.WAREHOUSE_NAME, s.SHIPPING_COURIER, s.SHIPPING_STATUS, s.LATEST_STATUS, s.TAT, CASE WHEN s.AWB IS NOT NULL AND v.AWB IS NULL THEN \'NOT SCANNED IN VMS\' ELSE \'SCANNED IN VMS\' END AS DERIVED_SCAN_STATUS, CASE WHEN v.remarks IS NOT NULL THEN v.remarks ELSE CASE WHEN s.AWB IS NOT NULL AND v.AWB IS NULL THEN \'NOT SCANNED IN VMS\' ELSE \'SCANNED IN VMS\' END END AS FINAL_SCAN_STATUS FROM SNITCH_DB.MAPLEMONK.S2_VMS_DATA v FULL OUTER JOIN sla_dedup s ON v.AWB = s.AWB;",
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
            