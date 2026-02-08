{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.S2_VMS_SCAN_STATUS AS WITH sla_dedup AS ( SELECT awb, warehouse_name, shipping_courier, shipping_status, latest_status, tat, CAST(packed_time AS DATE) AS sla_packed_date FROM ( SELECT s.awb, s.warehouse_name, s.shipping_courier, s.shipping_status, s.latest_status, s.tat, s.packed_time, ROW_NUMBER() OVER ( PARTITION BY s.awb ORDER BY s.order_date DESC ) AS rn FROM SNITCH_DB.MAPLEMONK.warehouse_sla_performance s WHERE s.marketplace_mapped = \'MYNTRA\' ) WHERE rn = 1 ), vms_scan AS ( SELECT std_awb AS awb, MAX(scan_date) AS vms_scan_date FROM ( SELECT UPPER(REPLACE(REPLACE(TRIM(r._airbyte_data:\"tracking_id_1\"::STRING),\' \',\'\'),\'-\',\'\')) AS std_awb, COALESCE( TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING, \'DD-MM-YYYY\'), TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING, \'DD/MM/YYYY\'), TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING) ) AS scan_date FROM SNITCH_DB.MAPLEMONK._AIRBYTE_RAW_S2_VMS_DATA r WHERE r._airbyte_data:\"tracking_id_1\" IS NOT NULL UNION ALL SELECT UPPER(REPLACE(REPLACE(TRIM(r._airbyte_data:\"tracking_id_2_3pl\"::STRING),\' \',\'\'),\'-\',\'\')) AS std_awb, COALESCE( TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING, \'DD-MM-YYYY\'), TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING, \'DD/MM/YYYY\'), TRY_TO_DATE(r._airbyte_data:\"packed_on\"::STRING) ) AS scan_date FROM SNITCH_DB.MAPLEMONK._AIRBYTE_RAW_S2_VMS_DATA r WHERE r._airbyte_data:\"tracking_id_2_3pl\" IS NOT NULL UNION ALL SELECT UPPER(REPLACE(REPLACE(TRIM(g.awb),\' \',\'\'),\'-\',\'\')) AS std_awb, NULL AS scan_date FROM SNITCH_DB.MAPLEMONK.gs_vms_sync g WHERE g.awb IS NOT NULL ) GROUP BY std_awb ) SELECT sla.awb, COALESCE(vms.vms_scan_date, sla.sla_packed_date) AS scan_date, sla.warehouse_name, sla.shipping_courier, sla.shipping_status, sla.latest_status, sla.tat, CASE WHEN vms.awb IS NULL THEN \'NOT SCANNED IN VMS\' ELSE \'SCANNED IN VMS\' END AS final_scan_status FROM sla_dedup sla LEFT JOIN vms_scan vms ON UPPER(REPLACE(REPLACE(TRIM(sla.awb),\' \',\'\'),\'-\',\'\')) = vms.awb;",
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
            