{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE s2_vms_data AS WITH vms_base AS ( SELECT _airbyte_data:\"seller_id\"::string AS seller_id, _airbyte_data:\"warehouse_id\"::string AS warehouse_id, _airbyte_data:\"order_id\"::string AS order_id, CASE WHEN TRY_TO_DATE(_airbyte_data:\"packed_on\"::string, \'DD-MM-YYYY\') IS NOT NULL THEN TRY_TO_DATE(_airbyte_data:\"packed_on\"::string, \'DD-MM-YYYY\') WHEN TRY_TO_DATE(_airbyte_data:\"packed_on\"::string, \'DD/MM/YYYY\') IS NOT NULL THEN TRY_TO_DATE(_airbyte_data:\"packed_on\"::string, \'DD/MM/YYYY\') ELSE TRY_TO_DATE(_airbyte_data:\"packed_on\"::string) END AS packed_on, _airbyte_data:\"tracking_id_1\"::string AS tracking_id_1, TRY_TO_NUMBER(_airbyte_data:\"video_size_sec\"::string) AS video_size_sec, _airbyte_data:\"Remarks\"::string AS remarks FROM snitch_db.MAPLEMONK._AIRBYTE_RAW_S2_VMS_DATA ), sla_dedup AS ( SELECT * FROM ( SELECT w.*, ROW_NUMBER() OVER ( PARTITION BY w.AWB ORDER BY COALESCE( w.LAST_UPDATED, w.DELIVERED_TIMESTAMP, w.DISPATCHED_TIMESTAMP1 ) DESC ) AS rn FROM snitch_db.maplemonk.warehouse_sla_performance w WHERE w.MARKETPLACE_MAPPED = \'MYNTRA\' AND w.WAREHOUSE_NAME IN ( \'SAPL-WH1\', \'SAPL-WH2\', \'SAPL-NORTH-TAURU\' ) ) WHERE rn = 1 ) SELECT v.seller_id, v.warehouse_id, v.order_id, v.packed_on, v.tracking_id_1 AS awb, v.video_size_sec, v.remarks, s.MARKETPLACE, s.ORDER_ID AS marketplace_order_id, s.WAREHOUSE_NAME, s.SHIPPING_COURIER, s.SHIPPING_STATUS, s.LATEST_STATUS, s.TAT, s.SLA_STATUS, s.DISPATCHED_TIMESTAMP, s.DELIVERED_TIMESTAMP FROM vms_base v LEFT JOIN sla_dedup s ON v.tracking_id_1 = s.AWB;",
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
            