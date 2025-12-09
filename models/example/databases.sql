{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE vms_updated AS WITH w AS ( SELECT wsp.*, REGEXP_SUBSTR(TO_VARCHAR(wsp.awb), \'[0-9]{8,}\', 1, 1) AS awb_num_key FROM snitch_db.maplemonk.warehouse_sla_performance wsp WHERE marketplace_mapped = \'MYNTRA\' AND warehouse_name = \'SAPL-WH2\' AND DATE_TRUNC(\'DAY\', packed_time) >= \'2025-12-01\' AND awb IS NOT NULL ), v AS ( SELECT vms.*, REGEXP_SUBSTR(TO_VARCHAR(vms.awb), \'[0-9]{8,}\', 1, 1) AS awb_num_key, ROW_NUMBER() OVER ( PARTITION BY REGEXP_SUBSTR(TO_VARCHAR(vms.awb), \'[0-9]{8,}\', 1, 1) ORDER BY vms._AIRBYTE_EMITTED_AT DESC ) AS rn FROM snitch_db.maplemonk.gs_vms_sync vms WHERE awb IS NOT NULL ) SELECT w.*, v.location AS vms_location, v.scandate AS vms_scandate, v.timestamp AS vms_timestamp, v._AIRBYTE_EMITTED_AT AS vms_emitted_at FROM w LEFT JOIN v ON w.awb_num_key = v.awb_num_key AND v.rn = 1;",
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
            