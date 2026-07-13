{{ config(
            materialized='table',
                post_hook={
                    "sql": "TRUNCATE TABLE `kerala-ayurveda-wh.MapleMonk.Swiggy_PO_po_booking_report_fact_table`; INSERT INTO `kerala-ayurveda-wh.MapleMonk.Swiggy_PO_po_booking_report_fact_table` SELECT `_airbyte_unique_key`, SAFE_CAST(NULLIF(TRIM(`DATE`), \'\') AS DATE) AS `DATE`, NULLIF(TRIM(`Slot`), \'\') AS `Slot`, SAFE_CAST(NULLIF(TRIM(`SlotDate`), \'\') AS DATE) AS `SlotDate`, NULLIF(TRIM(`CreatedBy`), \'\') AS `CreatedBy`, NULLIF(TRIM(`UpdatedBy`), \'\') AS `UpdatedBy`, NULLIF(TRIM(`FacilityId`), \'\') AS `FacilityId`, SAFE.PARSE_TIMESTAMP( \'%Y-%m-%d %H:%M:%E*S %z\', REGEXP_REPLACE( NULLIF(TRIM(`CreatedTime`), \'\'), r\'\s+IST$\', \'\' ) ) AS `CreatedTime`, SAFE.PARSE_TIMESTAMP( \'%Y-%m-%d %H:%M:%E*S %z\', REGEXP_REPLACE( NULLIF(TRIM(`UpdatedTime`), \'\'), r\'\s+IST$\', \'\' ) ) AS `UpdatedTime`, NULLIF(TRIM(`FacilityName`), \'\') AS `FacilityName`, NULLIF(TRIM(`SupplierCode`), \'\') AS `SupplierCode`, NULLIF(TRIM(`SupplierName`), \'\') AS `SupplierName`, NULLIF(TRIM(`AppointmentId`), \'\') AS `AppointmentId`, SAFE_CAST( REPLACE( NULLIF(TRIM(`BookedQuantity`), \'\'), \',\', \'\' ) AS FLOAT64 ) AS `BookedQuantity`, NULLIF(TRIM(`AppointmentType`), \'\') AS `AppointmentType`, NULLIF(TRIM(`AppointmentState`), \'\') AS `AppointmentState`, NULLIF(TRIM(`BusinessCategory`), \'\') AS `BusinessCategory`, NULLIF(TRIM(`PurchaseOrderIds`), \'\') AS `PurchaseOrderIds`, `_airbyte_ab_id`, `_airbyte_emitted_at`, `_airbyte_normalized_at`, `_airbyte_Swiggy_PO_po_booking_report_hashid`, CURRENT_TIMESTAMP() AS `bq_load_ts` FROM `kerala-ayurveda-wh.MapleMonk.Swiggy_PO_po_booking_report`;",
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
            