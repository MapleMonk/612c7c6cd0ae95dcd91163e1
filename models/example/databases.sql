{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE returns_qc_tat AS SELECT *, CASE WHEN qc_status IN (\'Verified UC\', \'Completed by Agent\') OR qc_remarks IN (\'Verified UC\', \'Completed by Agent\') THEN \'Completed\' ELSE qc_status END AS final_qc_status, CASE WHEN COALESCE( TRY_TO_TIMESTAMP(QC_TIMESTAMP, \'MON DD, YYYY HH12:MI AM\'), TRY_TO_TIMESTAMP(TOTE_NUMBER, \'YYYY-MM-DD HH24:MI:SS\'), DATEADD(day, 1, TRY_TO_DATE(SCANNING_DATE, \'DD-MM-YYYY\')) ) <= DATEADD(hour, 63, TRY_TO_DATE(RECEIVING_DATE, \'DD-MM-YYYY\')) THEN \'Within TAT\' ELSE \'TAT Breached\' END AS tat_compliance_status, DATEDIFF( hour, DATEADD(hour, 15, TRY_TO_DATE(RECEIVING_DATE, \'DD-MM-YYYY\')), COALESCE( TRY_TO_TIMESTAMP(QC_TIMESTAMP, \'MON DD, YYYY HH12:MI AM\'), TRY_TO_TIMESTAMP(TOTE_NUMBER, \'YYYY-MM-DD HH24:MI:SS\'), DATEADD(day, 1, TRY_TO_DATE(SCANNING_DATE, \'DD-MM-YYYY\')) ) ) AS tat_hours_taken FROM return_2_inwarding_data;",
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
            