{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fact_attendance AS WITH latest_headers AS ( SELECT h.* FROM snitch_db.maplemonk.attendance_header h INNER JOIN ( SELECT attendance_date, facility, department, COALESCE(shift, \'\') AS shift_key, MAX(id) AS max_id FROM snitch_db.maplemonk.attendance_header GROUP BY attendance_date, facility, department, COALESCE(shift, \'\') ) latest ON h.id = latest.max_id ), enriched AS ( SELECT TO_NUMBER(TO_CHAR(lh.attendance_date, \'YYYYMMDD\')) AS date_sk, de.employee_sk, lh.attendance_date, d.employee_code, d.employee_name, lh.facility, lh.department, lh.shift, d.attendance_status, lh.marked_by, d.remarks, CASE WHEN rr.request_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_edited, rr.request_id AS rewrite_request_id, rr.actioned_by AS edit_approved_by, d.attendance_status IN ( \'Present\', \'Half Day\', \'Work On Holiday\' ) AS is_present, d.attendance_status = \'Absent\' AS is_absent, d.attendance_status IN ( \'Sick Leave\', \'Paid Leave\', \'Unpaid Leave\', \'Maternity Leave\', \'Paternity Leave\', \'Bereavement Leave\' ) AS is_on_leave, d.attendance_status = \'Week Off\' AS is_week_off, lh.id AS source_header_id, d.id AS source_detail_id, d.created_at AS marked_at, lh.marked_at AS submitted_at, CURRENT_TIMESTAMP() AS loaded_at FROM latest_headers lh INNER JOIN snitch_db.maplemonk.attendance_detail d ON d.attendance_header_id = lh.id LEFT JOIN snitch_db.maplemonk.dim_employee de ON de.employee_code = d.employee_code LEFT JOIN ( SELECT attendance_date, facility, department, request_id, actioned_by, ROW_NUMBER() OVER ( PARTITION BY attendance_date, facility, department ORDER BY actioned_at DESC ) AS rn FROM snitch_db.maplemonk.attendance_rewrite_requests WHERE request_status = \'approved\' ) rr ON rr.attendance_date = lh.attendance_date AND rr.facility = lh.facility AND rr.department = lh.department AND rr.rn = 1 ) SELECT * FROM enriched;",
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
            