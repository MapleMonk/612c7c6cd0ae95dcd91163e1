{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.fact_attendance AS WITH latest_detail AS ( SELECT * FROM ( SELECT d.*, h.attendance_date AS hdr_date, h.facility AS hdr_facility, h.department AS hdr_department, h.shift AS hdr_shift, h.marked_by AS hdr_marked_by, h.marked_at AS hdr_marked_at, h.status AS hdr_status, ROW_NUMBER() OVER ( PARTITION BY d.employee_code, h.attendance_date, h.facility, h.department, COALESCE(h.shift, \'Day\') ORDER BY h.id DESC, d.id DESC ) AS rn FROM snitch_db.maplemonk.attendance_detail d INNER JOIN snitch_db.maplemonk.attendance_header h ON h.id = d.attendance_header_id WHERE h.attendance_date >= \'2026-04-26\' ) t WHERE rn = 1 ), enriched AS ( SELECT TO_NUMBER(TO_CHAR(ld.hdr_date, \'YYYYMMDD\')) AS date_sk, ld.hdr_date AS attendance_date, ld.employee_id, ld.employee_code, ld.employee_name, COALESCE(de.facility, ld.hdr_facility) AS facility, ld.hdr_department AS department, ld.hdr_shift AS shift, ld.attendance_status, ld.hdr_marked_by AS marked_by, ld.remarks, CASE WHEN rr.request_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_edited, rr.request_id AS rewrite_request_id, rr.actioned_by AS edit_approved_by, ld.attendance_status IN ( \'Present\', \'Half Day\', \'Work On Holiday\' ) AS is_present, ld.attendance_status = \'Absent\' AS is_absent, ld.attendance_status IN ( \'Sick Leave\', \'Paid Leave\', \'Unpaid Leave\', \'Maternity Leave\', \'Paternity Leave\', \'Bereavement Leave\' ) AS is_on_leave, ld.attendance_status = \'Week Off\' AS is_week_off, ld.attendance_header_id AS source_header_id, ld.id AS source_detail_id, ld.created_at AS marked_at, ld.hdr_marked_at AS submitted_at, CONVERT_TIMEZONE(\'Asia/Kolkata\', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ AS loaded_at FROM latest_detail ld LEFT JOIN snitch_db.maplemonk.dim_employee de ON de.employee_code = ld.employee_code LEFT JOIN ( SELECT attendance_date, facility, department, request_id, actioned_by, ROW_NUMBER() OVER ( PARTITION BY attendance_date, facility, department ORDER BY actioned_at DESC ) AS rn FROM snitch_db.maplemonk.attendance_rewrite_requests WHERE request_status = \'approved\' AND attendance_date >= \'2026-04-26\' ) rr ON rr.attendance_date = ld.hdr_date AND rr.facility = ld.hdr_facility AND rr.department = ld.hdr_department AND rr.rn = 1 ) SELECT * FROM enriched;",
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
            