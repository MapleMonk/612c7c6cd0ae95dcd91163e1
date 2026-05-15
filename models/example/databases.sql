{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE omni_logic_check AS WITH omni_base AS ( SELECT invoice_no, CONVERT_TIMEZONE( \'UTC\', \'Asia/Kolkata\', created_at ) AS created_at_ist, DATE( CONVERT_TIMEZONE( \'UTC\', \'Asia/Kolkata\', created_at ) ) AS created_date_ist, TRY_TO_DECIMAL(total_price, 18, 2) AS omni_amount, order_status, created_at FROM omni_order_processing_meta WHERE is_quick_com = FALSE AND invoice_no IS NOT NULL AND TRIM(invoice_no) <> \'\' AND LOWER(TRIM(invoice_no)) <> \'null\' AND order_status NOT IN ( \'AWB_CANCELLED\', \'AWB_FAILED\', \'CANCELED\' ) QUALIFY ROW_NUMBER() OVER ( PARTITION BY invoice_no ORDER BY created_at DESC ) = 1 ), logic_deduped AS ( SELECT TRIM( _airbyte_data:\"Order_No\"::STRING ) AS order_no, TRY_TO_DECIMAL( _airbyte_data:\"Net_Amount\"::STRING, 18, 2 ) AS logic_amount, TO_DATE( _airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\' ) AS logic_bill_date, _airbyte_data:\"GR_Number\"::STRING AS gr_number, _airbyte_data:\"Branch_Code\"::STRING AS branch_code FROM snitch_db.maplemonk._airbyte_raw_logicerpnew_get_sale_invoice WHERE _airbyte_data:\"Order_No\"::STRING IS NOT NULL AND TRIM(_airbyte_data:\"Order_No\"::STRING) <> \'\' QUALIFY ROW_NUMBER() OVER ( PARTITION BY TRIM( _airbyte_data:\"Order_No\"::STRING ) ORDER BY TO_DATE( _airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\' ) DESC ) = 1 ) SELECT o.created_date_ist, o.created_at_ist, o.invoice_no, o.omni_amount, o.order_status, l.order_no, l.logic_amount, l.logic_bill_date, l.gr_number, l.branch_code, CASE WHEN l.order_no IS NULL THEN \'MISMATCH\' ELSE \'MATCHED\' END AS match_status FROM omni_base o LEFT JOIN logic_deduped l ON TRIM(o.invoice_no) = TRIM(l.order_no);",
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
            