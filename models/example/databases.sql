{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE omni_logic_check as WITH omni_base AS ( SELECT invoice_no, CONVERT_TIMEZONE( \'UTC\', \'Asia/Kolkata\', created_at ) AS created_at_ist, total_price, order_status FROM omni_order_processing_meta WHERE CONVERT_TIMEZONE( \'UTC\', \'Asia/Kolkata\', created_at ) >= DATEADD(DAY, -2, CURRENT_DATE()) AND CONVERT_TIMEZONE( \'UTC\', \'Asia/Kolkata\', created_at ) < DATEADD(DAY, -1, CURRENT_DATE()) AND is_quick_com = FALSE AND invoice_no IS NOT NULL AND TRIM(invoice_no) <> \'\' AND order_status NOT IN ( \'AWB_CANCELLED\', \'AWB_FAILED\', \'CANCELED\' ) QUALIFY ROW_NUMBER() OVER ( PARTITION BY invoice_no ORDER BY created_at DESC ) = 1 ), logic_deduped AS ( SELECT _airbyte_data:\"Order_No\"::STRING AS order_no, TRY_TO_DECIMAL( _airbyte_data:\"Net_Amount\"::STRING, 18, 2 ) AS net_amount, TO_DATE( _airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\' ) AS bill_date, _airbyte_data:\"GR_Number\"::STRING AS gr_number FROM snitch_db.maplemonk._airbyte_raw_logicerpnew_get_sale_invoice QUALIFY ROW_NUMBER() OVER ( PARTITION BY _airbyte_data:\"Order_No\"::STRING ORDER BY TO_DATE( _airbyte_data:\"Bill_Date\"::STRING, \'DD/MM/YYYY\' ) DESC ) = 1 ) SELECT o.invoice_no, o.created_at_ist, o.total_price AS omni_amount, o.order_status, l.order_no, l.net_amount AS logic_amount, l.bill_date, l.gr_number, CASE WHEN l.order_no IS NULL THEN \'MISMATCH\' ELSE \'MATCHED\' END AS match_status FROM omni_base o LEFT JOIN logic_deduped l ON TRIM(o.invoice_no) = TRIM(l.order_no) ORDER BY o.created_at_ist ASC;",
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
            