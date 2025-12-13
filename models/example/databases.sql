{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE spf_b2c_final AS SELECT *, CASE WHEN lower(AWB) LIKE \'%prz%\' THEN \'Prozo\' WHEN AWB LIKE \'2222%\' THEN \'Delhivery\' WHEN lower(AWB) LIKE \'%sni%\' THEN \'Ekart\' ELSE \'Other\' END AS courier, TO_CHAR( COALESCE( TRY_TO_DATE(date, \'DD-MM-YYYY\'), TRY_TO_DATE(date, \'DD/MM/YYYY\'), TRY_TO_DATE(date, \'DD Mon YYYY\'), TRY_TO_DATE(date, \'DD Month YYYY\'), TRY_TO_DATE(date, \'DD Mon\'), TRY_TO_DATE(date, \'DD Month\') ), \'YYYY-MM-DD\' ) AS date_formatted, CASE WHEN status ILIKE \'%Claim Denied%\' THEN \'Rejected\' WHEN lower(status) = \'rejected\' THEN \'Rejected\' WHEN lower(status) = \'dispute not accepted\' THEN \'Rejected\' WHEN lower(status) IN ( \'dispute accepted\', \'claim accepted\', \'approved\', \'approved \', \' partial dispute accepted\' ) THEN \'Accepted\' WHEN status ILIKE \'%Dispute Accepted%\' THEN \'Accepted\' WHEN status ILIKE \'%Claim Accepted%\' THEN \'Accepted\' WHEN status ILIKE \'%Partial Dispute Accepted%\' THEN \'Accepted\' WHEN lower(status) IN ( \'pending\', \'re-raised case\', \'open\', \'under investigation\', \'pending from snitch team\' ) THEN \'Open\' WHEN status IS NULL OR trim(status) = \'\' THEN \'Open\' ELSE \'Open\' END AS final_status, CASE WHEN reason ILIKE \'%awb does not belong%\' OR reason ILIKE \'%no issue%\' OR reason ILIKE \'%original photo%\' OR reason ILIKE \'%tid not matching%\' OR reason ILIKE \'%snitch logo%\' OR reason IS NULL THEN \'Invalid claim\' WHEN reason ILIKE \'%non qc%\' OR reason ILIKE \'%catalog image%\' OR reason ILIKE \'%catalogue image%\' OR reason ILIKE \'%qc%\' OR reason ILIKE \'%no parameter%\' THEN \'Non qc\' WHEN reason ILIKE \'%negative%\' OR reason ILIKE \'%pod%\' OR reason ILIKE \'%tamper%\' OR reason ILIKE \'%empty shipment%\' THEN \'Pod\' WHEN reason ILIKE \'%video%\' OR reason ILIKE \'%footage%\' OR reason ILIKE \'%unboxing%\' OR reason ILIKE \'%not visible%\' THEN \'footage issue\' WHEN reason ILIKE \'%out tat%\' OR reason ILIKE \'tat exceeded%\' OR (reason ILIKE \'%delivered on%\' AND reason ILIKE \'%raised on%\') THEN \'out tat\' WHEN reason ILIKE \'%approved%\' THEN \'approved\' WHEN trim(lower(reason)) IN ( \'checking with courier\', \'claim denied earlier but re-opedned the case\', \'under investigation\' ) THEN \'open\' ELSE \'Invalid claim\' END AS final_reason FROM spf_b2c;",
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
            