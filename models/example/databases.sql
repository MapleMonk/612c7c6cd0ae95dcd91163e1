{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE spf_b2c_final AS SELECT *, CASE WHEN lower(AWB) LIKE \'%prz%\' THEN \'Prozo\' WHEN AWB LIKE \'2222%\' THEN \'Delhivery\' WHEN lower(AWB) LIKE \'%sni%\' THEN \'Ekart\' ELSE \'Other\' END AS courier, TO_CHAR( TO_DATE( REGEXP_REPLACE(date, \'[/-]\', \'-\'), \'DD-MM-YYYY\' ), \'YYYY-MM-DD\' ) AS date_formatted, CASE WHEN status ILIKE \'%Claim Denied%\' THEN \'Rejected\' WHEN lower(status) = \'rejected\' THEN \'Rejected\' WHEN lower(status) = \'dispute not accepted\' THEN \'Rejected\' WHEN lower(status) IN (\'dispute accepted\', \'claim accepted\', \'approved\', \'approved \', \' partial dispute accepted\') THEN \'Accepted\' WHEN status ILIKE \'%Dispute Accepted%\' THEN \'Accepted\' WHEN status ILIKE \'%Claim Accepted%\' THEN \'Accepted\' WHEN status ILIKE \'%Partial Dispute Accepted%\' THEN \'Accepted\' WHEN lower(status) IN (\'pending\', \'re-raised case\', \'open\', \'under investigation\', \'pending from snitch team\') THEN \'Open\' WHEN status IS NULL OR trim(status) = \'\' THEN \'Open\' ELSE \'Open\' END AS final_status, CASE WHEN reason ILIKE \'%out tat%\' THEN \'Out TAT\' WHEN reason ILIKE \'%negative%\' OR reason ILIKE \'%tamper%\' THEN \'POD Issue\' WHEN reason ILIKE \'%not visible%\' OR reason ILIKE \'%video%\' OR reason ILIKE \'%footage%\' THEN \'Footage Issue\' ELSE \'others\' END AS final_reason FROM spf_b2c;",
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
            