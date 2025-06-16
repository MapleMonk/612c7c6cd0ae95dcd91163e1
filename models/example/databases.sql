{{ config(
            materialized='table',
                post_hook={
                    "sql": "CREATE OR REPLACE TABLE snitch_db.maplemonk.gs_JJ_BAU as select CASE WHEN timestamp LIKE \'%-%\' THEN timestamp::TIMESTAMP ELSE TO_TIMESTAMP(timestamp, \'DD/MM/YYYY HH24:MI:SS\') END AS created_timestamp, \"Email address\" as email, reason, \"Order ID\" as order_id, \"AWB No.\" as awb, \"RAN Number\" as RAN_NUMBER, \"Gift Card Reason\" as giftcard_reason, \"Email ID of Customer\" as customer_email, amount, \"Done By Agent\" as agent_name, status, \"Reason for Not Done\" as not_done_reason, \"Gift-Card\" as updated_time, concern from snitch_db.maplemonk.gs_JJ_BAU ; CREATE OR REPLACE TABLE snitch_db.maplemonk.gs_JJ_RVP as select CASE WHEN timestamp LIKE \'%-%\' THEN timestamp::TIMESTAMP ELSE TO_TIMESTAMP(timestamp, \'DD/MM/YYYY HH24:MI:SS\') END AS created_timestamp, \"Email address\" as email, \"AWB Number\" as awb, \"Courier Partner\" as courier_partner, \"Issue\", voc, comments, remark, \"Done By Agent\" as agent, status, not_done_reason, duplicate as updated_time from snitch_db.maplemonk.gs_JJ_RVP ; CREATE OR REPLACE TABLE snitch_db.maplemonk.gs_JJ_FWD as select CASE WHEN timestamp LIKE \'%-%\' THEN timestamp::TIMESTAMP ELSE TO_TIMESTAMP(timestamp, \'DD/MM/YYYY HH24:MI:SS\') END AS created_timestamp, \"Email address\" as email, awb, \"Courier partner\" as courier_partner, \"Issue\", voc, comments, required, \"Agent Name\" as agent, status, \"Not Done Reason\" as not_done_reason, duplicate as updated_time from snitch_db.maplemonk.gs_JJ_FWD",
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
            