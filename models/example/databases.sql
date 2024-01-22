{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.s3_sc_csat AS SELECT NAME, RATING, COMMENT, agent_name, level_1_tags, TO_DATE(created_at,\'DD-MM-YYYY HH24:MI:SS\') as Created_Date, feedback_message FROM SNITCH_DB.maplemonk.S3_CUSTOMER_SUPPORT_CSAT; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.S3_SC_OUTBOUND_1 AS SELECT TO_DATE(NULLIF(TRIM(ASSIGNED_DATE), \'\'),\'DD-MM-YYYY\') AS ASSIGNED_DATE, TO_DATE(NULLIF(TRIM(FRT_DATE), \'\'),\'DD-MM-YYYY\') AS FRT_DATE, TO_DATE(NULLIF(TRIM(RESOLUTION_DATE), \'\'),\'DD-MM-YYYY\') AS RESOLUTION_DATE, Ticket_Status, TICKET_ID, Agent_Name, INBOX_NAME, \"Is_Resolved?\", Is_Outside_Working_Hours, Latest_Assignee_Check, agent_frt_at, NULLIF(NULLIF(TRIM(FRT), \'\'), \'\')::INT AS FRT, NULLIF(NULLIF(TRIM(Average_Wait_Time), \'\'), \'\')::INT AS Average_Wait_Time, NULLIF(NULLIF(TRIM(Resolution_Time), \'\'), \'\')::INT AS Resolution_Time FROM SNITCH_DB.MAPLEMONK.S3_CUSTOMER_SUPPORT_OUTBOUND; CREATE OR REPLACE TABLE snitch_db.maplemonk.Order_Over_Issues_real AS SELECT orders.ORDER_DATE, orders.daily_order_count, COALESCE(ticket_counts.daily_ticket_count, 0) AS daily_ticket_count, COALESCE((COALESCE(ticket_counts.daily_ticket_count, 0) * 100.0) / NULLIF(orders.daily_order_count, 0), 0) AS ticket_percentage FROM ( SELECT ORDER_DATE, COUNT(DISTINCT ORDER_NAME) AS daily_order_count FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE ORDER_DATE >= \'2023-11-01\' AND ORDER_DATE <= CURRENT_DATE AND MARKETPLACE_MAPPED = \'SHOPIFY\' GROUP BY ORDER_DATE ) AS orders LEFT JOIN ( SELECT TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') AS Converted_Created_At, COUNT(DISTINCT _AIRBYTE_DATA:\"Ticket_ID\"::string) AS daily_ticket_count FROM snitch_db.maplemonk._airbyte_raw_s3_customer_support_tags WHERE TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') >= \'2023-11-01\' AND _AIRBYTE_DATA:\"Level_1_Tags_(Main)\"::string NOT IN (\'post_share\',\'collaboration\',\'collaboration,others\',\'sample-tag\',\'franchise\',\'post_share,others\',\'others,post-share\',\'offline_store_query\') GROUP BY TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') ) AS ticket_counts ON orders.ORDER_DATE = ticket_counts.Converted_Created_At ORDER BY orders.ORDER_DATE DESC; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.s3_sc_tags AS SELECT _AIRBYTE_DATA:\"Ticket_ID\"::string AS Ticket_ID, _AIRBYTE_DATA:\"Inbox_Name\"::string AS Inbox_Name, TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') AS Converted_Created_At, _AIRBYTE_DATA:\"Ticket_Status\"::string AS Ticket_Status, _AIRBYTE_DATA:\"Level_1_Tags(Main)\"::string AS Level_Tags_(Main) FROM snitch_db.maplemonk._airbyte_raw_s3_customer_support_tags; CREATE OR REPLACE TABLE snitch_db.maplemonk.Order_Over_Issues AS SELECT orders.ORDER_DATE, orders.daily_order_count, COALESCE(ticket_counts.daily_ticket_count, 0) AS daily_ticket_count, COALESCE((COALESCE(ticket_counts.daily_ticket_count, 0) * 100.0) / NULLIF(orders.daily_order_count, 0), 0) AS ticket_percentage FROM ( SELECT ORDER_DATE, COUNT(DISTINCT ORDER_NAME) AS daily_order_count FROM snitch_db.maplemonk.unicommerce_fact_items_snitch WHERE ORDER_DATE >= \'2023-11-01\' AND ORDER_DATE <= CURRENT_DATE AND MARKETPLACE_MAPPED = \'SHOPIFY\' GROUP BY ORDER_DATE ) AS orders LEFT JOIN ( SELECT TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') AS Converted_Created_At, COUNT(DISTINCT _AIRBYTE_DATA:\"Ticket_ID\"::string) AS daily_ticket_count FROM snitch_db.maplemonk._airbyte_raw_s3_customer_support_tags WHERE TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') >= \'2023-11-01\' GROUP BY TO_DATE(_AIRBYTE_DATA:\"Created_At\"::string, \'DD-MM-YYYY HH24:MI:SS\') ) AS ticket_counts ON orders.ORDER_DATE = ticket_counts.Converted_Created_At ORDER BY orders.ORDER_DATE DESC; CREATE OR REPLACE TABLE SNITCH_DB.MAPLEMONK.s3_sc_inbound AS SELECT CASE WHEN LENGTH(TRIM(to_date)) = 0 THEN NULL WHEN POSITION(\' \' IN to_date) > 0 THEN TO_DATE(to_date, \'DD-MM-YYYY HH24:MI\') ELSE TO_DATE(to_date, \'DD-MM-YYYY\') END as Created_Date, Agent_Name, \"Adherence_%\", MISSED_CALLS, CALLS_OFFERED, CALLS_ANSWERED, CASE WHEN \"Missed_Calls_%\" = \'\' THEN NULL ELSE REPLACE(\"Missed_Calls_%\", \'%\', \'\')::FLOAT END as Missed_per, CASE WHEN \"Calls_Answered_%\" = \'\' THEN NULL ELSE REPLACE(\"Calls_Answered_%\", \'%\', \'\')::FLOAT END as Answered_per, PERFORMANCE_SCORE, ECPECTED_WORK_TIME, \"Total_Talk_Time_(Hrs)\", \"Total_Break_Time_(Hrs)\", \"Total_Logged_Time_(Hrs)\", \"Avg_Handling_Time_(Mins)\" FROM SNITCH_DB.maplemonk.S3_customer_support_inbound;",
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
                        