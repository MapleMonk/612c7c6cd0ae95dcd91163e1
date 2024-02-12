{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CE_REPORT_CONSOLIDATED as Select DATA_SOURCE ,COMMUNICATION_TYPE ,PLATFORM ,STATUS ,CONVERSATION_IDENTIFIER ID ,USER_ID ,CUSTOMER_NUMBER ,CUSTOMER_NAME ,AGENT_NAME ,AGENT_DISPLAY_NAME ,FINAL_AGENT_NAME ,FIRST_RESPONSE_TIME ,START_TIME ,FINAL_START_TIME ,END_TIME ,TOTAL_CALCULATED_RESOLUTION_TIME ,RESOLUTION_WO_QUEUE ,AGENT_RESOLUTION_TIME_WITHOUT_QUEUE_AND_WAIT_TIME ,COMPLETED_BY ,REASSIGNED_FLAG ,QUEUE_TIME ,USER_RATING ,USER_COMMENT ,CLOSING_CATEGORY ,CLOSING_SUB_CATEGORY ,REOPEN from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_HAPTIK_FACTITEMS union all Select DATA_SOURCE ,COMMUNICATION_TYPE ,PLATFORM ,STATUS ,CALL_ID ID ,USER_ID ,CUSTOMER_NUMBER ,CUSTOMER_NAME ,AGENT_NAME ,AGENT_DISPLAY_NAME ,FINAL_AGENT_NAME ,TIME_TO_ANSWER ,CALL_START_TIME ,FINAL_START_TIME ,CALL_END_TIME ,TOTAL_CALCULATED_RESOLUTION_TIME ,RESOLUTION_WO_QUEUE ,DURATION ,HANGUP_BY ,REASSIGNED_FLAG ,HOLD_TIME ,USER_RATING ,USER_COMMENT ,CLOSING_CATEGORY ,CLOSING_SUB_CATEGORY ,REOPEN from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_OZONOTEL_FACTITEMS union all Select DATA_SOURCE ,COMMUNICATION_TYPE ,LANDING_FOLDER ,SLA_STATUS ,TICKET_NO ID ,USER_ID ,CUSTOMER_NUMBER ,CUSTOMER_NAME ,AGENT_ID ,AGENT_DISPLAY_NAME ,FINAL_AGENT_NAME ,FIRST_RESPONSE_SLA_WORKING_HOURS ,START_TIME ,FINAL_START_TIME ,END_TIME ,TOTAL_CALCULATED_RESOLUTION_TIME ,RESOLUTION_WO_QUEUE ,RESOLUTION_TIME ,COMPLETED_BY ,REASSIGNED_FLAG ,HOLD_TIME ,CUSTOMER_RATING ,COMMENTS ,SUB_CATEGORY_1 ,SUB_CATEGORY_2 ,REOPEN from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_KAPTURE_CREATEDATE_FACTITEMS;",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from SLEEPYCAT_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        