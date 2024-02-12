{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_HAPTIK_FACTITEMS as select \'HAPTIK\' DATA_SOURCE ,\'CHAT\' COMMUNICATION_TYPE ,Upper(DEVICE_PLATFORM) PLATFORM ,NULL STATUS ,conversation_identifier ,User_ID ,right(regexp_replace((parse_json(USER_DETAILS_CUSTOM_DATA):\"mobile_no\"),\'\"[^a-zA-Z0-9]+\'),10) Customer_number ,upper(replace(parse_json(USER_DETAILS_CUSTOM_DATA):\"name\",\'\"\',\'\')) Customer_Name ,upper(agent_name) AGENT_NAME ,SPLIT_PART(Upper(DISPLAY_NAME), \' -> \', -1) AGENT_DISPLAY_NAME ,Upper(AGENT_NAME.FINAL_AGENT_NAME) FINAL_AGENT_NAME ,(try_cast(SPLIT_PART(first_response_time, \':\', 1) as integer) * 3600 + try_cast(SPLIT_PART(first_response_time, \':\', 2) as INTEGER) * 60 + try_cast(SPLIT_PART(first_response_time, \':\', 3) as integer)) first_response_time ,try_to_timestamp(CHAT_INITIATION_TIMESTAMP) START_TIME ,try_to_timestamp(CHAT_INITIATION_TIMESTAMP) FINAL_START_TIME ,try_to_timestamp(CHAT_HANDOVER_TIME) END_TIME ,datediff(second,try_to_timestamp(CHAT_INITIATION_TIMESTAMP),try_to_timestamp(CHAT_HANDOVER_TIME)) TOTAL_CALCULATED_RESOLUTION_TIME ,(SPLIT_PART(AGENT_RESOLUTION_TIME_WITHOUT_QUEUE_AND_WAIT_TIME, \':\', 1)::integer * 3600 + SPLIT_PART(AGENT_RESOLUTION_TIME_WITHOUT_QUEUE_AND_WAIT_TIME, \':\', 2)::INTEGER * 60 + SPLIT_PART(AGENT_RESOLUTION_TIME_WITHOUT_QUEUE_AND_WAIT_TIME, \':\', 3)::integer) as RESOLUTION_WO_QUEUE ,AGENT_RESOLUTION_TIME_WITHOUT_QUEUE_AND_WAIT_TIME ,COMPLETED_BY ,upper(CHAT_REASSIGNED) REASSIGNED_FLAG ,QUEUE_TIME ,(case when lower(USER_RATING) like any (\'%no user%\',\'none\') then null else USER_RATING end)::integer USER_RATING ,USER_COMMENT ,Upper(CLOSING_CATEGORY) CLOSING_CATEGORY ,Upper(CLOSING_SUB_CATEGORY) CLOSING_SUB_CATEGORY ,NULL AS REOPEN ,_ab_source_file_url from sleepycat_db.maplemonk.sleepycat_s3_haptik HP left join (select * from (select name ,upper(mapped_name) Final_Agent_Name ,row_number() over (partition by lower(name) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.CE_TEAM_NAME_MAPPING ) where rw=1 ) AGENT_NAME on lower(SPLIT_PART(Upper(HP.DISPLAY_NAME), \' -> \', -1)) = lower(AGENT_NAME.name) where lower(display_name) not in (\'message_delivery_buzzo\');",
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
                        