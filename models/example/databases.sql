{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_OZONOTEL_FACTITEMS as select \'OZONOTEL\' DATA_SOURCE ,\'CALLS\' COMMUNICATION_TYPE ,Upper(\"Call Type\") PLATFORM ,upper(case when \"Dialed Number\"=\'0\' then \'Missed\' else \'Answered\' end) STATUS ,\"Call ID\" CALL_ID ,NULL User_ID ,right(regexp_replace(\"Caller No\", \'[^a-zA-Z0-9]+\'),10) Customer_number ,NULL Customer_Name ,upper(\"Agent ID\") AGENT_NAME ,SPLIT_PART(Upper(AGENT), \' -> \', -1) AGENT_DISPLAY_NAME ,upper(AGENT_NAME.FINAL_AGENT_NAME) FINAL_AGENT_NAME ,(SPLIT_PART(\"Time to Answer\", \':\', 1)::integer * 3600 + SPLIT_PART(\"Time to Answer\", \':\', 2)::INTEGER * 60 + SPLIT_PART(\"Time to Answer\", \':\', 3)::integer) TIME_TO_ANSWER ,try_to_timestamp(concat(final_call_date,\' \',\"Start Time\"),\'DD-MM-YYYY HH:MI:SS\') CALL_START_TIME ,try_to_timestamp(concat(final_call_date,\' \',\"Start Time\"),\'DD-MM-YYYY HH:MI:SS\') FINAL_START_TIME ,try_to_timestamp(concat(final_call_date,\' \',\"End Time\"),\'DD-MM-YYYY HH:MI:SS\') CALL_END_TIME ,datediff(second,FINAL_START_TIME,CALL_END_TIME) TOTAL_CALCULATED_RESOLUTION_TIME ,(SPLIT_PART(DURATION, \':\', 1)::integer * 3600 + SPLIT_PART(DURATION, \':\', 2)::INTEGER * 60 + SPLIT_PART(DURATION, \':\', 3)::integer) as RESOLUTION_WO_QUEUE ,DURATION ,\"Hangup By\" HANGUP_BY ,case when \"Dialed Number\" like \'%->%\' then \'YES\' else \'NO\' end REASSIGNED_FLAG ,\"Hold Time\" HOLD_TIME ,NULL::integer USER_RATING ,COMMENTS USER_COMMENT ,Upper(SKILL) CLOSING_CATEGORY ,Upper(DISPOSITION) CLOSING_SUB_CATEGORY ,NULL as REOPEN from sleepycat_db.maplemonk.sleepycat_gs_ozonotel_data OZ left join (select * from (select name ,upper(mapped_name) Final_Agent_Name ,row_number() over (partition by lower(name) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.CE_TEAM_NAME_MAPPING ) where rw=1 ) AGENT_NAME on lower(SPLIT_PART(Upper(OZ.AGENT), \' -> \', -1)) = lower(AGENT_NAME.name) ;",
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
                        