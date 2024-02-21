{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CE_DAILY_SUMMARY as with created_tickets as ( select to_date(FINAL_START_TIME) Date ,case when hour(FINAL_START_TIME) < 11 then \'10 & Earlier\' when hour(FINAL_START_TIME) > 19 then \'After 7 PM\' else hour(FINAL_START_TIME)::varchar end HOUR_OF_THE_DAY ,final_agent_name ,count(ID) TOTAL_TICKETS_OFFERED ,count(case when lower(communication_type) like \'%chat%\' then ID end) TOTAL_CHATS_CREATED ,count(case when lower(communication_type) like \'%chat%\' and lower(final_agent_name) like \'gogo\' then ID end) TOTAL_CHATS_BOT ,count(case when lower(communication_type) like \'%chat%\' and not(lower(final_agent_name) like \'gogo\') then ID end) TOTAL_CHATS_AGENTS ,count(case when lower(communication_type) like \'%calls%\' then ID end) TOTAL_CALLS_RECEIVED ,count(case when lower(communication_type) like \'%email%\' then ID end) TOTAL_EMAIL_TICKETS_CREATED ,avg(case when lower(communication_type) like \'%chat%\' then first_response_time end) AVG_FIRST_RESPONSE_TIME_CHAT ,count(case when lower(communication_type) like \'%chat%\' and first_response_time <=30 then ID end) RESPONDED_WITHIN_30SEC_CHAT ,avg(case when lower(communication_type) like \'%chat%\' then resolution_wo_queue end) AVG_RESOLUTION_TIME_CHAT ,count(case when lower(communication_type) like \'%chat%\' and resolution_wo_queue <=900 then ID end) RESOLVED_WITHIN_15MIN_CHAT ,count(case when lower(communication_type) like \'%email%\' and first_response_time <=2*60*60 then ID end) RESPONDED_WITHIN_2HRS_EMAIL ,count(case when lower(communication_type) like \'%calls%\' and lower(status) like \'answered\' then ID end) ANSWERED_CALLS ,count(case when lower(communication_type) like \'%email%\' and start_time::date = end_time::date then ID end) EMAIL_TICKETS_FTR from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CE_REPORT_CONSOLIDATED group by 1,2,3 ), resolved_tickets as ( select to_date(end_time) Date ,case when hour(end_time) < 11 then \'10 & Earlier\' when hour(end_time) > 19 then \'After 7 PM\' else hour(end_time)::varchar end HOUR_OF_THE_DAY ,final_agent_name ,count(case when lower(communication_type) like \'%email%\' then Ticket_NO end) TOTAL_EMAIL_TICKETS_RESOLVED ,count(case when lower(communication_type) like \'%email%\' and (RESOLUTION_WO_QUEUE/(60*60)) <= 48 then TICKET_NO end) TICKETS_RESOLVED_WITHIN_2_DAYS ,count(case when lower(communication_type) like \'%email%\' and reopen <> 0 then Ticket_NO end) EMAIL_TICKETS_REOPEN from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_KAPTURE_RESOLVEDDATE_FACTITEMS group by 1,2,3 ) select coalesce(A.Date, B.Date) Date ,coalesce(A.HOUR_OF_THE_DAY, B.HOUR_OF_THE_DAY) HOUR_OF_THE_DAY ,coalesce(A.FINAL_AGENT_NAME, B.FINAL_AGENT_NAME) FINAL_AGENT_NAME ,A.TOTAL_CHATS_CREATED ,A.TOTAL_CHATS_BOT ,A.TOTAL_CHATS_AGENTS ,A.TOTAL_CALLS_RECEIVED ,A.TOTAL_EMAIL_TICKETS_CREATED ,A.AVG_FIRST_RESPONSE_TIME_CHAT ,A.RESPONDED_WITHIN_30SEC_CHAT ,A.AVG_RESOLUTION_TIME_CHAT ,A.RESOLVED_WITHIN_15MIN_CHAT ,A.RESPONDED_WITHIN_2HRS_EMAIL ,A.EMAIL_TICKETS_FTR ,A.ANSWERED_CALLS ,B.TOTAL_EMAIL_TICKETS_RESOLVED ,B.TICKETS_RESOLVED_WITHIN_2_DAYS ,B.EMAIL_TICKETS_REOPEN from created_tickets A full outer join resolved_tickets B on A.Date = B.Date and A.HOUR_OF_THE_DAY = B.HOUR_OF_THE_DAY and lower(A.final_agent_name) = lower(B.final_agent_name);",
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
                        