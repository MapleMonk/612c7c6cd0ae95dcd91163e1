{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_CSAT_SUMMARY as Select CSAT.* ,AGENT_NAME.FINAL_AGENT_NAME from (select \'KAPTURE\' DATA_SOURCE ,ticketid TICKET_NO ,coalesce(taskid,\'NA\') ID ,Upper(AGENTNAME) AGENT_NAME ,case when lower(source) like \'%call%\' then \'CALL\' else \'EMAIL\' end as COMMUNICATION_TYPE ,try_to_date(SENTDATE, \'dd-mm-yy\') DATE ,RATING::integer USER_RATING ,REVIEW COMMENTS ,_ab_source_file_url SOURCE_URL ,NULL as CUSTOMER_PHONE ,NULL as CUSTOMER_NAME from sleepycat_db.maplemonk.sleepycat_s3_kapture_csat_ratings where rating is not null union all select \'NINJA\' DATA_SOURCE ,\"Ticket Id\" as TICKET_NO ,coalesce(\"Ticket Id\",\"Phone No\",\'NA\') ID ,upper(AGENT) as AGENTNAME ,upper(channel) COMMUNICATION_TYPE ,try_to_date(date, \'dd-mm-yyyy\') DATE ,ratings::integer USER_RATING ,\"Cx Comments\" COMMENTS ,\'GOOGLE SHEET\' Source_URL ,right(regexp_replace(\"Phone No\", \'[^a-zA-Z0-9]+\'),10) CUSTOMER_PHONE ,NULL CUSTOMER_NAME from sleepycat_db.maplemonk.sleepycat_ce_ninja_ratings where ratings is not null union all select \'HAPTIK\' DATA_SOURCE ,CONVERSATION_IDENTIFIER ticketid ,coalesce(CONVERSATION_IDENTIFIER,\'NA\') ID ,upper(AGENT_DISPLAY_NAME) AGENT_NAME ,\'CHAT\' COMMUNICATION_TYPE ,final_start_time::date DATE ,USER_RATING ,USER_COMMENT COMMENTS ,_ab_source_file_url ,CUSTOMER_NUMBER ,CUSTOMER_NAME from SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_HAPTIK_FACTITEMS where USER_RATING is not null ) CSAT left join (select * from (select name ,upper(mapped_name) Final_Agent_Name ,row_number() over (partition by lower(name) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.CE_TEAM_NAME_MAPPING ) where rw=1 ) AGENT_NAME on lower(CSAT.AGENT_NAME) = lower(AGENT_NAME.name);",
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
                        