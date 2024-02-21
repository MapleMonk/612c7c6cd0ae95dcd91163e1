{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table SLEEPYCAT_DB.MAPLEMONK.SLEEPYCAT_DB_KAPTURE_CREATEDATE_FACTITEMS as select \'Kapture\' DATA_SOURCE ,\'Emails\' COMMUNICATION_TYPE ,Landing_Folder ,SLA_Status ,coalesce(TICKET__NO,\'NA\') Ticket_NO ,null as USER_ID ,null as CUSTOMER_NUMBER ,upper(CUSTOMER__NAME) CUSTOMER_NAME ,upper(ASSIGNED__TO) AGENT_ID ,upper(ASSIGNED__TO) AGENT_DISPLAY_NAME ,Upper(AGENT_NAME.Final_Agent_Name) FINAL_AGENT_NAME ,try_cast(SPLIT_PART(FIRST_RESPONSE_SLA_WORKING_HOURS, \':\', 1) as integer) * 3600 + try_cast(SPLIT_PART(FIRST_RESPONSE_SLA_WORKING_HOURS, \':\', 2) as integer) * 60 + try_cast(SPLIT_PART(FIRST_RESPONSE_SLA_WORKING_HOURS, \':\', 3) as integer) FIRST_RESPONSE_SLA_WORKING_HOURS ,try_to_timestamp(concat(FINAL_CREATED_DATE,\' \',CREATED__TIME),\'MM/DD/YYYY HH:MI:SS\') START_TIME ,case when try_cast(SPLIT_PART(CREATED__TIME, \':\', 1) as integer) >= 19 then dateadd(hour, 10, dateadd(day, 1, to_date(start_time))) else START_TIME end as FINAL_START_TIME ,try_to_timestamp(concat(FINAL_RESOLVED_DATE,\' \',RESOLVED_TIME),\'MM/DD/YYYY HH:MI:SS\') END_TIME ,datediff(second,FINAL_START_TIME,END_TIME) TOTAL_CALCULATED_RESOLUTION_TIME ,case when RESOLUTION_SLA =\'0:00:00\' then TOTAL_CALCULATED_RESOLUTION_TIME else try_cast(SPLIT_PART(RESOLUTION_SLA, \':\', 1) as integer) * 3600 + try_cast(SPLIT_PART(RESOLUTION_SLA, \':\', 2) as integer) * 60 + try_cast(SPLIT_PART(RESOLUTION_SLA, \':\', 3) as integer) end as RESOLUTION_WO_QUEUE ,RESOLUTION_SLA RESOLUTION_TIME ,null as COMPLETED_BY ,case when PREVIOUS_ASSIGNED is not null then \'YES\' else \'NO\' end REASSIGNED_FLAG ,null as HOLD_TIME ,case when CUSTOMER_RATING = \'0\' then null else CUSTOMER_RATING::integer end CUSTOMER_RATING ,REMARKS as COMMENTS ,SUB_CATEGORY_1 ,SUB_CATEGORY_2 ,reopen_count REOPEN from sleepycat_db.maplemonk.SLEEPYCAT_GS_KAPTURE_BY_CREATED KC left join (select * from (select name ,upper(mapped_name) Final_Agent_Name ,row_number() over (partition by lower(name) order by 1) rw from SLEEPYCAT_DB.MAPLEMONK.CE_TEAM_NAME_MAPPING ) where rw=1 ) AGENT_NAME on lower(KC.ASSIGNED__TO) = lower(AGENT_NAME.name) where lower(assigned__to) in (\'manisha\',\'jatin gupta\',\'muskan kalra\',\'akansha singh\',\'suresh abraham\',\'kamini rajput\') and lower(status) in (\'complete\', \'pending\') and lower(landing_folder) in (\'website\',\'info email\', \'flipkart\',\'amazon\') and not(FIRST_RESPONSE_SLA_WORKING_HOURS = \'0:00:00\' and RESOLVED__DATE is null) and not(FIRST_RESPONSE_SLA_WORKING_HOURS = \'0:00:00\' and try_cast(SPLIT_PART(RESOLUTION_SLA, \':\', 1) as integer) > 2) ;",
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
                        