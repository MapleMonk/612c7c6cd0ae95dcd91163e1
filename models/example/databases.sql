{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.Search_Result as ( select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term from snitch_db.maplemonk.app_ga4_audience_search where EVENTNAME = \'search\' and audiencename = \'Search\' and searchterm != \' \' group by 1,2,4 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term from snitch_db.maplemonk.web_search where EVENTNAME = \'view_search_results\' and audiencename = \'Search\' and searchterm != \' \' group by 1,2,4 UNION SELECT TO_DATE(DATE, \'YYYYMMDD\') AS GA_DATE, \'App_2_0_IOS\' as TYPE, sum(eventcount) as Event_Count, searchterm as Search_Term FROM snitch_db.maplemonk.search_app2_0_eventname_eventcount where lower(operatingsystem) = \'ios\' and lower(eventname) = \'search\' and searchterm != \' \' GROUP BY 1,2,4 union SELECT TO_DATE(DATE, \'YYYYMMDD\') AS GA_DATE, \'App_2_0_Android\' as TYPE, sum(eventcount) as Event_Count, searchterm as Search_Term FROM snitch_db.maplemonk.search_app2_0_eventname_eventcount where lower(operatingsystem) = \'android\' and lower(eventname) = \'search\' and searchterm != \' \' GROUP BY 1,2,4 );",
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
            