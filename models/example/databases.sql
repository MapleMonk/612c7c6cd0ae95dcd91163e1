{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Search_Result as with audience_serch_result as (select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, sum(CONVERSIONS) as Conversions from snitch_db.maplemonk.APP_GA4_AUDIENCE_SEARCH group by 1,3,5,6 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, sum(CONVERSIONS) as Conversions from snitch_db.maplemonk.WEB__AUDIANCE__SEARCH_RESULTS group by 1,3,5,6) select * from audience_serch_result",
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
                        