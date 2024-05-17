{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table snitch_db.maplemonk.Search_Result as ( select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, from snitch_db.maplemonk.app_ga4_audience_search group by 1,2,3,5,6 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, from snitch_db.maplemonk.web__audiance__search_results group by 1,2,3,5,6 ); create or replace table snitch_db.maplemonk.Search_Result_count as ( with audience_serch_result as ( select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, from snitch_db.maplemonk.app_ga4_audience_search group by 1,2,3,5,6 UNION select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, EVENTNAME as Event_Name, sum(EVENTCOUNT) as Event_Count, searchterm as Search_Term, audiencename as Audience_Name, from snitch_db.maplemonk.web__audiance__search_results group by 1,2,3,5,6 ), total_search_count as ( select ga_date, sum(Event_Count) as total_search from audience_serch_result where lower(AUDIENCE_NAME) = \'search\' group by 1 ), dod_web_app_search_count as ( select ga_date, type, sum(Event_Count) as search_count from audience_serch_result where lower(AUDIENCE_NAME) = \'search\' group by 1,2 ) select a.ga_date, a.type, a.search_count, b.total_search, (a.search_count/b.total_search)*100 as count_percentage from dod_web_app_search_count a left join total_search_count b on a.ga_date = b.ga_date ); create or replace table snitch_db.maplemonk.Search_Result_conversion as ( with sales_revenue as ( select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'APP\' as TYPE, sum(purchaserevenue) as sales_revenue, sum(totalpurchasers) as sales_qty from snitch_db.maplemonk.app_ga4_audience_search where audiencename = \'Search\' group by 1,2 union select TO_DATE(DATE,\'YYYYMMDD\') AS GA_DATE, \'WEB\' as TYPE, sum(purchaserevenue) as sales_revenue, sum(totalpurchasers) as sales_qty, from snitch_db.maplemonk.web__audiance__search_results where audiencename = \'Search\' group by 1,2 ) select a.*, b.search_count, (a.sales_qty/b.search_count)*100 as conversion_percent from sales_revenue a left join snitch_db.maplemonk.Search_Result_count b on a.ga_date = b.ga_date and a.type = b.type )",
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
                        