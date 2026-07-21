{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.website_day_wise_conversion_funnel as ( select eventName, \'WEBSITE\' as source, cast(PARSE_DATE(\'%Y%m%d\', date) as date) as date, sum(sessions) sessions from maplemonk.ga4_website_sessions__by__event group by 1,2,3 union all select eventName, \'APP\' as source, cast(PARSE_DATE(\'%Y%m%d\', date) as date) as date, sum(sessions) sessions from maplemonk.ga4_app_sessions_by__event group by 1,2,3 ) ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from maplemonk.INFORMATION_SCHEMA.TABLES
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            