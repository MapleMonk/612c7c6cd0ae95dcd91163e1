{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.symphony_ga4_hourly_user_analysis as select CAST(PARSE_Datetime(\'%Y%m%d%H\', CAST(dateHour AS STRING)) as date) as date_and_time, FORMAT_DATETIME(\'%l %p\', CAST(PARSE_DATETIME(\'%Y%m%d%H\', CAST(dateHour AS STRING)) AS DATETIME)) AS hour, cast(newUsers as int64) NewUsers, cast(sessions as int64) sessions, cast(checkouts as int64) checkouts, cast(addToCarts as int64) addtocarts, cast(totalUsers as int64) totalUsers, cast(transactions as int64) transactions, cast(engagedSessions as int64) engagedSessions, cast(screenPageViews as int64) screenPageViews, from maplemonk.symphony_GA4_sessionby_datetime;",
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
            