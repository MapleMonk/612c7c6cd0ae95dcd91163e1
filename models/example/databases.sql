{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table peesafe_db.maplemonk.ga4_landing_page_funnel_metrics as select to_date(to_varchar(date), \'YYYYMMDD\') as date, trim(landingpage) as landing_page, trim(sessionsourcemedium) as session_source_medium, \'Pee Safe\' as brand, sum(newusers) as new_users, sum(sessions) as sessions, sum(checkouts) as checkouts, sum(totalusers) as total_users, sum(transactions) as transactions, sum(addtocarts) as add_to_carts, sum(engagedsessions) as engaged_sessions, sum(screenpageviews) as screenpage_views from PEESAFE_DB.MAPLEMONK.PEE_SAFE_GA4_LANDING_PAGE_FUNNEL_METRICS group by 1,2,3 union all select to_date(to_varchar(date), \'YYYYMMDD\') as date, trim(landingpage) as landing_page, trim(sessionsourcemedium) as session_source_medium, \'Furr\' as brand, sum(newusers) as new_users, sum(sessions) as sessions, sum(checkouts) as checkouts, sum(totalusers) as total_users, sum(transactions) as transactions, sum(addtocarts) as add_to_carts, sum(engagedsessions) as engaged_sessions, sum(screenpageviews) as screenpage_views from PEESAFE_DB.MAPLEMONK.FURR_GA4_LANDING_PAGE_FUNNEL_METRICS group by 1,2,3;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from PEESAFE_DB.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            