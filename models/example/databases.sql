{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table quick-cogency-477406-p1.maplemonk.funnel_by_date as select PARSE_DATE(\'%Y%m%d\', date) DATE, sessions, checkouts, addtocarts, b.orders from quick-cogency-477406-p1.maplemonk.ga4_krvvy_funnelbydate a left join (select cast(order_timestamp as date) order_Date, count(distinct ordeR_name) orders from quick-cogency-477406-p1.maplemonk.quick_cogency_477406_p1_SHOPIFY_FACT_ITEMS group by 1) b on PARSE_DATE(\'%Y%m%d\', a.date) = b.ordeR_date ; create or replace table quick-cogency-477406-p1.maplemonk.funnel_by_marketing_channel as select PARSE_DATE(\'%Y%m%d\', date) DATE, sessionsource, sessionmedium, sessions, checkouts, addtocarts, transactions, b.orders from quick-cogency-477406-p1.maplemonk.ga4_krvvy_marketing_channel_funnel_metrics a left join (select cast(order_timestamp as date) order_Date,final_utm_source, final_utm_channel, count(distinct ordeR_name) orders from quick-cogency-477406-p1.maplemonk.quick_cogency_477406_p1_SHOPIFY_FACT_ITEMS group by 1,2,3) b on PARSE_DATE(\'%Y%m%d\', a.date) = b.ordeR_date and lower(a.sessionsource) = lower(b.final_utm_source) and lower(a.sessionmedium) = lower(b.final_utm_channel) ; create or replace table quick-cogency-477406-p1.maplemonk.funnel_by_landing_page as select PARSE_DATE(\'%Y%m%d\', date) DATE, landingpage, newusers, totalusers, sessions, checkouts, addtocarts, transactions from quick-cogency-477406-p1.maplemonk.ga4_krvvy_landingpagefunnel a ;",
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
            