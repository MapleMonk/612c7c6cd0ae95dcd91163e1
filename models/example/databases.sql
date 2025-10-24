{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table andamen_db.maplemonk.andamen_funnel_by_date as select to_date(date, \'yyyymmdd\') date, sessions, addtocarts, checkouts, transactions, totalusers, engagedsessions, userengagementduration, Orders from andamen_db.maplemonk.ga4_andamen_funnel_by_date ga left join ( select order_date, count(distinct reference_code) Orders from andamen_db.maplemonk.andamen_db_sales_consolidated where lower(marketplace) like \'%shopify%\' group by 1 )sc on sc.order_date = to_date(date, \'yyyymmdd\') ; create or replace table andamen_db.maplemonk.andamen_funnel_by_source_medium as select to_date(date, \'yyyymmdd\') date, upper(sessionSource) sessionSource, upper(sessionmedium) sessionmedium, upper(coalesce(final_channel, \'NOT MAPPED\')) finalchannel, upper(coalesce(final_source, \'NOT MAPPED\')) finalsource, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(transactions) transactions, sum(totalusers) totalusers, sum(engagedsessions) engagedsessions, sum(userengagementduration) userengagementduration from andamen_db.maplemonk.ga4_andamen_funnel_by_source_medium a left join (select * from (select ga_source, ga_medium,ga_sourcemedium, final_channel, final_source, row_number() over (partition by lower(ga_sourcemedium) order by lower(ga_sourcemedium)) rw from andamen_db.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(a.sessionSource) = lower(GCM.ga_source) and lower(a.sessionmedium) = lower(GCM.ga_medium) group by 1,2,3,4,5 ;",
                    "transaction": true
                }
            ) }}
            with sample_data as (

                select * from andamen_db.information_schema.databases
            ),
            
            final as (
                select * from sample_data
            )
            select * from final
            