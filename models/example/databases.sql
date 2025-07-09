{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table andamen_db.maplemonk.andamen_funnel_by_date as select to_date(date, \'yyyymmdd\') date, sessions, addtocarts, checkouts, transactions, totalusers, engagedsessions, userengagementduration from andamen_db.maplemonk.ga4_andamen_funnel_by_date ; create or replace table andamen_db.maplemonk.andamen_funnel_by_source_medium as select to_date(date, \'yyyymmdd\') date, upper(coalesce(final_channel, sessionmedium)) finalchannel, upper(coalesce(final_source, sessionsource)) finalsource, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(transactions) transactions, sum(totalusers) totalusers, sum(engagedsessions) engagedsessions, sum(userengagementduration) userengagementduration from andamen_db.maplemonk.ga4_andamen_funnel_by_source_medium a left join (select * from (select ga_source, ga_medium,ga_sourcemedium, final_channel, final_source, row_number() over (partition by lower(ga_sourcemedium) order by lower(ga_sourcemedium)) rw from andamen_db.MAPLEMONK.GA_CHANNEL_MAPPING) where rw=1) GCM on lower(a.sessionSource) = lower(GCM.ga_source) and lower(a.sessionmedium) = lower(GCM.ga_medium) group by 1,2,3 ;",
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
            