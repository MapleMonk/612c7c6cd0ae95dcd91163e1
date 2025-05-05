{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.funnel_metrics_source_medium as select sessionsource, sessionmedium, PARSE_DATE(\'%Y%m%d\', CAST(date AS STRING)) date, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(transactions) transactions from maplemonk.suroskie_ga4_marketing_channel_funnel_metrics group by 1,2,3 ; create or replace table maplemonk.funnel_metrics_overall as select PARSE_DATE(\'%Y%m%d\', CAST(date AS STRING)) date, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(transactions) transactions from maplemonk.suroskie_ga4_overall_funnel_by_date group by 1 ; create or replace table maplemonk.funnel_metrics_source_medium_campaign as select sessionsource, sessionmedium, sessioncampaignname, PARSE_DATE(\'%Y%m%d\', CAST(date AS STRING)) date, sum(sessions) sessions, sum(addtocarts) addtocarts, sum(checkouts) checkouts, sum(transactions) transactions from `MapleMonk.suroskie_ga4_funnel_by_channel_campaign` group by 1,2,3,4 ;",
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
            