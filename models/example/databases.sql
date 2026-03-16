{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table snitch_db.maplemonk.source_type_sessions as with source as ( select to_date(date,\'YYYYMMDD\') as date, case when lower(sessionsourcemedium) like \'%affiliate%\' or sessionsourcemedium = \'wishlink / wishlink.com\' then \'affiliate\' when lower(sessionsourcemedium) like \'%facebook%\' or lower(sessionsourcemedium) like \'%fb%\' or lower(sessionsourcemedium) like \'%ig%\' or sessionsourcemedium in (\'(not set) / paid\',\'(not set) / Prospect_Videos\') then \'facebook\' when lower(sessionsourcemedium) like \'%google%\' or sessionsourcemedium = \'alt-shop / brand-search\' then \'google\' when sessionsourcemedium in (\'(direct) / (none)\',\'google / organic\',\'website / (not set)\',\'(not set)\', \'(data not available)\',\'plp / (not set)\',\'cart_lucky_size / (not set)\',\'bing / organic\',\'(other)\', \'homepage / (not set)\',\'simpl / (not set)\',\'chatgpt.com / (not set)\',\'simpl / pi3_landing_page\', \'Simpl / SimplApp\',\'website / (none)\',\'perplexity / (not set)\',\'stylebot / (not set)\') or lower(sessionsourcemedium) like \'%organic%\' then \'organic\' when lower(sessionsourcemedium) like \'%sms%\' or lower(sessionsourcemedium) like \'%whatsapp%\' or lower(sessionsourcemedium) like \'%email%\' then \'retention\' when lower(sessionsourcemedium) like \'%referral%\' then \'referral\' end as source, \'web\' as type, sum(sessions) as session from snitch_db.maplemonk.ga_web2_sourcemedium group by 1,2,3 union all select to_date(date,\'YYYYMMDD\') as date, case when lower(sessionsourcemedium) like \'%affiliate%\' or sessionsourcemedium = \'wishlink / wishlink.com\' then \'affiliate\' when lower(sessionsourcemedium) like \'%facebook%\' or lower(sessionsourcemedium) like \'%fb%\' or lower(sessionsourcemedium) like \'%ig%\' or sessionsourcemedium = \'(not set) / paid\' then \'facebook\' when lower(sessionsourcemedium) like \'%google%\' or sessionsourcemedium = \'alt-shop / brand-search\' then \'google\' when sessionsourcemedium in (\'(direct) / (none)\',\'(not set)\',\'apps.instagram.com / (not set)\', \'simpl / (not set)\',\'apps.instagram.com / (none)\',\'chatgpt.com / (not set)\',\'(other)\', \'simpl / pi3_landing_page\',\'simpl / (none)\',\'Simpl / SimplApp\',\'Branch / (not set)\') then \'organic\' when lower(sessionsourcemedium) like \'%sms%\' or lower(sessionsourcemedium) like \'%whatsapp%\' or lower(sessionsourcemedium) like \'%email%\' or lower(sessionsourcemedium) like \'%push%\' then \'retention\' when lower(sessionsourcemedium) like \'%referral%\' then \'referral\' end as source, \'app\' as type, sum(sessions) as session from snitch_db.maplemonk.ga_app2_source_medium group by 1,2,3 ) select date, source, type, sum(session) as sessions from source group by 1,2,3 ;",
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
            