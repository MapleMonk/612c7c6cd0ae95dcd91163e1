{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table maplemonk.prolicious_kwikengage_campaign_report as select cast(parse_date(\'%Y-%m-%d\',date) as date) as Date, case when cost = \'NA\' then null else cast(replace(cost,\',\',\'\') as float64) end as Cost, case when ctr = \'NA\' then null else cast(replace(replace(ctr,\',\',\'\'),\'%\',\'\') as float64)/100 end as ctr, upper(name) as campaign_name, case when replace(roas,\',\',\'\') = \'NA\' then null else cast(replace(roas,\',\',\'\') as float64) end as roas, cast(replace(Sales,\',\',\'\') as float64) as revenue, cast(seen as int64) as seen, cast(sent as int64) as sent, cast(buyers as int64) as customers, cast(clicks as int64) as clicks, cast(orders as int64) as orders, cast(delivered as int64) as delivered, cast(Unsubscribers as int64) as unsubscribers, upper(Channel) as channel, upper(source) as source, \'CAMPAIGN INSIGHTS\' as report_type from `Maplemonk.campaign_report_campaign_insights` union all select cast(parse_date(\'%Y-%m-%d\',date) as date) as Date, case when cost = \'NA\' then null else cast(replace(cost,\',\',\'\') as float64) end as Cost, case when ctr = \'NA\' then null else cast(replace(replace(ctr,\',\',\'\'),\'%\',\'\') as float64)/100 end as ctr, upper(name) as campaign_name, case when replace(roas,\',\',\'\') = \'NA\' then null else cast(replace(roas,\',\',\'\') as float64) end as roas, case when Sales = \'NA\' then null else cast(replace(Sales,\',\',\'\') as float64) end as revenue, cast(seen as int64) as seen, cast(sent as int64) as sent, cast(buyers as int64) as customers, cast(clicks as int64) as clicks, cast(orders as int64) as orders, cast(delivered as int64) as delivered, cast(Unsubscribers as int64) as unsubscribers, upper(Channel) as channel, null as source, \'OTHER AUTOMATIONS\' as report_type from `Maplemonk.kwikengage_marketing_other_automation_report` ;",
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
            