{{ config(
            materialized='table',
                post_hook={
                    "sql": "create or replace table prolicious-wh.maplemonk.prolicious_retention_campaign_fact_items as with retention_data as ( select upper(trim(tool)) as channel, upper(trim(type)) as Ad_Type, case when start_date like \'%-%-____\' then parse_date(\'%d-%m-%Y\', start_date) else parse_date(\'%d-%b-%y\', start_date) end as start_date, case when end_date like \'%-%-____\' then parse_date(\'%d-%m-%Y\', end_date) else parse_date(\'%d-%b-%y\', end_date) end as end_date, upper(trim(campaign_name)) as campaign_name, cast(replace(message_spend,\',\',\'\') as float64) as message_spend, cast(replace(Opened,\',\',\'\') as float64) as message_opened, cast(replace(Revenue,\',\',\'\') as float64) as ad_revenue, cast(replace(cost,\',\',\'\') as float64) as ad_cost, cast(replace(No_of_Message_Sent,\',\',\'\') as int64) as No_of_Message_Sent, cast(replace(No_of_Message_Delivered,\',\',\'\') as int64) as No_of_Message_Delivered, cast(replace(No_of_Purchase,\',\',\'\') as int64) as No_of_Purchase, from prolicious-wh.maplemonk.google_sheets_prolicious_retention_costs ), valid_dates as ( select channel, ad_type, campaign_name, least(start_date, end_date) as start_date, greatest(start_date, end_date) as end_date, message_spend, message_opened, ad_revenue, ad_cost, No_of_Message_Sent, No_of_Message_Delivered, No_of_Purchase from retention_data ) select day_date as date, channel, ad_type, campaign_name, message_spend / (date_diff(end_date, start_date, day) + 1) as message_spend, message_opened / (date_diff(end_date, start_date, day) + 1) as message_opened, ad_revenue / (date_diff(end_date, start_date, day) + 1) as ad_revenue, ad_cost / (date_diff(end_date, start_date, day) + 1) as ad_cost, no_of_message_sent / (date_diff(end_date, start_date, day) + 1) as no_of_message_sent, no_of_message_delivered / (date_diff(end_date, start_date, day) + 1) as no_of_message_delivered, no_of_purchase / (date_diff(end_date, start_date, day) + 1) as no_of_purchase, concat(cast(start_date as string), \' to \', cast(end_date as string)) as date_range from valid_dates, unnest(generate_date_array(start_date, end_date)) as day_date order by date_range, date",
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
            