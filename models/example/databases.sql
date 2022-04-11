{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table vahdam_db.maplemonk.consolidated_marketing_sales as with orders as ( select final_utm_channel, landing_utm_campaign, order_timestamp::date as order_date, count(distinct order_id) as orders, sum(net_sales) as sales from VAHDAM_DB.MAPLEMONK.FACT_ITEMS where SHOP_NAME = \'Shopify_USA\' group by 1,2,3 ), marketing_data as ( select account as marketing_account, campaign_name as marketing_campaign, channel as marketing_channel, date::date as date, sum(impressions) as impressions, sum(clicks) as clicks, sum(spend) as spend, sum(conversions) as conversions, sum(conversion_value) as conversion_value from VAHDAM_DB.MAPLEMONK.MARKETING_CONSOLIDATED group by 1,2,3,4 ) select coalesce(date,order_date) as date ,coalesce(marketing_channel,case when lower(final_utm_channel) like \'%facebook%\' then \'Facebook Ads\' when lower(final_utm_channel) like \'%google ads%\' then \'Google Ads\' else \'Others\' end ) as channel ,coalesce(marketing_account,case when lower(final_utm_channel) like \'%facebook main account%\' then \'USA Main\' when lower(final_utm_channel) like \'%influencer%\' then \'Influencer USA\' when lower(final_utm_channel) like \'%whiteli%\' then \'BMG US FB\' when lower(final_utm_channel) like \'%google ads%\' then \'Google US\' else \'Others\' end ) as Accountfinal ,final_utm_channel ,marketing_account ,marketing_campaign ,landing_utm_campaign ,marketing_channel ,impressions ,clicks ,spend ,conversions ,conversion_value ,orders ,sales from marketing_data m full join orders o on m.date= o.order_date and m.marketing_campaign = o.landing_utm_campaign",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from VAHDAM_DB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        