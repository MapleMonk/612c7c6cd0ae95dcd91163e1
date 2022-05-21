{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.zero_billing_retailers as select zb.code, zb.area_classification, rrp.name AS parent_name, zb.beat_number, zb.onboarding_status, zb.onboarding_date, cast(timestampadd(minute,330,ooo.last_order_date) as date) as last_order_date, datediff(day,ooo.last_order_date,getdate()) as recency, datediff(day,zb.onboarding_date,getdate()) as days_since_onboard, ll.frequency, ll.total_bill_amount, ll.total_bill_amount/ll.frequency as average_order_amount from (select rr.id, rr.parent_id, rr.area_classification ,rr.code, rr.beat_number, rr.onboarding_status, cast(timestampadd(minute,330,rr.onboarding_date) as date) AS onboarding_date from eggozdb.maplemonk.my_sql_retailer_retailer rr left join (select * from eggozdb.maplemonk.my_sql_order_order where is_trial<>True AND cast(timestampadd(minute,330,date) as date) between dateadd(day, -8, getdate()) and getdate()) o ON o.retailer_id = rr.id where o.retailer_id is null) zb left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on zb.parent_id = rrp.id left join (select retailer_id, max(date) as last_order_date from eggozdb.maplemonk.my_sql_order_order where status in (\'delivered\',\'completed\') and status<>\'open_po\' and is_trial<>True group by retailer_id) ooo on ooo.retailer_id = zb.id left join (select retailer_id, count(orderId) as frequency, sum(order_price_amount) total_bill_amount from eggozdb.maplemonk.my_sql_order_order where status in (\'delivered\',\'completed\') group by retailer_id) ll on ll.retailer_id = zb.id where (recency < 31 and recency > 7) or datediff(day,zb.onboarding_date,getdate()) < 31",
                                "transaction": true
                            }
                        ) }}
                        with sample_data as (

                            select * from EGGOZDB.information_schema.databases
                        ),
                        
                        final as (
                            select * from sample_data
                        )
                        select * from final
                        