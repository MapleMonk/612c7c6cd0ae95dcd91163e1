{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.zero_retailers_test as select rr.code as retailer_name, rr.area_classification, rr.beat_number, rrp.name as parent_name, cast(timestampadd(minute, 660, rr.last_order_date) as date) last_order_date, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date),current_date()) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,cast(timestampadd(minute, 660, rr.last_order_date) as date),current_date()) as recency from eggozdb.maplemonk.my_sql_order_order oo right join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(rr.onboarding_status) = \'onboarded\' and lower(oo.status) in (\'delivered\') group by rr.code , rr.area_classification , rr.beat_number , rrp.name , cast(timestampadd(minute, 660, rr.last_order_date) as date) , cast(timestampadd(minute, 660, rr.onboarding_date) as date) ;",
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
                        