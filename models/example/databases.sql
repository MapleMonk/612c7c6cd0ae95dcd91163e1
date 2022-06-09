{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_utilization_test as select date, area, beat_number_original, operator, no_of_visits, sold, revenue, replaced, returned, collections, today_billing_collections, case when sold = 0 then 0 else replaced/sold end as replacement_per, case when sold = 0 then 0 else returned/sold end as returned_per from ( select date, count(distinct(retailer_name)) as no_of_visits, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO group by date, area, beat_number_original, operator )",
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
                        