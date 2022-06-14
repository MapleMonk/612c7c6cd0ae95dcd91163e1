{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_utilization_test as select mm.date, mm.area, mm.beat_number_original, mm.operator, mm.retailer_name, rr.total_onboarded, mm.no_of_visits/rr.total_onboarded as utilization, mm.sold, mm.revenue, mm.replaced, mm.returned, mm.collections, mm.today_billing_collections, case when mm.sold = 0 then 0 else mm.replaced/mm.sold end as replacement_per, case when mm.sold = 0 then 0 else mm.returned/mm.sold end as returned_per from ( select retailer_name, date, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO where date <=getdate() group by date, retailer_name ) mm JOIN (select area_classification, beat_number, count(code) as total_onboarded from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' group by area_classification, beat_number) rr on rr.area_classification = mm.area and rr.beat_number = mm.beat_number_original ;",
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
                        