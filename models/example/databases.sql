{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.beat_utilization_test as select mm.date, mm.area, mm.beat_number_original, mm.operator, mm.retailer_name, rr.total_onboarded, mm.sold, mm.revenue, mm.replaced, mm.returned, mm.collections, mm.today_billing_collections from ( select retailer_name, date, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO where date <=getdate() group by date, area, beat_number_original, operator, retailer_name having sum(net_sales)<>0 or sum(eggs_sold)<>0 or sum(eggs_replaced)<>0 or sum(eggs_return)<>0 ) mm JOIN (select area_classification, beat_number, count(code) as total_onboarded from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' group by area_classification, beat_number) rr on rr.area_classification = mm.area and rr.beat_number = mm.beat_number_original ; create or replace table eggozdb.maplemonk.gurgaon_untouched_retailers_daywise as select mm.date, mm.area, mm.beat_number_original, mm.operator, mm.retailer_name, rr.onboarded_retailer, rr.area_classification, rr.beat_number, mm.sold, mm.revenue, mm.replaced, mm.returned, mm.collections, mm.today_billing_collections from ( select retailer_name, date, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO where date = getdate() group by date, area, beat_number_original, operator, retailer_name having sum(net_sales)<>0 or sum(eggs_sold)<>0 or sum(eggs_replaced)<>0 or sum(eggs_return)<>0 ) mm right JOIN (select area_classification, beat_number, code as onboarded_retailer from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'onboarded\' and beat_number in (select beat_number_original from eggozdb.maplemonk.summary_reporting_table_beat_retailer_DSO where date = getdate() and area = \'Gurgaon-GT\' group by beat_number_original having count(retailer_name)>5) group by area_classification, beat_number, code) rr on mm.retailer_name = rr.onboarded_retailer where rr.area_classification = \'Gurgaon-GT\' ; create or replace table eggozdb.maplemonk.noida_untouched_retailers_daywise as select mm.date, mm.area, mm.beat_number_original, mm.operator, mm.retailer_name, rr.onboarded_retailer, rr.area_classification, rr.beat_number, mm.sold, mm.revenue, mm.replaced, mm.returned, mm.collections, mm.today_billing_collections from ( select retailer_name, date, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO where date = getdate() group by date, area, beat_number_original, operator, retailer_name having sum(net_sales)<>0 or sum(eggs_sold)<>0 or sum(eggs_replaced)<>0 or sum(eggs_return)<>0 ) mm right JOIN (select area_classification, beat_number, code as onboarded_retailer from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and beat_number in (select beat_number_original from eggozdb.maplemonk.summary_reporting_table_beat_retailer_DSO where date = getdate() and area = \'Noida-GT\' group by beat_number_original having count(retailer_name)>5) group by area_classification, beat_number, code) rr on mm.retailer_name = rr.onboarded_retailer where rr.area_classification = \'Noida-GT\' ; create or replace table eggozdb.maplemonk.delhi_untouched_retailers_daywise as select mm.date, mm.area, mm.beat_number_original, mm.operator, mm.retailer_name, rr.onboarded_retailer, rr.area_classification, rr.beat_number, mm.sold, mm.revenue, mm.replaced, mm.returned, mm.collections, mm.today_billing_collections from ( select retailer_name, date, area, beat_number_original, operator, sum(net_sales) as revenue, sum(eggs_sold) as sold, sum(collections) as collections, sum(eggs_replaced) as replaced, sum(eggs_promo) as promo, sum(eggs_return) as returned, sum(collections_as_of_today) as today_billing_collections from eggozdb.maplemonk.Summary_reporting_table_beat_retailer_DSO where date = getdate() group by date, area, beat_number_original, operator, retailer_name having sum(net_sales)<>0 or sum(eggs_sold)<>0 or sum(eggs_replaced)<>0 or sum(eggs_return)<>0 ) mm right JOIN (select area_classification, beat_number, code as onboarded_retailer from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and beat_number in (select beat_number_original from eggozdb.maplemonk.summary_reporting_table_beat_retailer_DSO where date = getdate() and area = \'Delhi-GT\' group by beat_number_original having count(retailer_name)>5) group by area_classification, beat_number, code) rr on mm.retailer_name = rr.onboarded_retailer where rr.area_classification = \'Delhi-GT\' ;",
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
                        