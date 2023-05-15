{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.date_region_skutype as select * from date_region_dim join sku_type where date between \'2020-01-01\' and getdate() ; Create or replace table eggozdb.maplemonk.Date_area_dim as select cast(Date as date) Date, area_classification from eggozdb.maplemonk.date_dim cross join (select distinct area_classification from eggozdb.maplemonk.my_sql_retailer_retailer) where date between \'2020-01-01\' and getdate() ; create or replace table eggozdb.maplemonk.date_area_retailer_dim_2 as select distinct dd.date::date as date, rr.id as retailer_id, rr.area_classification, rr.retailer_name, rr.beat_number, rr.category_id, rr.parent_id, rr.distributor_id from (select date::date as date from eggozdb.maplemonk.date_dim where date::date between \'2020-01-01\' and getdate()) dd full outer join (select id, code as retailer_name, area_classification, beat_number, category_id, parent_id, distributor_id from eggozdb.maplemonk.my_sql_retailer_retailer) rr ; create or replace table eggozdb.maplemonk.date_area_beat_dim as select t1.date, t1.area_classification, t2.beat_number from date_area_dim t1 left join (select distinct beat_number, area_classification from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status=\'Active\') t2 on t1.area_classification = t2.area_classification where t1.date between \'2020-01-01\' and getdate(); create or replace table eggozdb.maplemonk.date_area_retailer_dim as select date, area_classification, retailer_name, beat_number as beat_number_original from eggozdb.maplemonk.date_area_retailer_dim_2;",
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
                        