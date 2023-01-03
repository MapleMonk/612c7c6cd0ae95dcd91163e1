{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.untouched_retailers as select tt.*, bgt.so from (SELECT CAST(TIMESTAMPADD(MINUTE, 330, db.beat_date) AS DATE) beat_date, case when rr.distributor_id is not null then \'Secondary_Retailer\' else case when rcc.name =\'Distributor\' then \'Distributor\' else \'Primary_Retailer\' end end as retailer_type, rr2.code as distributor, db.beat_number, db.beat_name, db.beat_type, rc.retailer_count, rr.code AS retailer_name, rr.area_classification, CAST(TIMESTAMPADD(MINUTE, 330, rr.onboarding_date) AS DATE) onboarding_date, rr.onboarding_status, oo2.last_order_date, oor2.last_pickup_date FROM eggozdb.maplemonk.my_sql_distributionchain_beatassignment db left JOIN eggozdb.maplemonk.my_sql_retailer_retailer rr ON rr.beat_number = db.beat_number AND rr.area_classification = db.demand_classification left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rr.category_id = rcc.id left join (select id, code from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr.distributor_id = rr2.id LEFT JOIN (SELECT * FROM eggozdb.maplemonk.primary_and_secondary WHERE eggs_sold is not null and eggs_sold > 0 ) oo ON oo.beat_number_original = db.beat_number and oo.area_classification = db.demand_classification AND oo.date = CAST(TIMESTAMPADD(MINUTE, 330, db.beat_date) AS DATE) AND oo.retailer_name = rr.code LEFT JOIN (SELECT * FROM eggozdb.maplemonk.primary_and_secondary WHERE eggs_replaced is not null and eggs_return is not null and (eggs_replaced>0 or eggs_return>0) ) oor ON oor.beat_number_original = db.beat_number and oor.area_classification = db.demand_classification AND oor.date = CAST(TIMESTAMPADD(MINUTE, 330, db.beat_date) AS DATE) AND rr.code = oor.retailer_name LEFT JOIN (SELECT area_classification, beat_number, COUNT(code) retailer_count FROM eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' GROUP BY beat_number , area_classification) rc ON rc.area_classification = db.demand_classification AND rc.beat_number = db.beat_number LEFT JOIN (SELECT retailer_name, MAX(date) AS last_pickup_date FROM eggozdb.maplemonk.primary_and_secondary WHERE eggs_replaced is not null and eggs_return is not null and (eggs_replaced>0 or eggs_return>0) GROUP BY retailer_name) oor2 ON oor2.retailer_name = rr.code left join ( select retailer_name, max(date) as last_order_date from eggozdb.maplemonk.primary_and_secondary where eggs_sold is not null and eggs_sold > 0 group by retailer_name) oo2 on oo2.retailer_name = rr.code ) tt left join maplemonk.target_jse_gt bgt on bgt.beat_number_original = tt.beat_number and bgt.city = tt.area_classification ; CREATE OR REPLACE TABLE eggozdb.maplemonk.secondary_vs_distributor_data as select coalesce(sr.date, ds.date) date, coalesce(sr.distributor,ds.distributor) distributor, coalesce(sr.area_classification,ds.area_classification) as area_classification, ifnull(sr.revenue,0) as secondary_revenue, ifnull(ds.revenue,0) as distributor_revenue, ifnull(sr.eggs_sold,0) as secondary_eggs_sold, ifnull(ds.eggs_sold,0) as distributor_eggs_sold, ifnull(sr.eggs_replaced,0) as secondary_eggs_replaced, ifnull(ds.eggs_replaced,0) as distributor_eggs_replaced, ifnull(sr.eggs_return,0) as secondary_eggs_return, ifnull(ds.eggs_return,0) as distributor_eggs_return, ifnull(sr.eggs_promo,0) as secondary_eggs_promo, ifnull(ds.eggs_promo,0) as distributor_eggs_promo from ( select date, distributor, area_classification, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(eggs_promo) eggs_promo from eggozdb.maplemonk.primary_and_secondary where lower(retailer_type) = \'secondary_retailer\' group by date, distributor, area_classification ) sr full outer join ( select date, retailer_name as distributor, area_classification, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(eggs_promo) eggs_promo from eggozdb.maplemonk.primary_and_secondary where lower(retailer_type) = \'distributor\' group by date, retailer_name, area_classification ) ds on sr.date = ds.date and sr.distributor = ds.distributor and sr.area_classification = ds.area_classification ;",
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
                        