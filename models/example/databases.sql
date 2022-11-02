{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.zero_billing_retailers as select rr.code as retailer_name, rr.area_classification, rr.beat_number, rrp.name as parent_name, cast(timestampadd(minute, 660, rr.last_order_date) as date) last_order_date, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date),current_date()) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,cast(timestampadd(minute, 660, rr.last_order_date) as date),current_date()) as recency from eggozdb.maplemonk.my_sql_order_order oo right join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(rr.onboarding_status) = \'active\' and lower(oo.status) in (\'delivered\') and rr.distributor_id is null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , cast(timestampadd(minute, 660, rr.last_order_date) as date) , cast(timestampadd(minute, 660, rr.onboarding_date) as date) ; create or replace table eggozdb.maplemonk.secondary_zero_billing_retailers as select rr2.code as distributor, tt.* from (select rr.code as retailer_name, rr.distributor_id, rr.area_classification, rr.beat_number, rrp.name as parent_name, cast(timestampadd(minute, 660, rr.last_order_date) as date) last_order_date, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date),current_date()) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,cast(timestampadd(minute, 660, rr.last_order_date) as date),current_date()) as recency from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(rr.onboarding_status) = \'active\' and lower(oo.status) in (\'created\') and rr.distributor_id is not null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , cast(timestampadd(minute, 660, rr.last_order_date) as date) , cast(timestampadd(minute, 660, rr.onboarding_date) as date) , rr.distributor_id ) tt join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on tt.distributor_id = rr2.id ; create or replace table eggozdb.maplemonk.untouched_retailers as select tt.*, bgt.so from (SELECT CAST(TIMESTAMPADD(MINUTE, 660, db.beat_date) AS DATE) beat_date, db.beat_number, db.beat_name, rc.retailer_count, rr.code AS retailer_name, rr.area_classification, CAST(TIMESTAMPADD(MINUTE, 660, rr.onboarding_date) AS DATE) onboarding_date, CAST(TIMESTAMPADD(MINUTE, 660, rr.last_order_date) AS DATE) last_order_date, oo.status status_bill, oor.line_type status_ret_rep, oor2.last_pickup_date FROM eggozdb.maplemonk.my_sql_distributionchain_beatassignment db RIGHT JOIN eggozdb.maplemonk.my_sql_retailer_retailer rr ON rr.beat_number = db.beat_number AND rr.area_classification = db.demand_classification LEFT JOIN (SELECT * FROM eggozdb.maplemonk.my_sql_order_order WHERE is_trial <> TRUE AND LOWER(status) = \'delivered\') oo ON oo.beat_assignment_id = db.id AND CAST(TIMESTAMPADD(MINUTE, 660, oo.delivery_date) AS DATE) = CAST(TIMESTAMPADD(MINUTE, 660, db.beat_date) AS DATE) AND oo.retailer_id = rr.id LEFT JOIN (SELECT * FROM eggozdb.maplemonk.my_sql_order_orderreturnline WHERE LOWER(line_type) IN (\'replacement\' , \'return\')) oor ON oor.beat_assignment_id = db.id AND CAST(TIMESTAMPADD(MINUTE, 660, oor.pickup_date) AS DATE) = CAST(TIMESTAMPADD(MINUTE, 660, db.beat_date) AS DATE) AND rr.id = oor.retailer_id LEFT JOIN (SELECT area_classification, beat_number, COUNT(code) retailer_count FROM eggozdb.maplemonk.my_sql_retailer_retailer WHERE LOWER(onboarding_status) = \'active\' GROUP BY beat_number , area_classification) rc ON rc.area_classification = db.demand_classification AND rc.beat_number = db.beat_number LEFT JOIN (SELECT retailer_id, MAX(CAST(TIMESTAMPADD(MINUTE, 660, pickup_date) AS DATE)) AS last_pickup_date FROM eggozdb.maplemonk.my_sql_order_orderreturnline WHERE LOWER(line_type) IN (\'replacement\' , \'return\') GROUP BY retailer_id) oor2 ON oor2.retailer_id = rr.id WHERE LOWER(db.beat_type) = \'regular\' AND LOWER(db.beat_status) = \'completed\' AND LOWER(rr.onboarding_status) = \'active\' and rr.distributor_id is null ) tt left join maplemonk.target_jse_gt bgt on bgt.beat_number_original = tt.beat_number and bgt.city = tt.area_classification ; create or replace table eggozdb.maplemonk.secondary_untouched_retailers as select rr2.code as distributor, rr3.onboarded_retailers, rr3.onboarded_beats, rr4.onboarded_retailers_in_beat , dim.*, ss.bill_name, ss.sale, ss.eggs_sold, oo.status as order_status, rl.type as ret_rep_status, oo2.last_order_date, rl2.last_ret_rep_date from ( select dd.date::date as date, rr.code, rr.id as retailer_id, rr.distributor_id, rr.area_classification, rr.beat_number, cast(timestampadd(minute,660,rr.onboarding_date) as date) onboarding_date from maplemonk.date_dim dd join eggozdb.maplemonk.my_sql_retailer_retailer rr where dd.date between dateadd(\'day\',-30,cast(timestampadd(minute, 660, getdate()) as date)) and cast(timestampadd(minute, 660, getdate()) as date) and lower(rr.onboarding_status) = \'active\' and rr.distributor_id is not null ) dim join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = dim.distributor_id left join ( select * from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder where lower(status) = \'created\' ) oo on oo.retailer_id = dim.retailer_id and cast(timestampadd(minute, 660, oo.delivery_date) as date) = dim.date left join ( select * from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturnline where lower(type) in (\'replacement\',\'return\') ) rl on rl.retailer_id = dim.retailer_id and cast(timestampadd(minute, 660, rl.return_date) as date) = dim.date left join ( select max(cast(timestampadd(minute, 660, delivery_date) as date)) as last_order_date, retailer_id from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder group by retailer_id ) oo2 on dim.retailer_id = oo2.retailer_id left join ( select max(cast(timestampadd(minute, 660, return_date) as date)) as last_ret_rep_date, retailer_id from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturnline where lower(type) in (\'replacement\',\'return\') group by retailer_id ) rl2 on dim.retailer_id = rl2.retailer_id left join ( select distributor_id, area_classification, count(code) as onboarded_retailers, count(distinct(beat_number)) as onboarded_beats from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and distributor_id is not null group by distributor_id, area_classification ) rr3 on rr3.distributor_id = dim.distributor_id left join ( select distributor_id, beat_number, area_classification, count(code) as onboarded_retailers_in_beat from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and distributor_id is not null group by distributor_id, area_classification, beat_number ) rr4 on rr4.distributor_id = dim.distributor_id and rr4.area_classification = dim.area_classification and rr4.beat_number = dim.beat_number left join eggozdb.maplemonk.secondary_sales_test ss on dim.distributor_id = ss.distributor_id and dim.retailer_id = ss.retailer_id and dim.date = ss.delivery_date ;",
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
                        