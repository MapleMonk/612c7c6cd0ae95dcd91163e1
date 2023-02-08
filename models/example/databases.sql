{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.zero_billing_retailers as select rr.code as retailer_name, rr.area_classification, rr.beat_number, rrp.name as parent_name, lod.last_order_date, rr.onboarding_status, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date),current_date()) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,lod.last_order_date, cast(timestampadd(minute, 660, current_date()) as date)) as recency from eggozdb.maplemonk.my_sql_order_order oo right join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id left join ( select retailer_id, max(cast(timestampadd(minute, 660, delivery_date) as date)) last_order_date from eggozdb.maplemonk.my_sql_order_order where lower(status) in (\'completed\',\'delivered\') group by retailer_id ) lod on lod.retailer_id = rr.id where lower(oo.status) in (\'delivered\') and rr.distributor_id is null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , rr.onboarding_status , cast(timestampadd(minute, 660, rr.onboarding_date) as date) , lod.last_order_date ; create or replace table eggozdb.maplemonk.secondary_zero_billing_retailers as select rr2.code as distributor, tt.* from (select rr.code as retailer_name, rr.distributor_id, rr.area_classification, rr.beat_number, rrp.name as parent_name, lod.last_order_date, rr.onboarding_status, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, datediff(day,cast(timestampadd(minute, 660, rr.onboarding_date) as date), cast(timestampadd(minute, 660, current_date()) as date)) days_since_onboarded, sum(oo.order_price_amount) total_bill_amount, count(distinct(oo.id)) as frequency, sum(oo.order_price_amount)/count(distinct(oo.id)) as average_order_amount, datediff(day,lod.last_order_date,cast(timestampadd(minute, 660, current_date()) as date)) as recency from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join ( select retailer_id, max(cast(timestampadd(minute, 660, delivery_date) as date)) last_order_date from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder group by retailer_id ) lod on lod.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where lower(oo.status) in (\'created\') and rr.distributor_id is not null group by rr.code , rr.area_classification , rr.beat_number , rrp.name , cast(timestampadd(minute, 660, rr.last_order_date) as date) , cast(timestampadd(minute, 660, rr.onboarding_date) as date) , rr.distributor_id , rr.onboarding_status , lod.last_order_date ) tt join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on tt.distributor_id = rr2.id ; create or replace table eggozdb.maplemonk.secondary_untouched_retailers as select rr2.code as distributor, rr3.onboarded_retailers, rr3.onboarded_beats, rr4.onboarded_retailers_in_beat , dim.*, ifnull(ss.sale,0) sale, ifnull(ss.eggs_sold,0) eggs_sold, ifnull(rep.eggs_rep,0) eggs_rep, ifnull(ret.eggs_ret,0) eggs_ret, ifnull(promo.eggs_promo,0) eggs_promo, ss.status as order_status, rep.replacement_status, ret.return_status, promo.promo_status, ifnull(ret.total_return_amount,0) total_return_amount, oo2.last_order_date, rl2.last_ret_rep_date, datediff(day, oo2.last_order_date,cast(timestampadd(minute, 660, current_date()) as date)) as order_recency, datediff(day, rl2.last_ret_rep_date,cast(timestampadd(minute, 660, current_date()) as date)) as ret_rep_recency, datediff(day, dim.onboarding_date, cast(timestampadd(minute, 660, current_date()) as date)) as days_since_onboarded, tt.name as operator from ( select dd.date::date as date, rr.code, rr.id as retailer_id, rr.distributor_id, rr.area_classification, rr.beat_number, rr.onboarding_status, cast(timestampadd(minute,660,rr.onboarding_date) as date) onboarding_date from maplemonk.date_dim dd join eggozdb.maplemonk.my_sql_retailer_retailer rr where dd.date between dateadd(\'day\',-300,cast(timestampadd(minute, 660, getdate()) as date)) and cast(timestampadd(minute, 660, getdate()) as date) and rr.distributor_id is not null ) dim left join eggozdb.maplemonk.my_sql_distributor_sales_secondarytrip sst on cast(timestampadd(minute,660,sst.beat_date) as date) = dim.date and sst.beat_number = dim.beat_number LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) tt ON tt.id = sst.salesrepresentative_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = dim.distributor_id left join ( select max(cast(timestampadd(minute, 660, delivery_date) as date)) as last_order_date, retailer_id from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder group by retailer_id ) oo2 on dim.retailer_id = oo2.retailer_id left join ( select max(cast(timestampadd(minute, 660, return_date) as date)) as last_ret_rep_date, retailer_id from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturn where lower(type) in (\'replacement\',\'return\') group by retailer_id ) rl2 on dim.retailer_id = rl2.retailer_id left join ( select distributor_id, area_classification, count(code) as onboarded_retailers, count(distinct(beat_number)) as onboarded_beats from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and distributor_id is not null group by distributor_id, area_classification ) rr3 on rr3.distributor_id = dim.distributor_id left join ( select distributor_id, beat_number, area_classification, count(code) as onboarded_retailers_in_beat from eggozdb.maplemonk.my_sql_retailer_retailer where lower(onboarding_status) = \'active\' and distributor_id is not null group by distributor_id, area_classification, beat_number ) rr4 on rr4.distributor_id = dim.distributor_id and rr4.area_classification = dim.area_classification and rr4.beat_number = dim.beat_number left join (select status, distributor_id, retailer_id, delivery_date, sum(sale) as sale, sum(eggs_sold) as eggs_sold from eggozdb.maplemonk.secondary_sales where lower(status) <> \'cancelled\' group by distributor_id, retailer_id, delivery_date, status ) ss on dim.distributor_id = ss.distributor_id and dim.retailer_id = ss.retailer_id and dim.date = ss.delivery_date left join ( select distributor, distributor_id, retailer_name, area_classification, beat_number, type as replacement_status, sum(total_ret_amount) total_return_amount, pickup_date, retailer_id, sum(eggs) eggs_rep from eggozdb.maplemonk.secondary_return_replacement_promo where lower(type) = \'replacement\' and lower(status) <> \'cancelled\' group by distributor, distributor_id, retailer_name, area_classification, beat_number, type, pickup_date, retailer_id ) rep on dim.distributor_id = rep.distributor_id and dim.retailer_id = rep.retailer_id and dim.date = rep.pickup_date left join ( select distributor, distributor_id, retailer_name, area_classification, beat_number, type as return_status, sum(total_ret_amount) total_return_amount, pickup_date, retailer_id, sum(eggs) eggs_ret from eggozdb.maplemonk.secondary_return_replacement_promo where lower(type) = \'return\' and lower(status) <> \'cancelled\' group by distributor, distributor_id, retailer_name, area_classification, beat_number, type, pickup_date, retailer_id ) ret on dim.distributor_id = ret.distributor_id and dim.retailer_id = ret.retailer_id and dim.date = ret.pickup_date left join ( select distributor, distributor_id, retailer_name, area_classification, beat_number, type as promo_status, pickup_date, retailer_id, sum(eggs) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo where lower(type) = \'promo\' group by distributor, distributor_id, retailer_name, area_classification, beat_number, type, pickup_date, retailer_id ) promo on dim.distributor_id = promo.distributor_id and dim.retailer_id = promo.retailer_id and dim.date = promo.pickup_date ;",
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
                        