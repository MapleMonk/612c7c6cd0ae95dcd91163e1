{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.eggoz_soh as select distinct t1.date::date as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,left(pp.name,1)) as sku, t2.quantity as eggoz_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id ; create or replace table eggozdb.maplemonk.eggoz_soh_adherence as select t1.date::date date, cau.name as sales_person_name, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, count(distinct t1.retailer_id) retailers_covered, t2.active_retailers, count(distinct t1.retailer_id)*100/t2.active_retailers adherence from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join (select beat_number, area_classification, count(id) active_retailers from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status=\'Active\' group by area_classification, beat_number) t2 on t2.area_classification = rr.area_classification and t2.beat_number = rr.beat_number group by t1.date, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, cau.name, t2.active_retailers ; create or replace table eggozdb.maplemonk.doi as select nn.*, (nn.soh_old + sum(ll.eggs_sold)/nn.sku_count+sum(ll.eggs_promo)/nn.sku_count-sum(ll.eggs_return)/nn.sku_count - nn.soh_new + case when sum(ll.eggs_replaced)/nn.sku_count-nn.soh_old >0 then sum(ll.eggs_replaced)/nn.sku_count-nn.soh_old else 0 end) as tertiary_sales_sku, div0(nn.soh_new*(datediff(day,date_old,date_new)+1),(nn.soh_old + sum(ll.eggs_sold)/nn.sku_count+sum(ll.eggs_promo)/nn.sku_count-sum(ll.eggs_return)/nn.sku_count - nn.soh_new + case when (sum(ll.eggs_replaced)/nn.sku_count)-nn.soh_old >0 then (sum(ll.eggs_replaced)/nn.sku_count-nn.soh_old) else 0 end)) doi_sku, datediff(day,date_old,date_new)+1 days, sum(ll.revenue) revenue, sum(ll.eggs_sold)/nn.sku_count sku_sold, sum(ll.eggs_replaced)/nn.sku_count sku_replaced, sum(ll.eggs_return)/nn.sku_count sku_return, sum(ll.eggs_promo)/nn.sku_count sku_promo, (nn.soh_old*nn.sku_count + sum(ll.eggs_sold) + sum(ll.eggs_promo) - sum(ll.eggs_return) - nn.soh_new*nn.sku_count + case when sum(ll.eggs_replaced) - nn.soh_old*nn.sku_count > 0 then sum(ll.eggs_replaced)-nn.soh_old*nn.sku_count else 0 end) as tertiary_sales_eggs, div0(nn.soh_new*nn.sku_count*(datediff(day,date_old,date_new)+1),(nn.soh_old*nn.sku_count + sum(ll.eggs_sold)+sum(ll.eggs_promo)-sum(ll.eggs_return) - nn.soh_new*nn.sku_count + case when (sum(ll.eggs_replaced))-nn.soh_old*nn.sku_count >0 then (sum(ll.eggs_replaced)-nn.soh_old*nn.sku_count) else 0 end)) doi_eggs, sum(ll.eggs_sold) eggs_sold, sum(ll.eggs_replaced) eggs_replaced, sum(ll.eggs_return) eggs_return, sum(ll.eggs_promo) eggs_promo, ll.retailer_type, ll.distributor from (select mm.model_id, mm.visit_type, mm.sales_person, mm.retailer_name, mm.area_classification, mm.beat_number, mm.sku_count, mm.eggoz_sku, mm.date as date_new, mm.date_1 as date_old, mm.eggoz_soh soh_new, ifnull(mm.soh_1,0) soh_old from ( select tt.*, lag(tt.date) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number, tt.sku_count, tt.eggoz_sku order by tt.date asc) date_1, lag(tt.eggoz_soh) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number, tt.eggoz_sku order by tt.date asc) soh_1 from ( select t1.id model_id, cast(timestampadd(minute, 660, date) as date) date, t1.type visit_type, cau.name as sales_person, rr.code as retailer_name, rr.area_classification, rr.beat_number, pp.sku_count, concat(pp.sku_count,left(pp.name,1)) eggoz_sku, t2.quantity eggoz_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id where t1.type in (\'Visit\',\'Closing\') group by t1.id, t1.date, t1.type , cau.name, rr.code, rr.area_classification, rr.beat_number, pp.sku_count, pp.name, t2.quantity ) tt ) mm where mm.date_1 is not null) nn left join ( select date, retailer_name, area_classification, beat_number_original, sku, revenue, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, distributor from eggozdb.maplemonk.primary_and_secondary_sku where area_classification in (\'Noida-GT\',\'Gurgaon-GT\',\'Delhi-GT\',\'NCR-OF-MT\') and year(date) = 2023 and month(date) = 03 or month(date) = 02 and sku is not null ) ll on ll.retailer_name = nn.retailer_name and ll.sku = nn.eggoz_sku where ll.date between nn.date_old and nn.date_new group by nn.model_id, nn.visit_type, nn.sales_person, nn.retailer_name, nn.area_classification, nn.beat_number, nn.sku_count, nn.eggoz_sku, nn.date_new, nn.date_old, nn.soh_new, nn.soh_old, ll.retailer_type, ll.distributor ; create or replace table eggozdb.maplemonk.competitor_soh as select t1.id as model_id, t1.type as visit_type, cast(timestampadd(minute, 660, t1.date) as date) date, t2.brand_name, cau.name as sales_person, rr.code as retailer_name, rr.area_classification, rr.beat_number, concat(t3.sku_count,left(t3.sku,1)) sku, t3.quantity comp_soh, t3.sku_count from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_competitorsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_competitorsohinline t3 on t3.competitor_soh_id = t2.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id where t2.brand_name is not null ;",
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
                        