{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.eggoz_soh as with model_retailer_sku_dim_cte as ( with model_dim_cte as ( select distinct entry_date, model_id, visit_type, sales_person, retailer_name, retailer_id, area_classification, beat_number_original, frozen_beat_number, marketing_cluster, city_name, state, latitude, longitude, tt.sku, tt.sku_count from ( select distinct cast(timestampadd(minute, 660, t1.date) as date) as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, t1.id as model_id, rr.beat_number as beat_number_original, rb.beat_number as frozen_beat_number, rr.marketing_cluster, concat(pp.sku_count,pp.short_name) as sku, t2.quantity as eggoz_soh, t1.retailer_id, pp.sku_count, bc.city_name, bc.state, t1.latitude, t1.longitude from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rb.id = rr.frozenBeat_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id where t1.type in (\'Visit\',\'Closing\') ) cross join (select distinct concat(sku_count,short_name) sku, sku_count from eggozdb.maplemonk.my_sql_product_product where brand_type = \'branded\' ) tt where entry_date>=date_trunc(\'month\',dateadd(\'month\',-20,current_date())) ) select md.* from model_dim_cte md left join ( select distinct retailer_name, retailer_id, sku from eggozdb.maplemonk.primary_and_secondary_sku where eggs_sold>0 and eggs_sold is not null ) mm on md.retailer_id = mm.retailer_id and md.sku = mm.sku where mm.retailer_name is not null ) select cte.*, ifnull(e_soh.eggoz_soh,0) eggoz_soh, frc.frozen_retailer_count from model_retailer_sku_dim_cte cte left join ( select distinct cast(timestampadd(minute, 660, t1.date) as date) as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, t1.id as model_id, rr.beat_number as beat_number_original, rb.beat_number as frozen_beat_number, rr.marketing_cluster, concat(pp.sku_count,pp.short_name) as sku, t2.quantity as eggoz_soh, t1.retailer_id, pp.sku_count, bc.city_name, bc.state, t1.latitude, t1.longitude from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rb.id = rr.frozenBeat_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id ) e_soh on cte.entry_date = e_soh.entry_date and cte.retailer_name = e_soh.retailer_name and cte.model_id = e_soh.model_id and cte.sku = e_soh.sku and cte.retailer_id = e_soh.retailer_id left join ( select count(code) frozen_retailer_count, rb.beat_number from my_sql_retailer_retailer rr left join my_sql_retailer_retailerbeat rb on rb.id = rr.frozenbeat_id where rr.onboarding_status = \'Active\' and rr.frozenbeat_id is not null group by rb.beat_number ) frc on cte.frozen_beat_number = frc.beat_number ; with model_retailer_sku_dim_cte as ( with model_dim_cte as ( select distinct entry_date, model_id, visit_type, sales_person, retailer_name, retailer_id, area_classification, beat_number_original, frozen_beat_number, marketing_cluster, city_name, state, latitude, longitude, tt.sku, tt.sku_count from ( select distinct cast(timestampadd(minute, 660, t1.date) as date) as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, t1.id as model_id, rr.beat_number as beat_number_original, rb.beat_number as frozen_beat_number, rr.marketing_cluster, concat(pp.sku_count,pp.short_name) as sku, t2.quantity as eggoz_soh, t1.retailer_id, pp.sku_count, bc.city_name, bc.state, t1.latitude, t1.longitude from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rb.id = rr.frozenBeat_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id where t1.type in (\'Visit\',\'Closing\') ) cross join (select distinct concat(sku_count,short_name) sku, sku_count from eggozdb.maplemonk.my_sql_product_product where brand_type = \'branded\' ) tt where entry_date>=date_trunc(\'month\',dateadd(\'month\',-20,current_date())) ) select md.* from model_dim_cte md left join ( select distinct retailer_name, retailer_id, sku from eggozdb.maplemonk.primary_and_secondary_sku where eggs_sold>0 and eggs_sold is not null ) mm on md.retailer_id = mm.retailer_id and md.sku = mm.sku where mm.retailer_name is not null ) select cte.*, ifnull(e_soh.eggoz_soh,0) eggoz_soh, frc.frozen_retailer_count from model_retailer_sku_dim_cte cte left join ( select distinct cast(timestampadd(minute, 660, t1.date) as date) as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, t1.id as model_id, rr.beat_number as beat_number_original, rb.beat_number as frozen_beat_number, rr.marketing_cluster, concat(pp.sku_count,pp.short_name) as sku, t2.quantity as eggoz_soh, t1.retailer_id, pp.sku_count, bc.city_name, bc.state, t1.latitude, t1.longitude from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rb.id = rr.frozenBeat_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = rr.city_id ) e_soh on cte.entry_date = e_soh.entry_date and cte.retailer_name = e_soh.retailer_name and cte.model_id = e_soh.model_id and cte.sku = e_soh.sku and cte.retailer_id = e_soh.retailer_id left join ( select count(code) frozen_retailer_count, rb.beat_number from my_sql_retailer_retailer rr left join my_sql_retailer_retailerbeat rb on rb.id = rr.frozenbeat_id where rr.onboarding_status = \'Active\' and rr.frozenbeat_id is not null group by rb.beat_number ) frc on cte.frozen_beat_number = frc.beat_number ; create or replace table eggozdb.maplemonk.eggoz_soh_adherence as select cau.id, t1.date::date date, cau.name as sales_person_name, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, count(distinct t1.retailer_id) retailers_covered, t2.active_retailers,coalesce(oo.status,dso.status) status, zeroifnull(coalesce(nullifzero(count(dso.id)),nullifzero(count(oo.id)))) Bill_count ,zeroifnull(coalesce(sum(oo.order_price_amount),sum(dso.order_price_amount))) as revenue, ss.management_status from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join (select * from eggozdb.maplemonk.my_sql_order_order where status in(\'delivered\',\'completed\')) oo on t1.retailer_id = oo.retailer_id and t1.beatassignment_id = oo.beat_assignment_id left join (select * from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder where status in(\'created\',\'draft\')) dso on t1.retailer_id = dso.retailer_id and t1.secondarytrip_id = dso.trip_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join (select beat_number, area_classification, count(id) active_retailers from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status = \'Active\' and category_id not in (3,10) group by area_classification, beat_number) t2 on t2.area_classification = rr.area_classification and t2.beat_number = rr.beat_number where lower(t1.type) in (\'visit\',\'closing\') and rr.category_id not in (3,10) group by cau.id,oo.status,dso.status, t1.date, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, cau.name, t2.active_retailers, ss.management_status ; create or replace table eggozdb.maplemonk.doi as select row_number() over (partition by nn.retailer_name order by nn.date_new desc) rownumber ,nn.*, datediff(day,date_old,date_new)+1 days, div0(nn.soh_new_eggs*(datediff(day,nn.date_old,nn.date_new)+1),(nn.soh_old_eggs + sum(ifnull(ll.eggs_sold,0))+sum(ifnull(ll.eggs_promo,0))-sum(ifnull(ll.eggs_return,0)) - nn.soh_new_eggs + case when (sum(ifnull(ll.eggs_replaced,0)))-nn.soh_old_eggs >0 then (sum(ifnull(ll.eggs_replaced,0))-nn.soh_old_eggs) else 0 end)) doi_eggs, (nn.soh_old_eggs + sum(ifnull(ll.eggs_sold,0)) + sum(ifnull(ll.eggs_promo,0)) - sum(ifnull(ll.eggs_return,0)) - nn.soh_new_eggs + case when sum(ifnull(ll.eggs_replaced,0)) - nn.soh_old_eggs > 0 then sum(ifnull(ll.eggs_replaced,0))-nn.soh_old_eggs else 0 end) as tertiary_sales_eggs, sum(ifnull(ll.eggs_sold,0)) eggs_sold, sum(ifnull(ll.eggs_replaced,0)) eggs_replaced, sum(ifnull(ll.eggs_return,0)) eggs_return, sum(ifnull(ll.revenue,0)) revenue, sum(ifnull(ll.eggs_promo,0)) eggs_promo, ll.retailer_type, ll.distributor from ( select mm.visit_type, mm.retailer_name, mm.retailer_id, mm.area_classification, mm.beat_number, mm.frozen_beat_number, mm.marketing_cluster, mm.society_name, mm.activity_status, mm.date as date_new, mm.date_1 as date_old, mm.eggoz_soh_eggs soh_new_eggs, ifnull(mm.soh_1,0) soh_old_eggs from ( select tt.*, lag(tt.date) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number order by tt.date asc) date_1, lag(tt.eggoz_soh_eggs) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number order by tt.date asc) soh_1 from ( select cast(timestampadd(minute, 660, date) as date) date, t1.type visit_type, rr.code as retailer_name, rr.area_classification, rr.beat_number, rb.beat_number as frozen_beat_number, sum(pp.sku_count*t2.quantity) eggoz_soh_eggs, rr.marketing_cluster, t1.retailer_id, rr.society_name, rr.activity_status from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rb.id = rr.frozenBeat_id where t1.type in (\'Visit\',\'Closing\') group by t1.date, t1.type, rr.code, rr.area_classification, rr.beat_number,rb.beat_number, t1.retailer_id, rr.marketing_cluster, rr.society_name, rr.activity_status order by cast(timestampadd(minute, 660, date) as date) desc ) tt ) mm ) nn left join ( select date, retailer_name, area_classification, beat_number_original, revenue, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, distributor, retailer_id from eggozdb.maplemonk.primary_and_secondary where date >= \'2023-01-01\' ) ll on ll.retailer_name = nn.retailer_name and ll.retailer_id = nn.retailer_id and ll.area_classification=nn.area_classification where ll.date >= nn.date_old and ll.date < nn.date_new group by nn.visit_type, nn.retailer_name, nn.retailer_id, nn.area_classification, nn.beat_number, nn.frozen_beat_number, nn.marketing_cluster, nn.society_name, nn.activity_status, nn.date_new, nn.date_old, nn.soh_new_eggs, nn.soh_old_eggs, ll.retailer_type, ll.distributor order by nn.date_new desc ; create or replace table eggozdb.maplemonk.doi_eggs as select row_number() over (partition by nn.retailer_name order by nn.date_new desc) rownumber ,nn.*, datediff(day,date_old,date_new)+1 days, div0(nn.soh_new_eggs*(datediff(day,nn.date_old,nn.date_new)+1),(nn.soh_old_eggs + sum(ifnull(ll.eggs_sold,0))+sum(ifnull(ll.eggs_promo,0))-sum(ifnull(ll.eggs_return,0)) - nn.soh_new_eggs + case when (sum(ifnull(ll.eggs_replaced,0)))-nn.soh_old_eggs >0 then (sum(ifnull(ll.eggs_replaced,0))-nn.soh_old_eggs) else 0 end)) doi_eggs, (nn.soh_old_eggs + sum(ifnull(ll.eggs_sold,0)) + sum(ifnull(ll.eggs_promo,0)) - sum(ifnull(ll.eggs_return,0)) - nn.soh_new_eggs + case when sum(ifnull(ll.eggs_replaced,0)) - nn.soh_old_eggs > 0 then sum(ifnull(ll.eggs_replaced,0))-nn.soh_old_eggs else 0 end) as tertiary_sales_eggs, sum(ifnull(ll.eggs_sold,0)) eggs_sold, sum(ifnull(ll.eggs_replaced,0)) eggs_replaced, sum(ifnull(ll.eggs_return,0)) eggs_return, sum(ifnull(ll.revenue,0)) revenue, sum(ifnull(ll.eggs_promo,0)) eggs_promo, ll.retailer_type, ll.distributor from ( select mm.visit_type, mm.retailer_name, mm.retailer_id, mm.area_classification, mm.beat_number, mm.marketing_cluster, mm.society_name, mm.activity_status, mm.date as date_new, mm.date_1 as date_old, mm.eggoz_soh_eggs soh_new_eggs, ifnull(mm.soh_1,0) soh_old_eggs from ( select tt.*, lag(tt.date) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number order by tt.date asc) date_1, lag(tt.eggoz_soh_eggs) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number order by tt.date asc) soh_1 from ( select cast(timestampadd(minute, 660, date) as date) date, t1.type visit_type, rr.code as retailer_name, rr.area_classification, rr.beat_number, sum(pp.sku_count*t2.quantity) eggoz_soh_eggs, rr.marketing_cluster, t1.retailer_id, rr.society_name, rr.activity_status from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id where t1.type in (\'Visit\',\'Closing\') group by t1.date, t1.type, rr.code, rr.area_classification, rr.beat_number, t1.retailer_id, rr.marketing_cluster, rr.society_name, rr.activity_status order by cast(timestampadd(minute, 660, date) as date) desc ) tt ) mm ) nn left join ( select date, retailer_name, area_classification, beat_number_original, revenue, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, distributor, retailer_id from eggozdb.maplemonk.primary_and_secondary where date >= \'2023-01-01\' ) ll on ll.retailer_name = nn.retailer_name and ll.retailer_id = nn.retailer_id and ll.area_classification=nn.area_classification where ll.date >= nn.date_old and ll.date < nn.date_new group by nn.visit_type, nn.retailer_name, nn.retailer_id, nn.area_classification, nn.beat_number, nn.marketing_cluster, nn.society_name, nn.activity_status, nn.date_new, nn.date_old, nn.soh_new_eggs, nn.soh_old_eggs, ll.retailer_type, ll.distributor order by nn.date_new desc ; create or replace table eggozdb.maplemonk.competitor_soh as select t1.id as model_id, t1.type as visit_type, cast(timestampadd(minute, 660, t1.date) as date) date, t2.brand_name, cau.name as sales_person, rr.code as retailer_name, rr.area_classification, rr.beat_number, rr.marketing_cluster, concat(t3.sku_count,left(t3.sku,1)) sku, t3.quantity comp_soh, t3.sku_count, t1.retailer_id, t1.latitude, t1.longitude from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_competitorsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_competitorsohinline t3 on t3.competitor_soh_id = t2.id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id where t2.brand_name is not null ; create or replace table eggozdb.maplemonk.store_potential as select t5.*, t6.eggoz_soh_punch_count, t7.comp_soh_punch_count from (select date_from_parts(t3.year, t3.month, 01) date, t3.retailer_name, t3.retailer_id, t3.area_classification, t3.beat_number, t3.sku_count, t3.sku, t3.eggoz_soh, t3.comp_soh, t3.eggoz_soh_eggs, t3.comp_soh_eggs, ifnull(t4.revenue,0) revenue, ifnull(t4.eggs_sold,0) eggs_sold, ifnull(t4.eggs_replaced,0) eggs_replaced, ifnull(t4.eggs_return,0) eggs_return, ifnull(t4.eggs_promo,0) eggs_promo from ( select month(date) month, year(date) year, retailer_name, retailer_id, area_classification, beat_number, sku_count, sku, sum(eggoz_soh) eggoz_soh, sum(comp_soh) comp_soh, sum(eggoz_soh_eggs) eggoz_soh_eggs, sum(comp_soh_eggs) comp_soh_eggs from ( select coalesce(t1.model_id,t2.model_id) model_id, coalesce(t1.visit_type,t2.visit_type) visit_type, coalesce(t1.sales_person, t2.sales_person) sales_person, coalesce(t1.retailer_name, t2.retailer_name) retailer_name, coalesce(t1.retailer_id,t2.retailer_id) retailer_id, coalesce(t1.area_classification,t2.area_classification) area_classification, coalesce(t1.beat_number_original,t2.beat_number) beat_number, coalesce(t1.sku_count,t2.sku_count) sku_count, coalesce(t1.sku,t2.sku) sku, coalesce(t1.entry_date,t2.date) date, ifnull(t1.eggoz_soh,0) eggoz_soh, ifnull(t2.comp_soh,0) comp_soh, ifnull(t1.eggoz_soh*t1.sku_count,0) eggoz_soh_eggs, ifnull(t2.comp_soh_eggs,0) comp_soh_eggs from eggoz_soh t1 full outer join (select model_id, visit_type, date, sales_person, retailer_name, area_classification, beat_number, sku, sum(comp_soh) comp_soh, sum(comp_soh*sku_count) comp_soh_eggs, sku_count, retailer_id from competitor_soh where visit_type in (\'Closing\',\'Visit\') group by model_id, visit_type, date, sales_person, retailer_name, area_classification, beat_number, sku, sku_count, retailer_id) t2 on t1.retailer_id = t2.retailer_id and t1.entry_date = t2.date and t1.sku = t2.sku and t1.model_id = t2.model_id where t1.visit_type in (\'Closing\',\'Visit\') ) group by month(date), year(date), retailer_name, retailer_id, area_classification, beat_number, sku_count, sku ) t3 left join ( select year(date) year, month(date) month, retailer_name, area_classification, beat_number_original, onboarding_status, retailer_id, parent_retailer_name, sku, sum(revenue) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(eggs_promo) eggs_promo, retailer_type, distributor, cluster_dec, cluster_dec from primary_and_secondary_sku where sku is not null and area_classification in (\'Gurgaon-GT\',\'Delhi-GT\',\'Noida-GT\',\'NCR-OF-MT\') group by year(date), month(date), retailer_name, area_classification, beat_number_original, onboarding_status, retailer_id, parent_retailer_name, sku, retailer_type, distributor, cluster_dec, cluster_dec ) t4 on t3.retailer_id = t4.retailer_id and t3.month = t4.month and t3.year = t4.year and t3.sku = t4.sku ) t5 left join (select month(entry_date) month, year(entry_date) year, count(distinct model_id) eggoz_soh_punch_count, retailer_id from eggoz_soh where sku is not null group by month(entry_date), year(entry_date), retailer_id) t6 on month(t5.date) = t6.month and year(t5.date) = t6.year and t5.retailer_id = t6.retailer_id left join (select month(date) month, year(date) year, count(distinct model_id) comp_soh_punch_count, retailer_id from competitor_soh where sku is not null group by month(date), year(date), retailer_id) t7 on month(t5.date) = t7.month and year(t5.date) = t7.year and t5.retailer_id = t7.retailer_id ; create or replace table eggozdb.maplemonk.pjp_soh_adherance as select t1.pjp, coalesce(t1.date_pjp,t2.entry_date) date, t1.date_pjp, t2.entry_date, t2.sales_person as soh_person, coalesce(t1.beat_number_pjp,t2.beat_number_original) beat_number, (select count(code) from eggozdb.maplemonk.my_sql_retailer_retailer where beat_number = coalesce(t1.beat_number_pjp,t2.beat_number_original) and onboarding_status = \'Active\' and area_classification = coalesce(t1.area_classification_pjp,t2.area_classification)) as no_of_outlets , coalesce(t1.area_classification_pjp,t2.area_classification) area_classification, t2.outlets_touched, coalesce(t1.city_pjp,t2.city_name) city from (select \'pjp\' pjp, date::date date_pjp, beat_number beat_number_pjp, no_of_outlets, area_classification area_classification_pjp, city city_pjp from bi_pjp) t1 full outer join (select entry_date::date entry_date, count(distinct retailer_name) outlets_touched, area_classification, beat_number_original::varchar beat_number_original, city_name, sales_person from eggoz_soh group by entry_date::date, area_classification, beat_number_original, city_name, sales_person) t2 on t1.date_pjp = t2.entry_date and t1.beat_number_pjp = t2.beat_number_original and lower(t1.area_classification_pjp) = lower(t2.area_classification) where coalesce(t1.beat_number_pjp,t2.beat_number_original) like \'%1%\' ; create or replace table eggozdb.maplemonk.eggoz_and_competitor_soh as select coalesce(t1.model_id,t2.model_id) model_id, coalesce(t1.visit_type,t2.visit_type) visit_type, coalesce(t1.sales_person, t2.sales_person) sales_person, coalesce(t1.retailer_name, t2.retailer_name) retailer_name, coalesce(t1.retailer_id,t2.retailer_id) retailer_id, coalesce(t1.area_classification,t2.area_classification) area_classification, coalesce(t1.beat_number_original,t2.beat_number) beat_number, coalesce(t1.sku_count,t2.sku_count) sku_count, coalesce(t1.sku,t2.sku) sku, coalesce(t1.entry_date,t2.date) date, ifnull(t1.eggoz_soh,0) eggoz_soh, ifnull(t2.comp_soh,0) comp_soh, ifnull(t1.eggoz_soh*t1.sku_count,0) eggoz_soh_eggs, ifnull(t2.comp_soh_eggs,0) comp_soh_eggs, coalesce(t1.latitude,t2.latitude) latitude, coalesce(t1.longitude,t2.longitude) longitude, coalesce(t1.marketing_cluster,t2.marketing_cluster) marketing_cluster from eggoz_soh t1 full outer join (select model_id, visit_type, date, sales_person, retailer_name, area_classification, beat_number, sku, sum(comp_soh) comp_soh, sum(comp_soh*sku_count) comp_soh_eggs, sku_count, retailer_id, latitude, longitude, marketing_cluster from competitor_soh where visit_type in (\'Closing\',\'Visit\') group by model_id, visit_type, date, sales_person, retailer_name, area_classification, beat_number, sku, sku_count, retailer_id, latitude, longitude, marketing_cluster) t2 on t1.retailer_id = t2.retailer_id and t1.entry_date = t2.date and t1.sku = t2.sku and t1.model_id = t2.model_id where t1.visit_type in (\'Closing\',\'Visit\') ;",
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
                        