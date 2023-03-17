{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.eggoz_soh as select distinct t1.date::date as entry_date, cau.name as sales_person, t1.type as visit_type, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,left(pp.name,1)) as sku, t2.quantity as eggoz_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id ; create or replace table eggozdb.maplemonk.eggoz_soh_adherence as select t1.date::date date, cau.name as sales_person_name, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, count(distinct t1.retailer_id) retailers_covered, t2.active_retailers, count(distinct t1.retailer_id)*100/t2.active_retailers adherence from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id left join (select beat_number, area_classification, count(id) active_retailers from eggozdb.maplemonk.my_sql_retailer_retailer where onboarding_status=\'Active\' group by area_classification, beat_number) t2 on t2.area_classification = rr.area_classification and t2.beat_number = rr.beat_number group by t1.date, t1.sales_person_profile_id, rr.area_classification, rr.beat_number, cau.name, t2.active_retailers ; create or replace table eggozdb.maplemonk.sales_and_soh as select t1.date::date date, t1.retailer_name, t1.area_classification, t1.beat_number_original, t1.onboarding_status, t1.retailer_id, t1.onboarding_date, t1.parent_retailer_name, t1.sku, t2.sku_count, t1.revenue, t1.eggs_sold, t1.eggs_replaced, t1.eggs_return, t1.eggs_promo, t1.retailer_type, t1.distributor, t1.cluster_dec, t1.cluster_jan, t2.entry_date::date as entry_date, t2.visit_type, ifnull(t2.eggoz_soh,0) eggoz_soh, ifnull(t2.comp_soh,0) comp_soh from primary_and_secondary_sku t1 left join ( select * from ( select row_number() over (partition by retailer_name, sku order by entry_date desc) rownum, * from soh where visit_type in (\'Visit\',\'Closing\') ) where rownum=1 ) t2 on t1.retailer_id = t2.retailer_id and t1.sku = t2.sku where t1.area_classification in (\'Delhi-GT\',\'Noida-GT\',\'Gurgaon-GT\',\'NCR-OF-MT\') and t1.date between \'2023-01-01\' and \'2023-03-06\' and t1.sku is not null and visit_type is not null ; create or replace table eggozdb.maplemonk.doi as select nn.*, nn.soh1 + sum(ll.eggs_sold)+sum(ll.eggs_replaced)+sum(ll.eggs_promo)-sum(ll.eggs_return) - nn.soh2 as tertiary_sales, div0((nn.soh1 + sum(ll.eggs_sold)+sum(ll.eggs_replaced)+sum(ll.eggs_promo)-sum(ll.eggs_return) - nn.soh2)*datediff(day,date1,date2),nn.soh2) doi, datediff(day,date1,date2) days, sum(ll.revenue) revenue, sum(ll.eggs_sold) eggs_sold, sum(ll.eggs_replaced) eggs_replaced, sum(ll.eggs_return) eggs_return, sum(ll.eggs_promo) eggs_promo, ll.retailer_type, ll.distributor from (select mm.model_id, mm.visit_type, mm.sales_person, mm.retailer_name, mm.area_classification, mm.beat_number, mm.sku_count, mm.eggoz_sku, mm.date as date2, mm.date_2 as date1, mm.eggoz_soh*mm.sku_count soh2, ifnull(mm.soh_1,0)*mm.sku_count soh1 from ( select tt.*, lead(tt.date) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number, tt.sku_count, tt.eggoz_sku order by tt.date desc) date_2, lead(tt.eggoz_soh) over (partition by tt.retailer_name, tt.area_classification, tt.beat_number, tt.eggoz_sku order by tt.date desc) soh_1 from ( select t1.id model_id, cast(timestampadd(minute, 660, date) as date) date, t1.type visit_type, cau.name as sales_person, rr.code as retailer_name, rr.area_classification, rr.beat_number, pp.sku_count, concat(pp.sku_count,left(pp.name,1)) eggoz_sku, t2.quantity eggoz_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ss on ss.id = t1.sales_person_profile_id left join eggozdb.maplemonk.my_sql_custom_auth_user cau on cau.id = ss.user_id where t1.type in (\'Visit\',\'Closing\') group by t1.id, t1.date, t1.type , cau.name, rr.code, rr.area_classification, rr.beat_number, pp.sku_count, pp.name, t2.quantity ) tt ) mm where mm.date_2 is not null) nn left join ( select date, retailer_name, area_classification, beat_number_original, sku, revenue, eggs_sold, eggs_replaced, eggs_return, eggs_promo, retailer_type, distributor from eggozdb.maplemonk.primary_and_secondary_sku where area_classification in (\'Noida-GT\',\'Gurgaon-GT\',\'Delhi-GT\',\'NCR-OF-MT\') and year(date) = 2023 and month(date) = 03 or month(date) = 02 and sku is not null ) ll on ll.retailer_name = nn.retailer_name and ll.sku = nn.eggoz_sku where ll.date between nn.date1 and nn.date2 group by nn.model_id, nn.visit_type, nn.sales_person, nn.retailer_name, nn.area_classification, nn.beat_number, nn.sku_count, nn.eggoz_sku, nn.date2, nn.date1, nn.soh2, nn.soh1, ll.retailer_type, ll.distributor ;",
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
                        