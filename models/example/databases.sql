{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.secondary_sales AS select rr2.code as distributor_name, tt.* from ( select oo.name bill_name, oo.status, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, sum(oo.order_price_amount) order_price_amount, ol.quantity * (ol.single_sku_rate + ol.single_sku_discount) sale, ol.single_sku_rate, ol.single_sku_discount, CASE WHEN pp.name LIKE \'%liquid%\' THEN ol.quantity * 1000 / 35 ELSE ol.quantity * CASE WHEN pp.SKU_Count = 1 THEN CASE WHEN rr.area_classification = \'UP-UB\' THEN 1 ELSE 30 END ELSE pp.SKU_Count END END AS eggs_sold, concat(pp.sku_count,pp.short_name) sku, pp.slug, ol.quantity, oo.retailer_id, rr.code as retailer_name, rr.area_classification, rr.beat_number beat_number_original, bc.cluster_name, rr.onboarding_status, rr.city_id, rr.distributor_id, rcc.name as retailer_category from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ol on ol.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ol.product_id left join eggozdb.maplemonk.my_sql_base_cluster bc on rr.cluster_id = bc.id where lower(oo.status) = \'created\' and lower(pp.slug) not like \'%wd%\' and lower(pp.slug) not like \'%djn%\' and pp.brand_type <> \'unbranded\' and pp.name <> \'Frozen\' group by cast(timestampadd(minute, 660, oo.delivery_date) as date), oo.retailer_id, oo.name, pp.name, pp.short_name, pp.slug, oo.status, ol.quantity, pp.sku_count, rr.area_classification, pp.slug, rr.code, rr.beat_number, rr.onboarding_status, rr.city_id, rr.distributor_id, ol.single_sku_rate, ol.single_sku_discount, bc.cluster_name, rcc.name ) tt join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = tt.distributor_id ; CREATE OR REPLACE TABLE eggozdb.maplemonk.secondary_return_replacement_promo_sku AS select rr2.code as distributor, ret_rep.* from ( select orr.id as ret_rep_id, orr.type, orr.return_amount as total_ret_amount, cast(timestampadd(minute, 660, orr.return_date) as date) pickup_date, orr.retailer_id, orr.salesPerson_id, orr.status, rrl.quantity, rrl.single_sku_mrp, rrl.single_sku_discount, rrl.single_sku_rate, rrl.product_id, rr.code retailer_name, rr.area_classification, rr.beat_number, rr.city_id, rr.cluster_id, rr.distributor_id, pp.name, pp.short_name, pp.slug, pp.SKU_Count, pp.brand_type, concat(pp.sku_count,pp.short_name) sku, rcc.name as retailer_category from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturn orr left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturnreplaceline rrl on rrl.return_order_id = orr.id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = orr.retailer_id left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = rrl.product_id where lower(orr.status) = \'created\' and lower(pp.slug) not like \'%wd%\' and lower(pp.slug) not like \'%djn%\' and pp.brand_type <> \'unbranded\' and pp.name <> \'Frozen\' ) ret_rep left join eggozdb.maplemonk.my_sql_retailer_retailer rr2 on rr2.id = ret_rep.distributor_id ; CREATE OR REPLACE TABLE eggozdb.maplemonk.secondary_return_replacement_promo AS select t1.*, t2.id, t2.total_ret_amount from (select distributor, type, pickup_date, retailer_id, salesperson_id, status, ret_rep_id, sum(quantity*sku_count) as eggs, retailer_name, area_classification, beat_number, city_id, cluster_id, distributor_id, brand_type, retailer_category from eggozdb.maplemonk.secondary_return_replacement_promo_sku group by distributor, type, pickup_date, retailer_id, salesperson_id, status, retailer_name, area_classification, beat_number, city_id, cluster_id, distributor_id, brand_type, ret_rep_id, retailer_category ) t1 left join (select id, return_amount as total_ret_amount from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderreturn ) t2 on t1.ret_rep_id = t2.id order by t1.pickup_date desc ; CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary_sku AS Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan from ( select dd.*, dd2.parent_retailer_name, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.eggs_promo, dd2.sale_type, case when dd.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, datedim.sku, rr.id as retailer_id, rr.category_id, rr.distributor_id,rcc.name as retailer_category, rrslab.number as commission_slab, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster, rr.society_name, rr.activity_status, case when category_id=3 then \'Distributor\' else (case when distributor_id is null then \'Primary_Retailer\' else \'Secondary_Retailer\' end) end as retailer_type from eggozdb.maplemonk.date_area_retailer_beat_sku_dim datedim left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.code = datedim.retailer_name left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id ) dd left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select date, area area_classification, beat_number_original, retailer_name, parent_retailer_name, sku, sum(net_sales) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(eggs_promo) eggs_promo, \'Primary_sale\' sale_type from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku group by date, area, beat_number_original, retailer_name, parent_retailer_name, classification_name, sku, category_id union all select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, sku, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(eggs_promo,0) eggs_promo, \'Secondary_sale\' sale_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, coalesce(ss.sku, sret.sku, srep.sku, spromo.sku) sku, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, spromo.eggs_promo from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_return from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Return\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name and ss.sku = sret.sku full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Replacement\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name and ss.sku = srep.sku full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Promo\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name and ss.sku = spromo.sku group by ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, ss.sku, sret.area_classification, sret.beat_number, sret.retailer_name, sret.sku, srep.area_classification, srep.beat_number, srep.retailer_name, srep.sku, spromo.area_classification, spromo.beat_number, spromo.retailer_name, spromo.sku, srep.eggs_replaced, sret.eggs_return, spromo.eggs_promo ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name and dd2.sku=dd.sku where year(dd.date::date)>=2021 and dd.date::date <= getdate() ) xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area ; CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary as Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan from (select dd.*, dd2.parent_retailer_name, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.amount_return, dd2.eggs_promo, dd2.collections, dd2.sale_type, case when dd.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, rr.id as retailer_id, rr.distributor_id, rr.category_id, rcc.name as retailer_category, rrslab.number as commission_slab, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster, rr.society_name,rr.activity_status, case when category_id=3 then \'Distributor\' else (case when distributor_id is null then \'Primary_Retailer\' else \'Secondary_Retailer\' end) end as retailer_type from eggozdb.maplemonk.date_dim datedim cross join eggozdb.maplemonk.my_sql_retailer_retailer rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id ) dd left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select date, area area_classification, beat_number_original, retailer_name, parent_retailer_name, sum(net_sales) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, sum(eggs_promo) eggs_promo, sum(collections) collections, \'Primary_sale\' sale_type from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, beat_number_original, retailer_name, parent_retailer_name, classification_name, category_id union all select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(amount_return,0) amount_return, ifnull(eggs_promo,0) eggs_promo, collections, \'Secondary_sale\' sale_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo, 0 as collections from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_return, sum(total_ret_amount) amount_return from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Return\' group by pickup_date, area_classification, beat_number, retailer_name ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Replacement\' group by pickup_date, area_classification, beat_number, retailer_name ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Promo\' group by pickup_date, area_classification, beat_number, retailer_name ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name group by ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, sret.area_classification, sret.beat_number, sret.retailer_name, srep.area_classification, srep.beat_number, srep.retailer_name, spromo.area_classification, spromo.beat_number, spromo.retailer_name, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name where year(dd.date::date)>=2021 and dd.date::date <= getdate())xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area ;",
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
                        