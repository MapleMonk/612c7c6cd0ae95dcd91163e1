{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary_sku AS Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan from ( select dd.*, rcc.name, dd2.parent_retailer_name, dd2.sku, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.eggs_promo, dd2.retailer_type, case when dd2.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, rr.id as retailer_id, rr.category_id, rr.distributor_id, cast(timestampadd(minute, 330, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster from eggozdb.maplemonk.date_dim datedim cross join eggozdb.maplemonk.my_sql_retailer_retailer rr ) dd left join (select name ,id from eggozdb.maplemonk.my_sql_retailer_customer_category) rcc on rcc.id = dd.category_id left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select date, area area_classification, beat_number_original, retailer_name, parent_retailer_name, sku, sum(net_sales) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(eggs_promo) eggs_promo, case when category_id = 3 then \'Distributor\' else \'Primary_Retailer\' end as retailer_type from eggozdb.maplemonk.summary_reporting_table_beat_retailer_sku group by date, area, beat_number_original, retailer_name, parent_retailer_name, classification_name, sku, category_id union all select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, sku, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(eggs_promo,0) eggs_promo, \'Secondary_Retailer\' as retailer_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, coalesce(ss.sku, sret.sku, srep.sku, spromo.sku) sku, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, spromo.eggs_promo from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_return from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Return\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name and ss.sku = sret.sku full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Replacement\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name and ss.sku = srep.sku full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sku, sum(sku_count*quantity) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Promo\' group by pickup_date, area_classification, beat_number, retailer_name, sku, distributor ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name and ss.sku = spromo.sku group by ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, ss.sku, sret.area_classification, sret.beat_number, sret.retailer_name, sret.sku, srep.area_classification, srep.beat_number, srep.retailer_name, srep.sku, spromo.area_classification, spromo.beat_number, spromo.retailer_name, spromo.sku, srep.eggs_replaced, sret.eggs_return, spromo.eggs_promo ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name where year(dd.date::date)>=2021 and dd.date::date <= getdate() )xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area ; CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary as Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan from (select dd.*, rcc.name, dd2.parent_retailer_name, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.amount_return, dd2.eggs_promo, dd2.collections, dd2.retailer_type, case when dd2.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, rr.id as retailer_id, rr.distributor_id, rr.category_id, cast(timestampadd(minute, 330, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster from eggozdb.maplemonk.date_dim datedim cross join eggozdb.maplemonk.my_sql_retailer_retailer rr ) dd left join (select name ,id from eggozdb.maplemonk.my_sql_retailer_customer_category) rcc on rcc.id = dd.category_id left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select date, area area_classification, beat_number_original, retailer_name, parent_retailer_name, sum(net_sales) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, sum(eggs_promo) eggs_promo, sum(collections) collections, case when category_id = 3 then \'Distributor\' else \'Primary_Retailer\' end as retailer_type from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, beat_number_original, retailer_name, parent_retailer_name, classification_name, category_id union all select date, area_classification, beat_number_original, retailer_name, parent_retailer_name, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(amount_return,0) amount_return, ifnull(eggs_promo,0) eggs_promo, collections, \'Secondary_Retailer\' as retailer_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo, 0 as collections from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_return, sum(total_ret_amount) amount_return from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Return\' group by pickup_date, area_classification, beat_number, retailer_name ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Replacement\' group by pickup_date, area_classification, beat_number, retailer_name ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name full outer join (select pickup_date, area_classification, beat_number, retailer_name, null as parent_retailer_name, sum(eggs) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Promo\' group by pickup_date, area_classification, beat_number, retailer_name ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name group by ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, sret.area_classification, sret.beat_number, sret.retailer_name, srep.area_classification, srep.beat_number, srep.retailer_name, spromo.area_classification, spromo.beat_number, spromo.retailer_name, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name where year(dd.date::date)>=2021 and dd.date::date <= getdate() )xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area ;",
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
                        