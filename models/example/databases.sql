{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary_sku AS Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan,bc.state,bc.city_name,bc.iso from ( select dd.*,dd2.salesPerson_id, dd2.beat_number_operations, dd2.parent_retailer_name, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.sku_return_amount, dd2.eggs_promo, dd2.sale_type, case when dd.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, datedim.sku, datedim.product_type, rr.id as retailer_id, rr.category_id, rr.distributor_id,rcc.name as retailer_category, rrslab.number as commission_slab, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster, rr.society_name, rr.activity_status, case when category_id=3 then \'Distributor\' else (case when distributor_id is null then \'Primary_Retailer\' else \'Secondary_Retailer\' end) end as retailer_type from eggozdb.maplemonk.date_area_retailer_beat_sku_dim datedim left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.code = datedim.retailer_name left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id ) dd left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select oo.delivery_date date,rr.area_classification as area_classification, oo.discount_amount, rr.beat_number as beat_number_original, rb.beat_number as beat_number_operations, rr.code as retailer_name, rp.name as parent_retailer_name, concat(pp.sku_count,pp.short_name) as sku, product_type, oo.salesperson_id as salesPerson_id, ol.quantity * (ol.single_sku_rate + ol.single_sku_discount+ol.single_sku_tax) revenue, ol.quantity*pp.sku_count as eggs_sold, case when orl.line_type =\'Replacement\' then (orl.quantity*pp.sku_count) end eggs_replaced, case when orl.line_type =\'Return\' then (orl.quantity*pp.sku_count) end eggs_return, orl.amount as sku_return_amount, case when orl.line_type =\'Promo\' then (orl.quantity*pp.sku_count) end eggs_promo, \'Primary_sale\' sale_type from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_retailer_retailer rr on oo.retailer_id = rr.id left join eggozdb.maplemonk.my_sql_order_returnordertransaction ort on rr.id = ort.retailer_id left join eggozdb.maplemonk.my_sql_order_orderreturnline orl on ort.id = orl.return_order_transaction_id left join eggozdb.maplemonk.my_sql_retailer_retailerbeat rb on rr.retailerbeat_id = rb.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rp on rr.parent_id = rp.id left join eggozdb.maplemonk.my_sql_order_orderline ol on oo.id = ol.order_id left join eggozdb.maplemonk.my_sql_product_product pp on ol.product_id = pp.id group by oo.delivery_date,oo.discount_amount, ol.quantity,ol.single_sku_rate , ol.single_sku_discount , ol.single_sku_tax,pp.sku_count,orl.quantity, orl.line_type, rr.area_classification,oo.salesperson_id, beat_number_original, beat_number_operations, rr.code, parent_retailer_name, sku,product_type,orl.amount union all select date, area_classification,0 as discount_amount, beat_number_original, beat_number_operations, retailer_name, parent_retailer_name, sku, product_type,salesPerson_id, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(sku_return_amount,0) sku_return_amount, ifnull(eggs_promo,0) eggs_promo, \'Secondary_sale\' sale_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, ss.salesPerson_id, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.beat_number_operations, sret.beat_number_operations, srep.beat_number_operations, spromo.beat_number_operations) beat_number_operations, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, coalesce(ss.sku, sret.sku, srep.sku, spromo.sku) sku, coalesce(ss.product_type,sret.product_type,srep.product_type,spromo.product_type) product_type, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, sret.sku_return_amount, spromo.eggs_promo from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sku, product_type, sum(sku_count*quantity) eggs_return, sum(SKU_return_amount) sku_return_amount from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Return\' group by pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, sku, product_type, distributor ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name and ss.sku = sret.sku full outer join (select pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sku, product_type, sum(sku_count*quantity) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Replacement\' group by pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, sku, product_type, distributor ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name and ss.sku = srep.sku full outer join (select pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sku, product_type, sum(sku_count*quantity) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo_sku where type = \'Promo\' group by pickup_date,salesPerson_id, area_classification, beat_number, beat_number_operations, retailer_name, sku, product_type, distributor ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name and ss.sku = spromo.sku group by ss.delivery_date,ss.salesPerson_id, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, ss.sku, ss.product_type, sret.area_classification, sret.beat_number, sret.retailer_name, sret.sku, sret.product_type, srep.area_classification, srep.beat_number, srep.retailer_name, srep.sku, srep.product_type, spromo.area_classification, spromo.beat_number, spromo.retailer_name, spromo.sku, spromo.product_type, srep.eggs_replaced, sret.eggs_return, sret.sku_return_amount, spromo.eggs_promo, ss.beat_number_operations, sret.beat_number_operations, srep.beat_number_operations, spromo.beat_number_operations ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name and dd2.sku=dd.sku where year(dd.date::date)>=2021 and dd.date::date <= getdate() ) xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = xx.city_id ; CREATE OR REPLACE TABLE eggozdb.maplemonk.primary_and_secondary as Select xx.*, yy.Cluster_Dec,zz.Cluster_Jan,yy.Rank_Dec, zz.Rank_Jan, bc.city_name from (select dd.*, dd2.beat_number_operations, dd2.parent_retailer_name, dd2.revenue, dd2.eggs_sold, dd2.eggs_replaced, dd2.eggs_return, dd2.amount_return, dd2.eggs_promo, dd2.collections, dd2.sale_type, case when dd.retailer_type = \'Secondary_Retailer\' then rr2.code else \'no_distributor\' end as distributor from ( select datedim.date::date date, rr.code as retailer_name, rr.area_classification, rr.beat_number as beat_number_original, rr.onboarding_status, rr.id as retailer_id, rr.distributor_id, rr.category_id, rcc.name as retailer_category, rrslab.number as commission_slab, cast(timestampadd(minute, 660, rr.onboarding_date) as date) onboarding_date, rr.city_id, rr.marketing_cluster, rr.society_name,rr.activity_status, case when category_id=3 then \'Distributor\' else (case when distributor_id is null then \'Primary_Retailer\' else \'Secondary_Retailer\' end) end as retailer_type from (select date from eggozdb.maplemonk.date_dim where date between \'2021-01-01\' and getdate()) datedim cross join (select code, area_classification, beat_number, onboarding_status, id, distributor_id, category_id, onboarding_date, city_id, marketing_cluster, society_name, activity_status from eggozdb.maplemonk.my_sql_retailer_retailer) rr left join eggozdb.maplemonk.my_sql_retailer_customer_category rcc on rcc.id = rr.category_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id ) dd left join (select code, id from eggozdb.maplemonk.my_sql_retailer_retailer) rr2 on rr2.id = dd.distributor_id left join ( select date, area area_classification, beat_number_original, beat_number_operations, retailer_name, parent_retailer_name, sum(net_sales) revenue, sum(eggs_sold) eggs_sold, sum(eggs_replaced) eggs_replaced, sum(eggs_return) eggs_return, sum(amount_return) amount_return, sum(eggs_promo) eggs_promo, sum(collections) collections, \'Primary_sale\' sale_type from eggozdb.maplemonk.summary_reporting_table_beat_retailer group by date, area, beat_number_original, beat_number_operations, retailer_name, parent_retailer_name, classification_name, category_id union all select date, area_classification, beat_number_original, beat_number_operations, retailer_name, parent_retailer_name, ifnull(revenue,0) revenue, ifnull(eggs_sold,0) eggs_sold, ifnull(eggs_replaced,0) eggs_replaced, ifnull(eggs_return,0) eggs_return, ifnull(amount_return,0) amount_return, ifnull(eggs_promo,0) eggs_promo, collections, \'Secondary_sale\' sale_type from ( select coalesce(ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date) date, coalesce(ss.area_classification, sret.area_classification, srep.area_classification, spromo.area_classification) area_classification, coalesce(ss.beat_number_original, sret.beat_number, srep.beat_number, spromo.beat_number) beat_number_original, coalesce(ss.beat_number_operations, sret.beat_number_operations, srep.beat_number_operations, spromo.beat_number_operations) beat_number_operations, coalesce(ss.retailer_name, sret.retailer_name, srep.retailer_name, spromo.retailer_name) retailer_name, null as parent_retailer_name, sum(ss.sale) revenue, sum(ss.eggs_sold) eggs_sold, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo, 0 as collections from eggozdb.maplemonk.secondary_sales ss full outer join ( select pickup_date, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sum(eggs) eggs_return, sum(return_amount) amount_return from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Return\' group by pickup_date, area_classification, beat_number, beat_number_operations, retailer_name ) sret on ss.delivery_date = sret.pickup_date and ss.retailer_name = sret.retailer_name full outer join (select pickup_date, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sum(eggs) eggs_replaced from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Replacement\' group by pickup_date, area_classification, beat_number, beat_number_operations, retailer_name ) srep on ss.delivery_date = srep.pickup_date and ss.retailer_name = srep.retailer_name full outer join (select pickup_date, area_classification, beat_number, beat_number_operations, retailer_name, null as parent_retailer_name, sum(eggs) eggs_promo from eggozdb.maplemonk.secondary_return_replacement_promo where type = \'Promo\' group by pickup_date, area_classification, beat_number, beat_number_operations, retailer_name ) spromo on ss.delivery_date = spromo.pickup_date and ss.retailer_name = spromo.retailer_name group by ss.delivery_date, sret.pickup_date, srep.pickup_date, spromo.pickup_date, ss.area_classification, ss.beat_number_original, ss.retailer_name, sret.area_classification, sret.beat_number, sret.retailer_name, srep.area_classification, srep.beat_number, srep.retailer_name, spromo.area_classification, spromo.beat_number, spromo.retailer_name, srep.eggs_replaced, sret.eggs_return, sret.amount_return, spromo.eggs_promo, ss.beat_number_operations, sret.beat_number_operations, srep.beat_number_operations, spromo.beat_number_operations ) ) dd2 on dd.date = dd2.date and dd.retailer_name = dd2.retailer_name where year(dd.date::date)>=2021 and dd.date::date <= getdate() )xx left join (select code,Retailer_name,Area, Cluster as Cluster_Dec, Rank as Rank_Dec from eggozdb.maplemonk.retailer_ranking where date_ =\'2022-12-01\' ) yy on xx.Retailer_name = yy.Retailer_name and xx.area_classification = yy.Area left join (select code,Retailer_name,Area, Cluster as Cluster_Jan, Rank as Rank_Jan from eggozdb.maplemonk.retailer_ranking where date_ =\'2023-01-01\') zz on yy.Retailer_name=zz.Retailer_name and yy.Area = zz.Area left join eggozdb.maplemonk.my_sql_base_city bc on bc.id = xx.city_id ;",
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
                        