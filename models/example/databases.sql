{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.soh as select t1.date as entry_date, t1.type as visit_type, rr.code as retailer_name, t1.retailer_id, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,left(pp.name,1)) as sku, avg(t2.quantity) as eggoz_soh, ifnull(avg(t4.quantity),0) as comp_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_order_competitorsoh t3 on t3.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_order_competitorsohinline t4 on t4.competitor_soh_id = t3.id and concat(pp.sku_count,left(pp.name,1)) = concat(t4.sku_count,left(t4.sku,1)) group by t1.date, t1.type, t1.retailer_id, t2.product_id, rr.code, rr.area_classification, rr.beat_number, pp.sku_count, pp.name ; create or replace table eggozdb.maplemonk.sales_and_soh as select t1.date, t1.retailer_name, t1.area_classification, t1.beat_number_original, t1.onboarding_status, t1.retailer_id, t1.onboarding_date, t1.parent_retailer_name, t1.sku, t1.revenue, t1.eggs_sold, t1.eggs_replaced, t1.eggs_return, t1.eggs_promo, t1.retailer_type, t1.distributor, t1.cluster_dec, t1.cluster_jan, t2.entry_date::date as entry_date, t2.visit_type, ifnull(t2.eggoz_soh,0) eggoz_soh, ifnull(t2.comp_soh,0) comp_soh from primary_and_secondary_sku t1 left join ( select * from ( select row_number() over (partition by retailer_name, sku order by entry_date desc) rownum, * from soh where visit_type in (\'Visit\',\'Closing\') ) where rownum=1 ) t2 on t1.retailer_id = t2.retailer_id and t1.sku = t2.sku where t1.area_classification in (\'Delhi-GT\',\'Noida-GT\',\'Gurgaon-GT\',\'NCR-OF-MT\') and t1.date between \'2023-01-01\' and \'2023-03-06\' and t1.sku is not null and visit_type is not null ;",
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
                        