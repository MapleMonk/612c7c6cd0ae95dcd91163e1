{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.soh as select t1.date as entry_date, t1.type as visit_type, rr.code as retailer_name, t1.retailer_id, rr.area_classification, rr.beat_number as beat_number_original, concat(pp.sku_count,left(pp.name,1)) as sku, avg(t2.quantity) as eggoz_soh, ifnull(sum(t4.quantity),0) as comp_soh from eggozdb.maplemonk.my_sql_order_sohmodel t1 left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = t1.retailer_id left join eggozdb.maplemonk.my_sql_order_eggozsoh t2 on t2.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = t2.product_id left join eggozdb.maplemonk.my_sql_order_competitorsoh t3 on t3.soh_model_id = t1.id left join eggozdb.maplemonk.my_sql_order_competitorsohinline t4 on t4.competitor_soh_id = t3.id and concat(pp.sku_count,left(pp.name,1)) = concat(t4.sku_count,left(t4.sku,1)) group by t1.date, t1.type, t1.retailer_id, t2.product_id, rr.code, rr.area_classification, rr.beat_number, pp.sku_count, pp.name ;",
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
                        