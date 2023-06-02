{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "create or replace table eggozdb.maplemonk.darjan_sales as select oo.name as bill_name, rr.code as retailer_name, rrp.name as parent_retailer_name, rr.area_classification, rr.beat_number, cast(timestampadd(minute, 660, oo.delivery_date) as date) as date, oo.order_price_amount, ol.quantity, case when lower(pp.slug) like \'%djn-30%\' then 30 when lower(pp.slug) like \'%djn-10%\' then 10 else pp.SKU_Count end as SKU_Count, ol.single_sku_rate, ol.single_sku_mrp, ol.quantity * (case when lower(pp.slug) like \'%djn-30%\' then 30 when lower(pp.slug) like \'%djn-10%\' then 10 else pp.SKU_Count end) eggs, ol.quantity * ol.single_sku_rate as sales, ol.quantity * ol.single_sku_mrp as sales_mrp, pp.slug, pp.brand_type from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ol on ol.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ol.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id where (lower(pp.slug) like \'%wd%\' or lower(pp.slug) like \'%djn%\') and lower(oo.status) in (\'completed\',\'delivered\') and ol.quantity > 0 group by oo.name, rr.code, cast(timestampadd(minute, 660, oo.delivery_date) as date), pp.slug, rr.area_classification, rr.beat_number, rrp.name, ol.quantity, pp.sku_count, ol.single_sku_rate, ol.single_sku_mrp, oo.order_price_amount, pp.brand_type ; create or replace table eggozdb.maplemonk.darjan_vs_smallunprocessed as select ifnull(ds.date,epm.pdate) date, ifnull(ds.darjan_eggs,0) darjan_eggs, epm.* from (select date, sum(eggs) as darjan_eggs from eggozdb.maplemonk.darjan_sales where brand_type = \'branded\' group by date ) ds full outer join (select pdate, sum(\"UB\") as ub_eggs, sum(\"LOSS\") as loss_eggs, sum(\"DIRTY\") as dirty_eggs, sum(chatki) chatki_eggs, sum(hairline) hairline_eggs, sum(\"Small size\") small_size_eggs, sum(\"Total eggs\") procured_eggs, sum(\"Unprocesed Eggs\") unprocessed_eggs, sum(\"Total Clean Eggs\") total_clean_eggs from eggozdb.maplemonk.epm_sheet1 group by pdate ) epm on ds.date = epm.pdate ; create or replace table eggozdb.maplemonk.free_range_sales as select oo.name as bill_name, rr.code as retailer_name, rr.area_classification, rr.beat_number, cast(timestampadd(minute, 660, oo.delivery_date) as date) as date, oo.order_price_amount, ol.quantity, pp.SKU_Count, ol.single_sku_rate, ol.single_sku_mrp, ol.quantity * pp.SKU_Count eggs, ol.quantity * ol.single_sku_rate as sales, pp.slug, pp.brand_type from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ol on ol.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ol.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id where lower(pp.slug) like \'%fr%\' and lower(oo.status) in (\'completed\',\'delivered\') and ol.quantity > 0 group by oo.name, rr.code, cast(timestampadd(minute, 660, oo.delivery_date) as date), pp.slug, rr.area_classification, rr.beat_number, ol.quantity, pp.sku_count, ol.single_sku_rate, ol.single_sku_mrp, oo.order_price_amount, pp.brand_type ; create or replace table eggozdb.maplemonk.Champs_Sales as select oo.name, cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, ool.quantity*pp.SKU_Count as Eggs_sold, ool.quantity as Quantity, pp.SKU_Count , rr.code retailer_name, rrslab.number as commission_slab, rr.beat_number, rr.area_classification, cast(timestampadd(minute, 330, rr.onboarding_date) as date) onboarding_date, rr.onboarding_status, rr.society_name ,pp.current_price as Single_sku_mrp, oo.status from eggozdb.maplemonk.my_sql_distributor_sales_secondaryorder oo left join eggozdb.maplemonk.my_sql_distributor_sales_secondaryorderline ool on ool.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_rateslab rrslab on rrslab.retailer_id = rr.id where lower(pp.name) = \'champs\'",
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
                        