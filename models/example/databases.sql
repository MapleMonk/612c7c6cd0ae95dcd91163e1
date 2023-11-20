{{ config(
                        materialized='table',
                            post_hook={
                                "sql": "Create or replace table eggozdb.maplemonk.po_list as SELECT oo.name as order_name, oo.date, oo.generation_date, oo.delivery_date, oo.order_type, rr.code, oo.order_price_amount, po.po_status, ool.quantity, oo.bill_no, pp.slug, rrp.name as parent_name, rr.area_classification, rr.beat_number, case when sp.name is null then sp2.name else sp.name end as Operator FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id LEFT JOIN (SELECT ddp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ddp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ddp.user_id) sp ON sp.id = oo.jse_id LEFT JOIN (SELECT ssp.id, cau.name FROM eggozdb.maplemonk.my_sql_saleschain_salespersonprofile ssp LEFT JOIN eggozdb.maplemonk.my_sql_custom_auth_user cau ON cau.id = ssp.user_id) sp2 ON sp2.id = oo.salesPerson_id ; create or replace table eggozdb.maplemonk.demand_supply_po_list as select distinct retailer_name, parent_name, area_classification, city, beat_number, po_date, delivery_date, po_status, ifnull(supply_status,\'not_delivered\') supply_status, po_type, po_no, po_total_amount, ifnull(supply_total_amount,0) as supply_total_amount, slug,description, sku, egg_type,productSubDivision_id, case when productSubDivision_id in(42,45) then \'NPD\' else egg_type end category, po_quantity/(count(po_no) over (partition by po_no, sku order by po_date)) as po_quantity, ifnull(supply_quantity,0) supply_quantity, po_egg_count/(count(po_no) over (partition by po_no, sku order by po_date)) as po_egg_count, ifnull(supply_egg_count,0) supply_egg_count, ifnull(egg_fillrate,0) egg_fillrate, po_amount/(count(po_no) over (partition by po_no, sku order by po_date)) as po_amount, ifnull(supply_amount,0) supply_amount, ifnull(amount_fillrate,0) amount_fillrate from (select distinct table1.code as retailer_name, table1.parent_name, table1.area_classification, table1.city_name as city, table1.beat_number, table1.po_date, table2.delivery_date, table1.po_status, table2.status as supply_status, table1.po_type, table1.bill_no as po_no, table1.order_price_amount as po_total_amount, table2.order_price_amount as supply_total_amount, table1.slug,table1.description, table1.sku, table1.egg_type,table1.productSubDivision_id, table1.quantity as po_quantity, table2.quantity as supply_quantity, table1.egg_count as po_egg_count, table2.egg_count as supply_egg_count, (table2.egg_count/table1.egg_count) as egg_fillrate, table1.amount as po_amount, table2.amount as supply_amount, (table2.amount/table1.amount) as amount_fillrate from ( SELECT distinct rr.code, cast(timestampadd(minute, 660, oo.date) as date) po_date, oo.order_type, sum(oo.order_price_amount) order_price_amount, oo.status as po_status, po.po_type, sum(ool.quantity) quantity, pp.slug,pp.description, pp.name as egg_type,pp.productSubDivision_id, concat(pp.sku_count,pp.short_name) as sku, pp.SKU_Count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate+ool.single_sku_discount)) amount, oo.bill_no, oo.id as bill_id, rrp.name as parent_name, rr.area_classification, bc.city_name, rr.beat_number FROM eggozdb.maplemonk.my_sql_order_purchaseorder po join eggozdb.maplemonk.my_sql_order_order oo on oo.id = po.order_ptr_id join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id join eggozdb.maplemonk.my_sql_base_city bc on rr.city_id = bc.id join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id where lower(oo.order_type) = \'purchase order\' group by rr.code, oo.bill_no, oo.id, cast(timestampadd(minute, 660, oo.date) as date), oo.order_type, po.po_status, oo.status, po.po_type, pp.slug,pp.description, pp.SKU_Count, pp.productSubDivision_id, po.purchase_id, rrp.name, rr.area_classification, bc.city_name, rr.beat_number, pp.short_name, pp.name ) table1 left join ( SELECT cast(timestampadd(minute, 660, oo.delivery_date) as date) delivery_date, oo.order_type, rr.code, sum(oo.order_price_amount) order_price_amount, oo.status, sum(ool.quantity) quantity, pp.slug,pp.description, pp.name as egg_type,pp.productSubDivision_id, case when pp.productSubDivision_id in(42,52) then \'NPD\' else pp.name end as EGG, concat(pp.sku_count,pp.short_name) as sku, pp.sku_count, sum(ool.quantity*pp.sku_count) egg_count, sum(ool.quantity*(ool.single_sku_rate + ool.single_sku_discount)) amount, oo.po_no, oo.purchase_order_id as po_id, rrp.name as parent_name, rr.area_classification, rr.beat_number from eggozdb.maplemonk.my_sql_order_order oo join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_order_orderline ool on oo.id = ool.order_id join eggozdb.maplemonk.my_sql_product_product pp on ool.product_id = pp.id join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rr.parent_id = rrp.id where lower(oo.order_type) = \'retailer\' and lower(oo.status) in (\'delivered\', \'completed\') group by oo.purchase_order_id, cast(timestampadd(minute, 660, oo.delivery_date) as date), oo.order_type, rr.code, oo.status, pp.slug,pp.description, pp.sku_count, pp.productSubDivision_id, oo.po_no, rrp.name, rr.area_classification, rr.beat_number, pp.short_name, pp.name ) table2 on table1.bill_id = table2.po_id and table1.sku = table2.sku ) ; create or replace table eggozdb.maplemonk.repeated_po as select purchase_id, count(purchase_id) po_counts from eggozdb.maplemonk.my_sql_order_purchaseorder group by purchase_id having count(purchase_id)>1 ; create or replace table eggozdb.maplemonk.po_data_for_TG as select t1.order_type, t1.status, t1.po_date, t1.bill_no, t1.retailer_name, t1.area_classification, t1.beat_number, t1.city_name, t1.demanded_quantity, t1.demanded_eggs, t1.sku, t2.po_no, t2.delivered_quantity, t2.delivered_eggs from ( select distinct oo.order_type, oo.status, cast(timestampadd(minute, 660, oo.date) as date) as po_date, oo.bill_no, rr.code as retailer_name, rr.area_classification, rr.beat_number, bc.city_name, ool.quantity demanded_quantity, ool.quantity*pp.SKU_Count demanded_eggs, concat(pp.sku_count,pp.short_name) sku from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ool on ool.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id left join eggozdb.maplemonk.my_sql_base_city as bc on bc.id = rr.city_id where oo.order_type = \'Purchase Order\' and lower(oo.status) in (\'closed_po\',\'open_po\',\'expired_po\',\'ongoing_po\') and cast(timestampadd(minute, 660, oo.date) as date) between \'2022-04-01\' and cast(timestampadd(minute, 660, current_date) as date) ) t1 left join ( select oo.po_no, sum(ool.quantity) delivered_quantity, sum(ool.quantity*pp.SKU_Count) delivered_eggs, concat(pp.sku_count,pp.short_name) sku from eggozdb.maplemonk.my_sql_order_order oo left join eggozdb.maplemonk.my_sql_order_orderline ool on ool.order_id = oo.id left join eggozdb.maplemonk.my_sql_product_product pp on pp.id = ool.product_id left join eggozdb.maplemonk.my_sql_retailer_retailer rr on rr.id = oo.retailer_id left join eggozdb.maplemonk.my_sql_retailer_retailerparent rrp on rrp.id = rr.parent_id left join eggozdb.maplemonk.my_sql_base_city as bc on bc.id = rr.city_id where order_type = \'Retailer\' and lower(oo.status) in (\'completed\',\'delivered\') and cast(timestampadd(minute, 660, oo.delivery_date) as date) between \'2022-04-01\' and cast(timestampadd(minute, 660, current_date) as date) and oo.po_no is not null and oo.po_no <> \'\' group by oo.po_no, pp.sku_count, pp.short_name ) t2 on t1.bill_no = t2.po_no and t1.sku = t2.sku ;",
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
                        